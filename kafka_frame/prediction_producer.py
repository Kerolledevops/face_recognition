import json
import socket
import time
from contextlib import contextmanager
from multiprocessing import Process

import cv2
import face_recognition
import numpy as np
from kafka import KafkaProducer, KafkaConsumer, KafkaClient, KafkaAdminClient
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.partitioner import RoundRobinPartitioner, Murmur2Partitioner
from kafka.structs import OffsetAndMetadata, TopicPartition
from numpy.ma.bench import timer

from kafka_frame import *
from .utils import np_from_json, np_to_json
from .params import *


class ConsumeFrames(Process):
    def __int__(self,
                frame_topic,
                processed_frame_topic,
                topic_partitions=TOPIC_PARTITIONS,
                scale=SCALE,
                verbose=False,
                rr_distribute=False,
                group=None,
                target=None,
                name=None
                ):
        """
               FACE DETECTION IN FRAMES --> Consuming encoded frame messages, detect faces and their encodings [PRE PROCESS],
               publish it to processed_frame_topic where these values are used for face matching with query faces.

               :param frame_topic: kafka topic to consume stamped encoded frames.
               :param processed_frame_topic: kafka topic to publish stamped encoded frames with face detection and encodings.
               :param topic_partitions: number of partitions processed_frame_topic topic has, for distributing messages among partitions
               :param scale: (0, 1] scale image before face recognition, but less accurate, trade off!!
               :param verbose: print logs on stdout
               :param rr_distribute:  use round robin partitioner and assignor, should be set same as respective producers or consumers.
               :param group: group should always be None; it exists solely for compatibility with threading.
               :param target: Process Target
               :param name: Process name
               """
        super.__init__(group=group, target=target, name=name)
        self.iam = "{}-{}".format(socket.gethostname(), self.name)
        self.frame_topic = frame_topic
        self.verbose = verbose
        self.topic_partitions = topic_partitions
        self.processed_frame_topic = processed_frame_topic
        self.rr_distribute = rr_distribute
        print("[INFO] I AM", self.iam)

    def run(self):
        """Consume raw frames, detects faces, finds their encoding [PRE PROCESS],
                   predictions Published to processed_frame_topic fro face matching."""

        partition_assignment_strategy = [RoundRobinPartitionAssignor] if self.rr_distribute else [
            RangePartitionAssignor,
            RoundRobinPartitionAssignor
        ]

        frame_consumer = KafkaConsumer(
            group_id=GROUP_ID,
            client_id=self.iam,
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            key_deserializer=lambda key: key.decode(),
            value_deserializer=lambda value: json.loads(value.decode()),
            partition_assignment_strategy=partition_assignment_strategy,
            auto_offset_reset=AUTO_OFFSET_RESET
        )

        frame_consumer.subscribe(
            [self.frame_topic]
        )

        if self.rr_distribute:  # partitioner for processed frame topic
            partitioner = RoundRobinPartitioner(
                partitions=[TopicPartition(
                    topic=self.frame_topic, partition=i
                ) for i in range(self.topic_partitions)]
            )
        else:
            partitioner = Murmur2Partitioner(
                partitions=[TopicPartition(
                    topic=self.frame_topic, partition=i
                ) for i in range(self.topic_partitions)]
            )
        processed_frame_producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            key_serializer=lambda key: str(key).encode(),
            value_serializer=lambda value: json.dumps(value).encode(),
            partitioner=partitioner
        )

        try:
            while True:
                if self.verbose:
                    print("[ConsumeFrames {}] WAITING FOR NEXT FRAMES..".format(socket.gethostname()))
                raw_frame_messages = frame_consumer.poll(timeout_ms=10, max_records=10)

                for topic_partion, msgs in raw_frame_messages.items():
                    for msg in msgs:
                        result = self.get_processed_frame_object(msg.value, self.scale)

                        tp = TopicPartition(msg.topic, msg.partition)
                        offsets = {tp: OffsetAndMetadata(msg.offset, None)}
                        frame_consumer.commit(offsets=offsets)

                        # Partition to be sent to
                        processed_frame_producer.send(self.processed_frame_topic,
                                                      key="{}_{}".format(result["camera"], result["frame_num"]),
                                                      value=result)
                        processed_frame_producer.send(self.processed_frame_topic,
                                                      key="{}_{}".format(result["camera"], result["frame_num"]),
                                                      value=result
                                                      )
                    processed_frame_producer.flush()
        except KeyboardInterrupt as e:
            print(e)
            pass
        finally:
            print("clossing stream")
            frame_consumer.close()


class PredictFrames(Process):

    def __init__(self,
                 processed_frame_topic,
                 query_faces_topic,
                 scale=1.0,
                 verbose=False,
                 rr_distribute=False,
                 group=None,
                 target=None,
                 name=None):
        """
        FACE MATCHING TO QUERY FACES --> Consuming frame objects to produce predictions.

        :param processed_frame_topic: kafka topic to consume from stamped encoded frames with face detection and encodings.
        :param query_faces_topic: kafka topic which broadcasts query face names and encodings.
        :param scale: (0, 1] scale used during pre processing step.
        :param verbose: print log
        :param rr_distribute: use round robin partitioner and assignor, should be set same as respective producers or consumers.
        :param group: group should always be None; it exists solely for compatibility with threading.
        :param target: Process Target
        :param name: Process name
        """
        super().__init__(group=group, target=target, name=name)

        self.iam = "{}-{}".format(socket.gethostname(), self.name)
        self.frame_topic = processed_frame_topic
        self.query_faces_topic = query_faces_topic
        self.verbose = verbose
        self.scale = scale
        self.rr_distribute = rr_distribute
        print("[INFO] I am ", self.iam)

    def run(self):
        """Consume pre processed frames, match query face with faces detected in pre processing step
        (published to processed frame topic) publish results, box added to frame data if in params,
        ORIGINAL_PREFIX == PREDICTED_PREFIX"""

        # Connect to kafka, Consume frame obj bytes deserialize to json
        partition_assignment_strategy = [RoundRobinPartitionAssignor] if self.rr_distribute else [
            RangePartitionAssignor,
            RoundRobinPartitionAssignor]

        frame_consumer = KafkaConsumer(
            group_id=GROUP_ID,
            client_id=self.iam,
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            key_deserializer=lambda key: key.decode(),
            value_deserializer=lambda value: json.loads(value.decode()),
            partition_assignment_strategy=partition_assignment_strategy,
            auto_offset_reset=AUTO_OFFSET_RESET)

        frame_consumer.subscribe([self.frame_topic])

        #  Produces prediction object
        prediction_producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVERS],
                                            key_serializer=lambda key: str(key).encode(),
                                            value_serializer=lambda value: json.dumps(value).encode())

        # Consume known face object to know what faces are the target
        query_faces_consumer = KafkaConsumer(self.query_faces_topic, group_id=self.iam, client_id=self.iam,
                                             bootstrap_servers=[BOOTSTRAP_SERVERS],
                                             value_deserializer=lambda value: json.loads(value.decode()))

        print("[PredictFrames {}] WAITING FOR TRACKING INFO..".format(socket.gethostname()))
        query_faces_message = next(query_faces_consumer)
        print("[PredictFrames {}] GOT TRACKING INFO..".format(socket.gethostname()))
        log_file_name = '{}/{}/consumer_{}_workers_{}_cameras_{}.csv'.format(MAIN_PATH, LOG_DIR,
                                                                             self.name,
                                                                             SET_PARTITIONS,
                                                                             TOTAL_CAMERAS)
        log_file = None
        try:
            # log just one process, gives a good estimate of the total lag
            print("Writing header to {}".format(log_file_name))
            log_file = open(log_file_name, 'w')
            log_file.write('camera,frame,prediction,consumers,latency\n')
            while True:

                if self.verbose:
                    print("[PredictFrames {}] WAITING FOR NEXT FRAMES..".format(socket.gethostname()))

                raw_frame_messages = frame_consumer.poll(timeout_ms=10, max_records=10)

                for topic_partition, msgs in raw_frame_messages.items():
                    # Get the predicted Object, JSON with frame and meta info about the frame
                    for msg in msgs:
                        result = self.get_face_object(msg.value, query_faces_message.value, self.scale)

                        tp = TopicPartition(msg.topic, msg.partition)
                        offsets = {tp: OffsetAndMetadata(msg.offset, None)}
                        frame_consumer.commit(offsets=offsets)

                        print("frame_num: {},camera_num: {}, latency: {}, y_hat: {}".format(
                            result["frame_num"],
                            result["camera"],
                            result["latency"],
                            result["prediction"]
                        ))
                        log_file.write("{},{},{},{},{},{}".format(result["camera"],
                                                                  result["frame_num"],
                                                                  result["prediction"],
                                                                  SET_PARTITIONS,
                                                                  result["latency"],
                                                                  "\n"))

                        # camera specific topic
                        prediction_topic = "{}_{}".format(PREDICTION_TOPIC_PREFIX, result["camera"])

                        prediction_producer.send(prediction_topic, key=result["frame_num"], value=result)

                    prediction_producer.flush()

        except KeyboardInterrupt as e:
            print("Closing Stream")
            frame_consumer.close()
            if str(self.name) == "1":
                log_file.close()

        finally:
            print("Closing Stream")
            frame_consumer.close()
            log_file.close()


    @staticmethod
    def get_processed_frame_object(frame_obj, scale=1.0):
        """Processes value produced by producer, returns prediction with png image.

        :param frame_obj: frame dictionary with frame information and frame itself
        :param scale: (0, 1] scale image before face recognition, speeds up processing, decreases accuracy
        :return: A dict updated with faces found in that frame, i.e. their location and encoding.
        """

        frame = np_from_json(frame_obj, prefix_name=ORIGINAL_PREFIX)  # frame_obj = json
        frame = cv2.cvtColor(frame.astype(np.uint8), cv2.COLOR_BGR2RGB)

        if scale != 1:
            rgb_small_frame = cv2.resize(frame, (0, 0), fx=scale, fy=scale)

        else:
            rgb_small_frame = frame

        with timer("PROCESS RAW FRAME {}".format(frame_obj["frame_num"])):
            with timer("Locations in frame"):
                face_locations = np.array(face_recognition.face_locations(rgb_small_frame))
                face_locations_dict = np_to_json(face_locations, prefix_name="face_locations")

            with timer("Encodings in frame"):
                face_encodings = np.array(face_recognition.face_encodings(rgb_small_frame, face_locations))
                face_encodings_dict = np_to_json(face_encodings, prefix_name="face_encodings")

        frame_obj.update(face_locations_dict)
        frame_obj.update(face_encodings_dict)

        return frame_obj
