import json
import re
import sys
import time
from multiprocessing import Process

import cv2
import imutils
import numpy as np
from imutils.video import VideoStream
from kafka import KafkaProducer
from kafka.partitioner import RoundRobinPartitioner, HashedPartitioner, Murmur2Partitioner
from kafka.serializer import Serializer, Deserializer
from kafka.structs import TopicPartition, OffsetAndMetadata
from .utils import np_to_json
from .params import *


class StreamVideo(Process):

    def __init__(self, video_uri,
                 topic,
                 topic_partitions=TOPIC_PARTITIONS,
                 use_cv2=False,
                 pub_obj_key="original",
                 group=None,
                 target=None,
                 name=None,
                 verbose=False,
                 rr_distribute=False):
        """Video Streaming Producer Process Class. Publishes frames from a video source to a topic.

        :param video_uri: video path or url
        :param topic: kafka topic to publish stamped encoded frames.
        :param topic_partitions: number of partitions this topic has, for distributing messages among partitions
        :param use_cv2: send every frame, using cv2 library, else will use imutils to speedup training
        :param pub_obj_key: associate tag with every frame encoded, can be used later to separate raw frames
        :param group: group should always be None; it exists solely for compatibility with threading.
        :param target: Process Target
        :param name: Process name
        :param verbose: print logs on stdout
        :param rr_distribute: use round robin partitioner, should be set same as consumers.

        """

        super().__init__(group=group, target=target, name=name)

        # URL for streaming video
        self.video_uri = video_uri
        # TOPIC TO PUBLISH
        self.frame_topic = topic
        self.topic_partitions = topic_partitions
        self.num_camera = int(re.findall(r"StreamVideo-([0-9]*)", self.name)[0])
        self.use_cv2 = use_cv2
        self.object_key = pub_obj_key
        self.verbose = verbose
        self.rr_distribute = rr_distribute

    def run(self):
        """Publish video frames as json objects, timestamped, marked with camera number.

        Source:
            self.video_path: URL for streaming video
            self.kwargs["use_cv2"]: use raw cv2 streaming, set to false to use smart fast streaming --> not every frame is sent.
        Publishes:
            A dict {"frame": string(base64encodedarray), "dtype": obj.dtype.str, "shape": obj.shape,
                    "timestamp": time.time(), "camera": camera, "frame_num": frame_num}
        """

        if self.rr_distribute:
            partitioner = RoundRobinPartitioner(partitions=
                                                [TopicPartition(topic=self.frame_topic, partition=i)
                                                 for i in range(self.topic_partitions)])

        else:
            partitioner = Murmur2Partitioner(
                partitions=[TopicPartition(topic=self.frame_topic, partition=i)
                            for i in range(self.topic_partitions)]
            )

        frame_producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVERS],
                                       key_serializer=lambda key: str(key).encode(),
                                       value_serializer=lambda value: json.dumps(value).encode(),
                                       partitioner=partitioner)

        print("[CAM {}] URL: {}, SET PARTITIONS FOR FRAME TOPIC: {}".format(self.num_camera,
                                                                            self.video_path,
                                                                            frame_producer.partitions_for(
                                                                                self.frame_topic)))

        video = cv2.VideoCapture(self.video_uri) if self.use_cv2 else VideoStream(self.video_uri).start()

        frame_num = 0
        start_time = time.time()
        print("[CAM {} START TIME {}:".format(self.num_camera, start_time))

        while True:  # read uri , transform , publish
            if self.use_cv2:
                success, image = video.read()
                if not success:
                    if self.verbose:
                        print("{CAM {} URL:{} , end frame: {}".format(
                            self.name,
                            self.video_uri,
                            frame_num
                        ))
                        break
            else:  # using smart ,only unique frames skip
                image = video.read()
                if self.verbose:
                    print("{CAM {} URL:{} , end frame: {}".format(
                        self.name,
                        self.video_uri,
                        frame_num
                    ))
                    break
            message = self.transform(frame=image,
                                     frame_num=frame_num,
                                     object_key=self.object_key,
                                     camera=self.num_camera,
                                     verbose=self.verbose)

            part = frame_num % self.topic_partitions
            # Logging
            if self.verbose:
                print("\r[PRODUCER][Cam {}] FRAME: {} TO PARTITION: {}".format(message["camera"],
                                                                               frame_num, part), end="")
            # Publish to specific partition
            frame_producer.send(self.frame_topic, key="{}_{}".format(self.num_camera, frame_num), value=message)

            # if frame_num % 1000 == 0:
            frame_producer.flush()

            frame_num += 1

            # clear the capture
        if self.use_cv2:
            video.release()
        else:
            video.stop()

        if self.verbose:
            print("[CAM {}] FINISHED. STREAM TIME {}: ".format(self.num_camera, time.time() - start_time))

        return True if frame_num > 0 else False


        def admin():
            return "admin"

        @staticmethod
        def transform(frame, frame_num, object_key="original", camera=0, verbose=False):
            """Serialize frame, create json message with serialized frame, camera number and timestamp.

            :param frame: numpy.ndarray, raw frame
            :param frame_num: frame number in the particular video/camera
            :param object_key: identifier for these objects
            :param camera: Camera Number the frame is from
            :param verbose: print out logs
            :return: A dict {"frame": string(base64encodedarray), "dtype": obj.dtype.str, "shape": obj.shape,
                        "timestamp": time.time(), "camera": camera, "frame_num": frame_num}
            """
            frame = imutils.resize(frame, width=400)

            if verbose:
                # print raw frame size
                print("\nRAW ARRAY SIZE: ", sys.getsizeof(frame))

            # serialize frame
            frame_dict = np_to_json(frame.astype(np.uint8), prefix_name=object_key)
            # Metadata for frame
            message = {"timestamp": time.time(), "camera": camera, "frame_num": frame_num}
            # add frame and metadata related to frame
            message.update(frame_dict)

            if verbose:
                # print message size
                print("\nMESSAGE SIZE: ", sys.getsizeof(message))

            return message
