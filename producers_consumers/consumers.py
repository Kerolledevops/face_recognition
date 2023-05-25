import os.path

from kafka_frame.params import *
from kafka_frame.prediction_producer import ConsumeFrames,PredictFrames

log_path = os.path.join(MAIN_PATH,LOG_DIR)

if not os.path.isdir(log_path):
    os.makedirs(log_path)

CONSUME_FRAMES = [ConsumeFrames(frame_topic=FRAME_TOPIC,
                                processed_frame_topic=PROCESSED_FRAME_TOPIC,
                                topic_partitions=SET_PARTITIONS,
                                scale=1,
                                rr_distribute=ROUND_ROBIN) for _ in
                  range(HM_PROCESSESS)]

PREDICT_FRAMES = [PredictFrames(processed_frame_topic=PROCESSED_FRAME_TOPIC,
                                query_faces_topic=TARGET_FACE_TOPIC,
                                scale=1,
                                rr_distribute=ROUND_ROBIN) for _ in
                  range(HM_PROCESSESS)]

for p in PREDICT_FRAMES:
    p.start()

for c in CONSUME_FRAMES:
    c.start()

for c in CONSUME_FRAMES:
    c.join()

for p in PREDICT_FRAMES:
    p.join()
