import time
from kafka_frame.params import *
from kafka_frame.produce_frame import StreamVideo
from kafka_frame.utils import clear_topic, set_topic, get_video_feed_url , get_png

clear_topic(TARGET_FACE_TOPIC)

if CLEAR_PRE_PROCESS_TOPICS:
    clear_topic(FRAME_TOPIC)
    clear_topic(PROCESSED_FRAME_TOPIC)
    print("done cleaning")

set_topic(FRAME_TOPIC,SET_PARTITIONS)
set_topic(PROCESSED_FRAME_TOPIC,SET_PARTITIONS)
time.sleep(5)

CAMERA_URLS = [get_video_feed_url(i, folder="tracking") for i in CAMERAS]

PRODUCERS = [StreamVideo(url, FRAME_TOPIC, SET_PARTITIONS,
                         use_cv2=USE_RAW_CV2_STREAMING,
                         verbose=True,
                         pub_obj_key=ORIGINAL_PREFIX,
                         rr_distribute=ROUND_ROBIN) for url in
             CAMERA_URLS]

for p in PRODUCERS:
    p.start()

