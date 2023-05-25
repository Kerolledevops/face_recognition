from dotenv import load_dotenv, dotenv_values
import os

load_dotenv()
config = dotenv_values(".env.development")

MAIN_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

TARGET_FACE_TOPIC = "target_face_topic"
FRAME_TOPIC = "raw_frame_topic"
PROCESSED_FRAME_TOPIC = "processed_frame_topic"
ORIGINAL_PREFIX = "predicted"
PREDICTED_PREFIX = "predicted"
PREDICTION_TOPIC_PREFIX = "{}_{}".format("predicted_object", FRAME_TOPIC)

USE_RAW_CV2_STREAMING = False
SET_PARTITIONS = 16
ROUND_ROBIN = False

LOG_DIR = "logs"
CLEAR_PRE_PROCESS_TOPICS = True
C_FRONT_ENDPOINT = "http://d3tj01z94i74qz.cloudfront.net/"
CAMERAS = [0, 1, 2, 3, 4, 5]
TOTAL_CAMERAS = len(CAMERAS)

HM_PROCESSESS = SET_PARTITIONS // 8

# KAFKA CONSUMER PARAMS
BOOTSTRAP_SERVERS = "0.0.0.0:9092"
GROUP_ID = "view"
AUTO_OFFSET_RESET = "earliest"

BUFFER_SIZE = 180
TOPIC_PARTITIONS = 8
SCALE = 1

# CONSUMER CONFIG
GROUP_ID = "consume"
