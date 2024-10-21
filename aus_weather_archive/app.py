import os
import sys

from loguru import logger

from download import download_all_radar_frames

logger.remove()
logger.add(sys.stdout, level="INFO")


def lambda_handler(event, context):

    download_all_radar_frames()
    return {"statusCode": 200, "body": f"Hello, World!"}


if __name__ == "__main__":
    lambda_handler(None, None)
