import os
import sys
import time
import datetime

from loguru import logger
from concurrent.futures import ThreadPoolExecutor

from aus_weather_data import (
    get_matching_files,
    BOMFTPPool,
    BOM_RADAR_PATH,
    BOMRadarLocation,
    BOMRadarLocationModel,
    RADAR_TYPE,
)


ROOT_PATH = "C:\\Users\\Nathan\\Downloads\\Frames"


def get_all_idrs():

    for idr_str in BOMRadarLocation.IDR_LIST:
        location: BOMRadarLocationModel = getattr(BOMRadarLocation, idr_str)
        for radar_type in location.radar_types:
            yield f"{location.base}{radar_type.value}"


def get_idr_existing_files(
    prefix: str,
    dt: datetime.datetime,
) -> list[str]:

    logger.debug(f"Checking for existing files: {prefix}")

    if prefix[-1] == "D":
        dt = dt - datetime.timedelta(days=15)
    else:
        dt = dt - datetime.timedelta(hours=26)

    idr_path = os.path.join(ROOT_PATH, prefix)

    try:
        return [
            f for f in os.listdir(idr_path) if os.path.isfile(os.path.join(idr_path, f))
        ][-100:]
    except Exception as e:
        os.makedirs(idr_path, exist_ok=True)


def get_existing_radar_files() -> dict[str, list]:

    dt = datetime.datetime.now(tz=datetime.timezone.utc)

    idr_list = get_all_idrs()

    existing_files = {idr: get_idr_existing_files(idr, dt) for idr in idr_list}

    return existing_files


def download_idr_frames_and_upload_to_s3(
    pool: BOMFTPPool,
    prefix: str,
    ftp_files: list[str],
    ignore_list: list[str],
):

    if len(ftp_files) == 0:
        return

    location: BOMRadarLocationModel = getattr(BOMRadarLocation, prefix[:-1])
    radar_type = RADAR_TYPE(prefix[-1])

    files_to_get = get_matching_files(
        ftp_files,
        [location],
        [radar_type],
        ignore_list=ignore_list,
    )

    if len(files_to_get) == 0:
        return

    for remote_file in files_to_get:

        conn = None
        while conn is None:
            try:
                conn = pool.get_connection()
            except Exception as e:
                logger.debug(f"No available connections, sleeping: {e}")
                time.sleep(2)

        try:
            logger.info(f"Downloading file: {remote_file.filename}")
            frame = conn.get_file(remote_file)
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            pool.release_connection(conn)
            continue

        pool.release_connection(conn)

        frame.save_png_to_file(os.path.join(ROOT_PATH, prefix))

        logger.info(f"Saved {frame.filename}")


def download_all_radar_frames():

    existing_files = get_existing_radar_files()

    logger.info(f"Opening Connection")
    with BOMFTPPool(connections=15) as ftp_pool:

        conn = ftp_pool.get_connection()
        logger.info(f"Getting Directory Contents")
        ftp_files = conn.get_directory_contents(BOM_RADAR_PATH)
        ftp_pool.release_connection(conn)

        logger.info(f"Starting Downloads")
        with ThreadPoolExecutor(max_workers=30) as executor:

            for idr in existing_files.keys():

                executor.submit(
                    download_idr_frames_and_upload_to_s3,
                    ftp_pool,
                    idr,
                    ftp_files,
                    existing_files[idr],
                )


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stdout, level="INFO")
    download_all_radar_frames()
