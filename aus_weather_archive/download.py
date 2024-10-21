import boto3
import time
import datetime

from loguru import logger
from concurrent.futures import ThreadPoolExecutor


from aus_weather_data import (
    get_matching_files,
    BOMFTPPool,
    BOM_RADAR_PATH,
    BOMFTPConn,
    BOMRadarLocation,
    BOMRadarLocationModel,
    RADAR_TYPE,
    BOMRadarFrameMetadata,
    BOMRadarFramePNG,
)


def get_all_idrs():

    for idr_str in BOMRadarLocation.IDR_LIST:
        location: BOMRadarLocationModel = getattr(BOMRadarLocation, idr_str)
        for radar_type in location.radar_types:
            yield f"{location.base}{radar_type.value}"


def get_idr_existing_files(
    s3_client: boto3.client, prefix: str, dt: datetime.datetime
) -> list[str]:

    logger.debug(f"Checking for existing files: {prefix}")

    if prefix[-1] == "D":
        dt = dt - datetime.timedelta(days=15)
    else:
        dt = dt - datetime.timedelta(hours=26)

    dt_str = dt.strftime("%Y%m%d%H%M")
    filename_start = f"{prefix}.T.{dt_str}.png"

    paginator = s3_client.get_paginator("list_objects_v2")
    paginator = paginator.paginate(
        Bucket="radar-frames-829280336906",
        Prefix=f"{prefix}/",
        StartAfter=f"{prefix}/{filename_start}",
    )

    return [
        item["Key"].split("/")[-1]
        for page in paginator
        for item in page.get("Contents", [])
    ]


def get_existing_radar_files(s3_client: boto3.client) -> dict[str, list]:

    dt = datetime.datetime.now(tz=datetime.timezone.utc)

    idr_list = get_all_idrs()

    existing_files = {
        idr: get_idr_existing_files(s3_client, idr, dt) for idr in idr_list
    }

    return existing_files


def download_idr_frames_and_upload_to_s3(
    pool: BOMFTPPool,
    s3_client: boto3.client,
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

        s3_client.put_object(
            Bucket="radar-frames-829280336906",
            Key=f"{prefix}/{frame.filename}",
            Body=frame.png_data,
        )
        logger.info(f"Uploaded {frame.filename}")


def download_all_radar_frames():

    s3_client = boto3.client("s3")
    existing_files = get_existing_radar_files(s3_client)

    with BOMFTPPool(connections=15) as ftp_pool:

        conn = ftp_pool.get_connection()
        ftp_files = conn.get_directory_contents(BOM_RADAR_PATH)
        ftp_pool.release_connection(conn)

        with ThreadPoolExecutor(max_workers=30) as executor:

            for idr in existing_files.keys():

                executor.submit(
                    download_idr_frames_and_upload_to_s3,
                    ftp_pool,
                    s3_client,
                    idr,
                    ftp_files,
                    existing_files[idr],
                )
