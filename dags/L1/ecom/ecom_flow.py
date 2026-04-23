import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from prefect import flow, task
from utils.s3_helper import list_files, move_file
from utils.parser import parse_ecom_filename
from utils.config import CONFIG
from dags.L1.ecom.construct_s3_key import build_ecom_key
import re

@task
def parse_s3_key(s3_key: str):
    from prefect import get_run_logger
    logger = get_run_logger()

    logger.info(f"Parsing s3_key: {s3_key}")
    parts = s3_key.strip("/").split("/")

    if len(parts) < 6:
        logger.error("Invalid format detected!")
        raise ValueError(f"Invalid s3_key format: {s3_key}")

    return {
        "system": parts[1],
        "table": parts[2],
        "year": parts[3],
        "month": parts[4],
        "day": parts[5],
    }

@task
def build_file_date(info):
    return f"{info['month']}{info['day']}{info['year']}"


@task
def get_matching_files(bucket: str, table: str, date: str):
    files = list_files(bucket)

    matched = []

    for f in files:
        filename = f.split("/")[-1]

        if filename.startswith("trigger_"):
            continue

        if table in filename and date in filename:
            matched.append(f)

    print("Matched:", matched)
    return matched

@task
def process_file(bucket, target_bucket, key):
    filename = key.split("/")[-1]

    info = parse_ecom_filename(filename)

    if not info:
        print(f"Skip invalid file: {filename}")
        return

    target_key = build_ecom_key(info, filename)

    move_file(bucket, key, target_bucket, target_key)

    print(f"Moved: {filename}")
    
from prefect import get_run_logger
@flow(name="ecom_rcv_to_l0")
def ecom_flow(s3_key: str):
    logger = get_run_logger()

    logger.info(f"INPUT s3_key = {s3_key}")
    bucket = CONFIG["ecom"]["source_bucket"]
    target_bucket = CONFIG["target_bucket"]

    info = parse_s3_key(s3_key)

    file_date = build_file_date(info)

    files = get_matching_files(
        bucket,
        info["table"],
        file_date
    )

    if not files:
        print("No files found")
        return
    
    for f in files:
        process_file(bucket, target_bucket, f)