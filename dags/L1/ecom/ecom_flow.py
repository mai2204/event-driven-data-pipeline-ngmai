import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from prefect import flow, task, get_run_logger
from utils.s3_helper import list_files, move_file
from utils.config import CONFIG
from dags.L1.ecom.construct_s3_key import build_ecom_key
import re
import boto3

@task
def parse_s3_key(s3_key: str):
    logger = get_run_logger()
    logger.info(f"[INPUT] s3_key = {s3_key}")
    
    pattern = r"data/ecom/(.+)/(\d{4})/(\d{2})/(\d{2})/"
    match = re.match(pattern, s3_key)

    if not match:
        logger.error(f"[ERROR] Invalid s3_key format: {s3_key}")
        raise ValueError("Invalid s3_key format")

    table = match.group(1)
    year = match.group(2)
    month = match.group(3)
    day = match.group(4)

    return {
        "table": table,
        "year": year,
        "month": month,
        "day": day,
        "date_mmddyyyy": f"{month}{day}{year}"
    }

@task
def build_file_date(info):
    return f"{info['month']}{info['day']}{info['year']}"


@task
def get_matching_files(bucket: str, tablename: str, date: str):
    logger = get_run_logger()
    s3 = boto3.client("s3")

    logger.info(f"[S3] Scanning bucket: {bucket}")
    logger.info(f"[FILTER] table={tablename}, date={date}")
    
    try:
        files = list_files(bucket)
    except Exception as e:
        logger.error(f"[ERROR] Cannot list files from bucket: {bucket}")
        logger.error(str(e))
        raise
    
    logger.info(f"[S3] Total files found: {len(files)}")

    matched_files = []
    for key in files:
        filename = key.split(".")[0]
        logger.info(f"Check key: {key}")
        logger.info(f"Check file name: {filename}")
        
        if filename.startswith(f"{tablename}_ecom_") and date in filename:
            matched_files.append(key)

    logger.info(f"Matched files: {matched_files}")

    return matched_files

@task
def process_file(source_bucket, target_bucket, key, info):
    filename = key.split("/")[-1]

    target_key = build_ecom_key(info, filename)

    move_file(source_bucket, key, target_bucket, target_key)

    print(f"Moved: {filename} → {target_key}")

@flow(name="ecom_rcv_to_l0")
def ecom_flow(s3_key: str):
    logger = get_run_logger()

    source_bucket = CONFIG["ecom"]["source_bucket"]   # rcv bucket
    target_bucket = CONFIG["target_bucket"]

    logger.info("===== START FLOW =====")
    logger.info(f"Source bucket: {source_bucket}")
    logger.info(f"Target bucket: {target_bucket}")
    
    info = parse_s3_key(s3_key)

    files = get_matching_files(
        source_bucket,
        info["table"],
        info["date_mmddyyyy"]
    )

    if not files:
        print("No files found")
        return

    for f in files:
        process_file(source_bucket, target_bucket, f, info)