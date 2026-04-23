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
    import re
    pattern = r"data/ecom/(.+)/(\d{4})/(\d{2})/(\d{2})/"

    match = re.match(pattern, s3_key)

    if not match:
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
        "date_mmddyyyy": f"{month}{day}{year}  # 04092026"
    }

@task
def build_file_date(info):
    return f"{info['month']}{info['day']}{info['year']}"


@task
def get_matching_files(bucket: str, table: str, date: str):
    import boto3
    from prefect import get_run_logger

    logger = get_run_logger()
    s3 = boto3.client("s3")

    logger.info(f"Searching in bucket: {bucket}")

    response = s3.list_objects_v2(Bucket=bucket)
    contents = response.get("Contents", [])

    matched_files = []
    print(table)

    for obj in contents:
        key = obj["Key"]# order_reviews_ecom_04092026.csv
        filename = key.split(".")[-1]
        print(filename)
        print(key)
        if filename.startswith(f"{table}_ecom_") and date in filename:
            matched_files.append(key)

    logger.info(f"Matched files: {matched_files}")

    return matched_files

@task
def process_file(source_bucket, target_bucket, key, info):
    from utils.s3_helper import move_file
    from dags.L1.ecom.construct_s3_key import build_ecom_key

    filename = key.split("/")[-1]

    target_key = build_ecom_key(info, filename)

    move_file(source_bucket, key, target_bucket, target_key)

    print(f"Moved: {filename} → {target_key}")

@flow(name="ecom_rcv_to_l0")
def ecom_flow(s3_key: str):

    source_bucket = CONFIG["ecom"]["source_bucket"]   # rcv bucket
    target_bucket = CONFIG["target_bucket"]

    print("INPUT s3_key =", s3_key)

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