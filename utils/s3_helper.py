import boto3
from prefect_aws import AwsCredentials

aws_creds = AwsCredentials.load("aws-credentials")
s3_client = aws_creds.get_boto3_session().client("s3")
def list_files(bucket, s3_client):
    response = s3_client.list_objects_v2(Bucket=bucket)
    return [obj["Key"] for obj in response.get("Contents", [])]

def move_file(source_bucket, key, target_bucket, target_key):
    s3_client.copy_object(
        Bucket=target_bucket,
        CopySource={'Bucket': source_bucket, 'Key': key},
        Key=target_key
    )

    s3_client.delete_object(
        Bucket=source_bucket,
        Key=key
    )