from prefect_aws import AwsCredentials
import os


def create_aws_block():
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-1")

    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials not found in environment variables")

    aws_creds = AwsCredentials(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    aws_creds.save("aws-credentials", overwrite=True)

    print("AWS block 'aws-credentials' created successfully!")


if __name__ == "__main__":
    create_aws_block()