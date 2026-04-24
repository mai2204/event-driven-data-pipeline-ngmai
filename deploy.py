from prefect import flow
import boto3
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials
# from prefect.deployments import Deployment
from prefect_aws import AwsCredentials

def deploy():
 # 1. Load the GitHub credentials block
 # Note: Ensure the block name matches what you created in the UI
    github_creds = GitHubCredentials.load("git-api")

 # 2. Load the AWS credentials block
 # Note: Ensure the block name matches what you created in the UI
    aws_creds = AwsCredentials.load("aws-credentials")

 # 2. Define the remote source (The "Pull" step)
    source = GitRepository(
        url="https://github.com/mai2204/RCV-to-L0-Prefect-AWS.git",
        branch="main",
        credentials=github_creds
    )
 # 2. Define the S3 source (The "Pull" step)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_creds.aws_access_key_id,
        aws_secret_access_key=aws_creds.aws_secret_access_key,
        region_name=aws_creds.region_name
)

 # 3. Create the deployment from the source
 # .from_source() replaces the 'pull' section of your YAML
    my_flow = flow.from_source(
        source=source,
        entrypoint="dags/L1/ecom/ecom_flow.py:ecom_flow"
    )

 # 4. Ship it to Prefect Cloud (The 'deployments' section)
    my_flow.deploy(
        name="ecom-deploy",
        work_pool_name="Serverless",
        job_variables={
            "pip_packages": ["boto3"] 
        }
    )

    print(my_flow)

if __name__ == "__main__":
    deploy()