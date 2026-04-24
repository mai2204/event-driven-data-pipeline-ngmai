from prefect_github import GitHubCredentials
import os

creds = GitHubCredentials(
    token=os.getenv("GITHUB_TOKEN")
)

creds.save("git-api", overwrite=True)