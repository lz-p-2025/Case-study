"""
This script is for a data pipeline to read data from given github repos and list
pull request information for the last 30 days.
"""

import requests
import json
from datetime import datetime, timezone, timedelta
import re
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

# TODO: configure logging for spark session. This would enable better monitoring
# and debugging
# LOG = logging.getLogger()

SPARK=SparkSession.builder.getOrCreate()

# If reading from private repositories then would require
# getting a personal access token from the environment variables that has the permissions
# to read from the repository. Would also require `import os`
# GITHUB_TOKEN=os.getenv("GITHUB_TOKEN")

# Set the list of repos to pull data from
# This is set locally for this task, for better control it would be in a configuration
# file or even in a data table that is static, depending on the use case.
# The elements must be of the format owner/repo.
REPO_LIST=[
    "octocat/Spoon-Knife",
    "octocat/Hello-World"
]


# Define the headers for the request
# Since we are looking at public repositories in this case then we don't need to
# authenticate. But if authentication was required then an extra header would be used
# "Authorization": "Bearer GITHUB_TOKEN"
HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}


def load_raw_data(repo: str) -> None:
    """
    Function that inputs a repo and loads the pull requests from the last 30 days
    and stores this locally.
    """
    current_timestamp = datetime.now(timezone.utc)

    # Filter to the time_delta that is being requested
    start_date = (current_timestamp - timedelta(days=30)).date()

    # We use the "search" endpoint as we are limiting on 30 days of data, the "pull"
    # endpoint could be used if wanting to get all data for pull requests. We construct
    # a query for this. We are only interested in new PRs that have been created in the
    # last 30 days
    query = (
        f"repo:{repo} is:pr created:>={start_date}"
    )
    url = f"https://api.github.com/search/issues"

    # Note: in a production environment proper logging statements would be setup
    # rather than local print.
    print(f"Loading API responses for {url} with query {query}")

    response = requests.get(url, headers=HEADERS, params={"q": query})
    response.raise_for_status()
    pull_requests = response.json()

    # Output the data to the src layer
    # For the time being it is as json for simplicity,
    # which can be read into pyspark dataframes

    # Replace the / with _ in the repo name
    repo_path_name = repo.replace("/", "_")
    safe_timestamp = re.sub(r'[: ]', '_', str(current_timestamp))
    output_path=f"data/src/{repo_path_name}_{safe_timestamp}.json"
    with open(output_path, "w") as file:
        json.dump(pull_requests, file)


def refine_raw_data(src_path: str) -> None:
    """
    This reads all the json files in a directory, transforms it, and uploads it to
    a local location.
    """
    df = (
        SPARK.read.json(src_path)
        .selectExpr("explode(items) as item").select("item.*")
        # Explicitly select the columns that we are interested in for further analysis
        .select(
            "url"
            , "repository_url"
            , "id"
            , "number"
            , "title"
            # The user information could be stored in a separate table and be joined
            # on the user.id. Here we extract the id and login as an example.
            , f.col("user.id").alias("user_id")
            , f.col("user.login").alias("user_login")
            # Arrays are explicitly cast to be compatible with csv export
            # But in something like databricks this can stay as nested data
            , f.to_json("labels").alias("labels")
            , "state"
            , "locked"
            , "assignee"
            , f.to_json("assignees").alias("assignees")
            # Explicit timestamp casting so that it can be used in further transformations
            , f.to_timestamp("created_at").alias("created_at")
            , f.to_timestamp("updated_at").alias("updated_at")
            , f.to_timestamp("closed_at").alias("closed_at")
        )
        # This is for deduplication
        .distinct()
        # create a column to indicate the length of time the PR has been open
        .withColumn("days_open",
            f.when(f.col("closed_at").isNull(), f.lit(None))
            .otherwise((f.unix_timestamp("closed_at") - f.unix_timestamp("created_at")) / 86400)
        )
    )

    # For debugging
    df.printSchema()

    # For simplicity, it is writing a file locally. But in production environment this
    # would be an upsert using syntax such as
    # (
    #     target_table.alias("old")
    #     .merge(
    #         df.alias("new"),
    #         "old.id = new.id"
    #     )
    #     .whenMatchedUpdateAll()
    #     .whenNotMatchedInsertAll()
    # )
    # And the PR number and repo could be used as the merge keys

    full_file_path="data/agg/pull_requests"

    # Write it out as one partition for now, on a larger scale could use the repo
    # name as a partition. .coalesce(1) is to keep it in one partition and one file
    (df.coalesce(1).write.mode("overwrite").csv(full_file_path, header=True))

    print(f"{df.count()} rows outputted to file {full_file_path}")



def main():
    """
    This is the entry point to run the pipeline.

    This setup is not scalable and for larger data it would be each layer having
    a separate job and either triggered between them or timed schedules for each one.
    """
    # Bronze layer
    for repo in REPO_LIST:
        load_raw_data(repo=repo)

    # Silver layer
    refine_raw_data(src_path="data/src/*")

    # Gold layer - not implemented



if __name__ == "__main__":
    main()

