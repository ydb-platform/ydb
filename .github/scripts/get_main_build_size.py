#!/usr/bin/env python3

import configparser
import os
import ydb


dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../config/ydb_qa_db.ini"
config.read(config_file_path)

build_preset = os.environ.get("build_preset")
branch = os.environ.get("branch_to_compare")

DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]


def get_build_size(time_of_current_commit):
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print(
            "Error: Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping"
        )
        return 0
    else:
        # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
        # So, set up it locally
        os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ[
            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        ]

    sql = f"""
    --!syntax_v1
    select git_commit_time,github_sha,size_bytes,size_stripped_bytes,build_preset from binary_size 
    where 
    github_workflow like "Postcommit%" and 
    github_ref_name="{branch}" and 
    build_preset="{build_preset}" and
    git_commit_time <= DateTime::FromSeconds({time_of_current_commit})
    order by git_commit_time desc
    limit 1;    
    """

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )
        with session.transaction() as transaction:
            result = transaction.execute(sql, commit_tx=True)
            if result[0].rows:
                for row in result[0].rows:
                    main_data = {}
                    for field in row:
                        main_data[field] = (
                            row[field]
                            if type(row[field]) != bytes
                            else row[field].decode("utf-8")
                        )
            else:
                print(
                    f"Error: Cant get binary size in db with params: github_workflow like 'Postcommit%', github_ref_name='{branch}', build_preset='{build_preset}, git_commit_time <= DateTime::FromSeconds({time_of_current_commit})'"
                )
                return 0

        return {
            "github_sha": main_data["github_sha"],
            "git_commit_time": str(main_data["git_commit_time"]),
            "size_bytes": str(main_data["size_bytes"]),
            "size_stripped_bytes": str(main_data["size_stripped_bytes"]),
        }


if __name__ == "__main__":
    get_build_size()
