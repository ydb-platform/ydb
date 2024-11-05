#!/usr/bin/env python3

import configparser
import datetime
import os
import ydb
import uuid
import subprocess

dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = f"{dir}/../config/ydb_qa_db.ini"
config.read(config_file_path)

YDBD_PATH = config["YDBD"]["YDBD_PATH"]
DATABASE_ENDPOINT = config["QA_DB"]["DATABASE_ENDPOINT"]
DATABASE_PATH = config["QA_DB"]["DATABASE_PATH"]

FROM_ENV_COLUMNS = [
    "github_head_ref",
    "github_workflow",
    "github_workflow_ref",
    "github_sha",
    "github_repository",
    "github_event_name",
    "github_ref_type",
    "github_ref_name",
    "github_ref",
]

STRING_COLUMNS = FROM_ENV_COLUMNS + [
    "id",
    "git_commit_message",
    "binary_path",
    "build_preset",
]

DATETIME_COLUMNS = [
    "git_commit_time",
]

UINT64_COLUMNS = [
    "size_bytes",
    "size_stripped_bytes",
]

ALL_COLUMNS = STRING_COLUMNS + DATETIME_COLUMNS + UINT64_COLUMNS


def sanitize_str(s):
    # YDB SDK expects bytes for 'String' columns
    if s is None:
        s = "N\A"
    return s.encode("utf-8")


def main():
    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print("Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
        return 1
    
    # Do not set up 'real' variable from gh workflows because it interfere with ydb tests 
    # So, set up it locally
    os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ["CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]

    if not os.path.exists(YDBD_PATH):
        # can be possible due to incremental builds and ydbd itself is not affected by changes
        print("{} not exists, skipping".format(YDBD_PATH))
        return 0

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )
        with session.transaction() as tx:
            text_query_builder = []
            for type_ in STRING_COLUMNS:
                text_query_builder.append("DECLARE ${} as String;".format(type_))
            for type_ in UINT64_COLUMNS:
                text_query_builder.append("DECLARE ${} as Uint64;".format(type_))
            for type_ in DATETIME_COLUMNS:
                text_query_builder.append("DECLARE ${} as Datetime;".format(type_))

            text_query_builder.append(
                """INSERT INTO binary_size
(
    {}
)
VALUES
(
    {}                   
);                         
""".format(
                    ", \n    ".join(ALL_COLUMNS),
                    ", \n    ".join(["$" + column for column in ALL_COLUMNS]),
                )
            )

            text_query = "\n".join(text_query_builder)

            prepared_query = session.prepare(text_query)

            binary_size_bytes = subprocess.check_output(
                ["bash", "-c", "cat {} | wc -c".format(YDBD_PATH)]
            )
            binary_size_stripped_bytes = subprocess.check_output(
                ["bash", "-c", "./ya tool strip {} -o - | wc -c".format(YDBD_PATH)]
            )

            build_preset = os.environ.get("build_preset", None)
            github_sha = os.environ.get("commit_git_sha", None)

            if github_sha is not None:
                git_commit_time_bytes = subprocess.check_output(
                    ["git", "show", "--no-patch", "--format=%cI", github_sha]
                )
                git_commit_message_bytes = subprocess.check_output(
                    ["git", "log", "--format=%s", "-n", "1", github_sha]
                )
                git_commit_time = datetime.datetime.fromisoformat(
                    git_commit_time_bytes.decode("utf-8").strip()
                )
                git_commit_message = git_commit_message_bytes.decode("utf-8").strip()
                git_commit_time_unix = int(git_commit_time.timestamp())
            else:
                git_commit_time = None
                git_commit_message = None
                git_commit_time_unix = 0

            parameters = {
                "$id": sanitize_str(str(uuid.uuid4())),
                "$build_preset": sanitize_str(build_preset),
                "$binary_path": sanitize_str(YDBD_PATH),
                "$size_stripped_bytes": int(binary_size_stripped_bytes.decode("utf-8")),
                "$size_bytes": int(binary_size_bytes.decode("utf-8")),
                "$git_commit_time": git_commit_time_unix,
                "$git_commit_message": sanitize_str(git_commit_message),
            }

            for column in FROM_ENV_COLUMNS:
                value = os.environ.get(column.upper(), None)
                parameters["$" + column] = sanitize_str(value)
            
            #workaround for https://github.com/ydb-platform/ydb/issues/5294
            parameters["$github_sha"] = sanitize_str(github_sha)
            
            print("Executing query:\n{}".format(text_query))
            print("With parameters:")
            for k, v in parameters.items():
                print("{}: {}".format(k, v))

            tx.execute(prepared_query, parameters, commit_tx=True)


if __name__ == "__main__":
    exit(main())
