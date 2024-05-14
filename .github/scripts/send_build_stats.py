#!/usr/bin/env python3

import datetime
import os
import ydb
import uuid
import subprocess

"""
CREATE TABLE binary_size (
    id String NOT NULL,
    github_head_ref String,
    github_workflow String,
    github_workflow_ref String,

    github_sha String,
    github_repository String,

    github_event_name String,

    github_ref_type String,
    github_ref_name String,
    github_ref String,

    binary_path String,
    size_bytes Uint64,
    size_stripped_bytes Uint64,
    build_type String,

    PRIMARY KEY (id)
)
"""

YDBD_PATH = "ydb/apps/ydbd/ydbd"

from_env_columns = [
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

string_columns = from_env_columns + [
    "id",
    "git_commit_message",
    "binary_path",
    "build_type",
]

datetime_colums = [
    "git_commit_time",
]

uint64_columns = [
    "size_bytes",
    "size_stripped_bytes",
]

all_columns = string_columns + datetime_colums + uint64_columns


def sanitize_str(s):
    # YDB SDK expects bytes for 'String' columns
    if s is None:
        s = "N\A"
    return s.encode("utf-8")


def main():
    """
    os.environ[
        "YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
    ] = "/tmp/ydb_service_account.json"
    """

    with ydb.Driver(
        endpoint="grpcs://ydb.serverless.yandexcloud.net:2135",
        database="/ru-central1/b1ggceeul2pkher8vhb6/etn6d1qbals0c29ho4lf",
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )
        with session.transaction() as tx:
            text_query_builder = []
            for type_ in string_columns:
                text_query_builder.append("DECLARE ${} as String;".format(type_))
            for type_ in uint64_columns:
                text_query_builder.append("DECLARE ${} as Uint64;".format(type_))
            for type_ in datetime_colums:
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
                    ", \n    ".join(all_columns),
                    ", \n    ".join(["$" + column for column in all_columns]),
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

            build_type = os.environ.get("build_type", None)
            github_sha = os.environ.get("GITHUB_SHA", None)

            if github_sha is not None:
                git_commit_time_bytes = subprocess.check_output(
                    ["git", "show", "--no-patch", "--format=%cI", github_sha]
                )
                git_commit_message_bytes = subprocess.check_output(
                    ["git", "log", "--format=%B", "-n", "1", github_sha]
                )
                git_commit_time = datetime.datetime.fromisoformat(
                    git_commit_time_bytes.decode("utf-8").strip()
                )
                git_commit_message = git_commit_message_bytes.decode("utf-8")
                git_commit_time_unix = int(git_commit_time.timestamp())
            else:
                git_commit_time = None
                git_commit_message = None
                git_commit_time_unix = 0

            parameters = {
                "$id": sanitize_str(str(uuid.uuid4())),
                "$build_type": sanitize_str(build_type),
                "$binary_path": sanitize_str(YDBD_PATH),
                "$size_stripped_bytes": int(binary_size_bytes.decode("utf-8")),
                "$size_bytes": int(binary_size_stripped_bytes.decode("utf-8")),
                "$git_commit_time": git_commit_time_unix,
                "$git_commit_message": sanitize_str(git_commit_message),
            }

            for column in from_env_columns:
                value = os.environ.get(column.upper(), None)
                parameters["$" + column] = sanitize_str(value)

            print("Executing query:\n{}".format(text_query))
            print("With parameters:")
            for k, v in parameters.items():
                print("{}: {}".format(k, v))

            result_sets = tx.execute(prepared_query, parameters, commit_tx=True)


if __name__ == "__main__":
    main()
