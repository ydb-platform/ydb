#!/usr/bin/env python3

import copy
import datetime
import json
import os
import ydb
import uuid
import subprocess

DATABASE_PATH = "/ru-central1/b1ggceeul2pkher8vhb6/etn6d1qbals0c29ho4lf"

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

UTF8_COLUMNS = FROM_ENV_COLUMNS + [
    "id",
    "git_commit_message",
    "path",
    "build_preset",
]

DATETIME_COLUMNS = [
    "git_commit_time",
]

UINT64_COLUMNS = [
]

DOUBLE_COLUMNS = [
    "compilation_time_s"
]

ALL_COLUMNS = UTF8_COLUMNS + DATETIME_COLUMNS + UINT64_COLUMNS


def sanitize_str(s):
    return s or "N\\A"
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

    with ydb.Driver(
        endpoint="grpcs://ydb.serverless.yandexcloud.net:2135",
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(
            lambda: driver.table_client.session().create()
        )

        column_types = ydb.BulkUpsertColumns()
        for type_ in UTF8_COLUMNS:
            column_types = column_types.add_column(type_, ydb.PrimitiveType.Utf8)
        for type_ in UINT64_COLUMNS:
            column_types = column_types.add_column(type_, ydb.PrimitiveType.Uint64)
        for type_ in DATETIME_COLUMNS:
            column_types = column_types.add_column(type_, ydb.PrimitiveType.Datetime)
        for type_ in DOUBLE_COLUMNS:
            column_types = column_types.add_column(type_, ydb.PrimitiveType.Double)

        build_preset = os.environ.get("build_preset", None)
        github_sha = os.environ.get("GITHUB_SHA", None)

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
    
        common_parameters = {
            "build_preset": sanitize_str(build_preset),
            "git_commit_time": git_commit_time_unix,
            "git_commit_message": sanitize_str(git_commit_message),
        }

        for column in FROM_ENV_COLUMNS:
            value = os.environ.get(column.upper(), None)
            common_parameters[column] = sanitize_str(value)

        with open("html_cpp_impact/output.json") as f:
            cpp_stats = json.load(f)

        rows = []

        for entry in cpp_stats["cpp_compilation_times"]:
            path = entry["path"]
            time_s = entry["time_s"]
            parameters["path"] = sanitize_str(path)
            parameters["compilation_time_s"] = time_s
            parameters["id"] = str(uuid.uuid4())
            rows.append(copy.copy(parameters))
        
        driver.table_client.bulk_upsert(DATABASE_PATH + "/cpp_compile_time", rows, column_types)


if __name__ == "__main__":
    exit(main())
