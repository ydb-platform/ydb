#!/usr/bin/env python3

import argparse
import copy
import datetime
import json
import os
import ydb
import uuid
import subprocess

DATABASE_PATH = "/ru-central1/b1ggceeul2pkher8vhb6/etnvsjbk7kh1jc6bbfi8"
DATABASE_ENDPOINT = "grpcs://lb.etnvsjbk7kh1jc6bbfi8.ydb.mdb.yandexcloud.net:2135"

FROM_ENV_COLUMNS = [
    "GITHUB_HEAD_REF",
    "GITHUB_WORKFLOW",
    "GITHUB_WORKFLOW_REF",
    "GITHUB_SHA",
    "GITHUB_REPOSITORY",
    "GITHUB_EVENT_NAME",
    "GITHUB_REF_TYPE",
    "GITHUB_REF_NAME",
    "GITHUB_REF",
    "build_preset",
    "build_target",
]

UTF8_COLUMNS = [val.lower() for val in FROM_ENV_COLUMNS] + [
    "id",
    "git_commit_message",
    "path",
    "sub_path",
]

DATETIME_COLUMNS = [
    "git_commit_time",
]

UINT64_COLUMNS = [
    "inclusion_count",
]

DOUBLE_COLUMNS = [
    "total_compilation_time_s",
    "compilation_time_s",
    "mean_compilation_time_s",
    "total_time_s",
]

ALL_COLUMNS = UTF8_COLUMNS + DATETIME_COLUMNS + UINT64_COLUMNS


def sanitize_str(s):
    return s or "N\\A"


def generate_column_types(row):
    column_types = ydb.BulkUpsertColumns()
    for column_name in row:
        if column_name in UTF8_COLUMNS:
            column_types = column_types.add_column(column_name, ydb.PrimitiveType.Utf8)
        elif column_name in UINT64_COLUMNS:
            column_types = column_types.add_column(column_name, ydb.PrimitiveType.Uint64)
        elif column_name in DOUBLE_COLUMNS:
            column_types = column_types.add_column(column_name, ydb.PrimitiveType.Double)
        elif column_name in DATETIME_COLUMNS:
            column_types = column_types.add_column(column_name, ydb.PrimitiveType.Datetime)
        else:
            assert False
    return column_types

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# We split bulk upsert because of 
# https://github.com/ydb-platform/ydb-python-sdk/issues/460
BULK_UPSERT_CHUNK_SIZE = 5000

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--html-dir-cpp",
        required=True,
        help="Path to treemap view of compilation times",
    )
    parser.add_argument(
        "-i",
        "--html-dir-headers",
        required=False,
        default="html_headers_impact",
        help="Path to treemap view of headers impact on cpp compilation",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" not in os.environ:
        print("Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
        return 1

    # Do not set up 'real' variable from gh workflows because it interfere with ydb tests
    # So, set up it locally
    os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = os.environ["CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]

    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)

        build_preset = os.environ.get("build_preset", None)
        github_sha = os.environ.get("GITHUB_SHA", None)

        if github_sha is not None:
            git_commit_time_bytes = subprocess.check_output(["git", "show", "--no-patch", "--format=%cI", github_sha])
            git_commit_message_bytes = subprocess.check_output(["git", "log", "--format=%s", "-n", "1", github_sha])
            git_commit_time = datetime.datetime.fromisoformat(git_commit_time_bytes.decode("utf-8").strip())
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
            value = os.environ.get(column, None)
            common_parameters[column.lower()] = sanitize_str(value)

        with open(os.path.join(args.html_dir_cpp, "output.json")) as f:
            cpp_stats = json.load(f)

        with open(os.path.join(args.html_dir_headers, "output.json")) as f:
            header_stats = json.load(f)

        rows = []

        # upload into cpp_compile_time
        for entry in cpp_stats["cpp_compilation_times"]:
            path = entry["path"]
            time_s = entry["time_s"]
            row = copy.copy(common_parameters)
            row["path"] = sanitize_str(path)
            row["compilation_time_s"] = time_s
            row["id"] = str(uuid.uuid4())
            rows.append(copy.copy(row))
        
        for chunk_rows in chunks(rows, BULK_UPSERT_CHUNK_SIZE):
            row = chunk_rows[0]
            driver.table_client.bulk_upsert(
                DATABASE_PATH + "/code-agility/cpp_compile_time", chunk_rows, generate_column_types(row)
            )

        # upload into total_compile_time
        row = copy.copy(common_parameters)
        row["id"] = str(uuid.uuid4())
        row["total_compilation_time_s"] = cpp_stats["total_compilation_time"]

        driver.table_client.bulk_upsert(
            DATABASE_PATH + "/code-agility/total_compile_time", [row], generate_column_types(row)
        )

        # upload into headers_impact
        rows = []
        for entry in header_stats["headers_compile_duration"]:
            path = entry["path"]
            inclusion_count = entry["inclusion_count"]
            mean_compilation_time_s = entry["mean_compilation_time_s"]
            row = copy.copy(common_parameters)
            row["id"] = str(uuid.uuid4())
            row["path"] = sanitize_str(path)
            row["mean_compilation_time_s"] = mean_compilation_time_s
            row["inclusion_count"] = inclusion_count
            rows.append(copy.copy(row))

        for chunk_rows in chunks(rows, BULK_UPSERT_CHUNK_SIZE):
            row = chunk_rows[0]
            driver.table_client.bulk_upsert(
                DATABASE_PATH + "/code-agility/headers_impact", chunk_rows, generate_column_types(row)
            )

        # upload into compile_breakdown
        rows = []
        for path in header_stats["time_breakdown"]:
            entry = header_stats["time_breakdown"][path]
            for sub_entry in entry:
                sub_path = sub_entry["path"]
                inclusion_count = sub_entry["inclusion_count"]
                total_time_s = sub_entry["total_time_s"]

                row = copy.copy(common_parameters)
                row["id"] = str(uuid.uuid4())
                row["path"] = path
                row["sub_path"] = sub_path
                row["inclusion_count"] = inclusion_count
                row["total_time_s"] = total_time_s

                rows.append(copy.copy(row))

        for chunk_rows in chunks(rows, BULK_UPSERT_CHUNK_SIZE):
            row = chunk_rows[0]
            driver.table_client.bulk_upsert(
                DATABASE_PATH + "/code-agility/compile_breakdown", chunk_rows, generate_column_types(row)
            )


if __name__ == "__main__":
    exit(main())
