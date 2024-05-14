#!/usr/bin/env python3

import os
import ydb 
import uuid

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

string_types = from_env_columns + [
    "id",


    "binary_path",
    "build_type",
]

uint64_types = [
    "size_bytes",
    "size_stripped_bytes",
]


def main():
    # token = open("/home/maxim-yurchuk/.yc_token").read().strip()
    # os.environ["IAM_TOKEN"] = "/home/maxim-yurchuk/.iam"
    
    # os.environ["YDB_ACCESS_TOKEN_CREDENTIALS"] = "YDB_ACCESS_TOKEN_CREDENTIALS"
    
    # token = open("/home/maxim-yurchuk/.iam").read().strip()
    # os.environ["YDB_ACCESS_TOKEN_CREDENTIALS"] = token


    os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = "/tmp/ydb_service_account.json"
    
    with ydb.Driver(
        endpoint="grpcs://ydb.serverless.yandexcloud.net:2135",
        database="/ru-central1/b1ggceeul2pkher8vhb6/etn6d1qbals0c29ho4lf",
        # credentials=ydb.credentials.AccessTokenCredentials(token),
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        session = ydb.retry_operation_sync(lambda: driver.table_client.session().create())
        session = driver.table_client.session().create()  # TODO REMOVE
        with session.transaction() as tx:
            text_query_builder = []
            for type_ in string_types:
                text_query_builder.append("DECLARE ${} as String;".format(type_))
            for type_ in uint64_types:
                text_query_builder.append("DECLARE ${} as Uint64;".format(type_))
            
            # text_query_builder.append("DECLARE $v as Uint64;")
            # text_query_builder.append("INSERT $v;")
            

            text_query_builder.append("""INSERT INTO binary_size
            (
                {}
            )
            VALUES
            (
                {}                   
            );                         
            """.format(", ".join(string_types + uint64_types), ", ".join(["$" + column for column in string_types + uint64_types])))

            text_query = "".join(text_query_builder)

            prepared_query = session.prepare(text_query)

            parameters = {
                "$id": uuid.uuid4().bytes,
                "$build_type": b"sample",
                "$binary_path": b"sample",
                "$size_stripped_bytes": 123,
                "$size_bytes": 123,
            }

            for column in from_env_columns:
                value = os.environ.get(column.upper(), "N/A")
                value_bytes = value.encode("utf-8")
                parameters["$" + column] = value_bytes
            
            result_sets = tx.execute(prepared_query, parameters, commit_tx=True)
            

            for result_set in result_sets:
                print(result_set.rows)
                # assert len(result_set.rows) == 1
                # assert result_set.rows[0]["column0"] == result
            
            
        """
        with ydb.SessionPool(driver) as pool:
            def callee(session):
                res = session.transaction().execute(
                    "SELECT $seasonsData",
                    {
                        "$seriesData": 1,
                        "$seasonsData": "2",
                        "$episodesData": 3,
                    },
                    commit_tx=True,
                )
                print(res)

            return pool.retry_operation_sync(callee)
            """
        print("Hi")
        
if __name__ == "__main__":
    main()
