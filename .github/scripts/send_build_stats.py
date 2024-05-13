#!/usr/bin/env python3

import os
import ydb 

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

def main():
    # token = open("/home/maxim-yurchuk/.yc_token").read().strip()
    # os.environ["IAM_TOKEN"] = "/home/maxim-yurchuk/.iam"
    
    # os.environ["YDB_ACCESS_TOKEN_CREDENTIALS"] = "YDB_ACCESS_TOKEN_CREDENTIALS"
    
    # token = open("/home/maxim-yurchuk/.iam").read().strip()
    # os.environ["YDB_ACCESS_TOKEN_CREDENTIALS"] = token


    # os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = "/home/maxim-yurchuk/.sa_token"
    
    with ydb.Driver(
        endpoint="grpcs://ydb.serverless.yandexcloud.net:2135",
        database="/ru-central1/b1ggceeul2pkher8vhb6/etn6d1qbals0c29ho4lf",
        # credentials=ydb.credentials.AccessTokenCredentials(token),
        credentials=ydb.credentials_from_env_variables()
    ) as driver:
        driver.wait(timeout=10, fail_fast=True)
        print("Hi")
        
if __name__ == "__main__":
    main()
