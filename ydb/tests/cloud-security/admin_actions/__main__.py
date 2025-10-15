# -*- coding: utf-8 -*-
import argparse
import ydb
import logging
import yaml
import time
from hamcrest import assert_that

from ydb.tests.library.clients.kikimr_dynconfig_client import DynConfigClient
from ydb.tests.library.common.types import Erasure
import ydb.tests.library.common.cms as cms
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.clients.kikimr_dynconfig_client import DynConfigClient
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
import ydb.public.api.protos.draft.ydb_dynamic_config_pb2 as dynconfig

def fetch_config_dynconfig(dynconfig_client):
    fetch_config_response = dynconfig_client.fetch_config()
    assert_that(fetch_config_response.operation.status == StatusIds.SUCCESS)

    result = dynconfig.GetConfigResult()
    fetch_config_response.operation.result.Unpack(result)
    if result.config[0] == "":
        return None
    else:
        return result.config[0]

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--endpoint', required=True, help='Endpoint to connect. Protocols: grpc, grpcs (default: grpcs)')
    parser.add_argument('--database', help='Database to work with')

    token_group = parser.add_mutually_exclusive_group(required=True)
    token_group.add_argument('--token', help='Token')
    token_group.add_argument('--token-file', help='Path to file with token')

    parser.add_argument('--port', default=2136, help='Port to connect')

    args = parser.parse_args()

    token = None

    if args.token:
        token = args.token
    else:
        try:
            with open(args.token_file, 'r') as f:
                token = f.read().strip()
        except FileNotFoundError:
            assert False, f"File {args.token_file} does not exist"

    print(f"Endpoint: {args.endpoint}")
    print(f"Database: {args.database}")
    print(f"Token: {token}")

    client = DynConfigClient(server=args.endpoint, port=args.port, token=token)
    fetched_config = fetch_config_dynconfig(client)
    parsed_fetched_config = yaml.safe_load(fetched_config)
    yaml.dump(parsed_fetched_config)

if __name__ == '__main__':
    main()
