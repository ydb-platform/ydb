# -*- coding: utf-8 -*-
import argparse
import grpc
import ydb
import os
import logging
import yaml
import time
from hamcrest import assert_that
from enum import Enum

from ydb.public.api.grpc.draft import ydb_dynamic_config_v1_pb2_grpc as grpc_server
from ydb.public.api.grpc import ydb_cms_v1_pb2_grpc as cms_grpc_server
from ydb.public.api.protos.draft import ydb_dynamic_config_pb2 as dynamic_config_api
from ydb.public.api.protos import ydb_cms_pb2 as cms_api

from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
import ydb.public.api.protos.draft.ydb_dynamic_config_pb2 as dynconfig

class ClientMetaOptions:
    def __init__(self):
        self.endpoint = None
        self.port = None
        self.database = None
        self.token = None


# defaults:
# port = 2135
def parse_args() -> ClientMetaOptions:
    def mask_token(token):
        if token is None:
            return "None"

        if len(token) <= 10:
            return "****"

        return token[:5] + "****" + token[-5:]

    parser = argparse.ArgumentParser()

    parser.add_argument('-e', '--endpoint', required=True, help='Endpoint to connect. Protocols: grpc, grpcs (default: grpcs)')
    parser.add_argument('-d', '--database', help='Database to work with')

    token_group = parser.add_mutually_exclusive_group()
    token_group.add_argument('-t', '--token', help='Token')
    token_group.add_argument('-f', '--token-file', help='Path to file with token')

    parser.add_argument('-p', '--port', default=2135, help='Port to connect')

    args = parser.parse_args()

    token = None

    if args.token:
        token = args.token
    elif args.token_file:
        try:
            with open(args.token_file, 'r') as f:
                token = f.read().strip()
        except FileNotFoundError:
            assert False, f"File {args.token_file} does not exist"

    if "://" in args.endpoint:
        s = args.endpoint.find("://")
        args.endpoint = args.endpoint[s+3:]

    print(f"Endpoint: {args.endpoint}")
    print(f"Database: {args.database}")
    print(f"Token: {mask_token(token)}")
    print(f"Port: {args.port}", flush=True)

    result = ClientMetaOptions()
    result.endpoint = args.endpoint
    result.port = args.port
    result.database = args.database
    result.token = token

    return result

class CommonClientInvoker(object):
    def __init__(self, meta : ClientMetaOptions, retry_count=1):
        self.endpoint = meta.endpoint
        self.port = meta.port
        self.database = meta.database
        self.token = meta.token

        self.__retry_count = retry_count
        self.__retry_sleep_seconds = 10
        self._options = [
            ('grpc.max_receive_message_length', 64 * 10 ** 6),
            ('grpc.max_send_message_length', 64 * 10 ** 6)
        ]
        self._channel = grpc.insecure_channel("%s:%s" % (self.endpoint, self.port), options=self._options)
        self._dynconfig_stub = grpc_server.DynamicConfigServiceStub(self._channel)
        self._cms_stub = cms_grpc_server.CmsServiceStub(self._channel)

    def _create_metadata(self):
        metadata = []
        if self.token:
            metadata.append(('x-ydb-auth-ticket', self.token))
        return metadata

    def _get_invoke_callee(self, method, service):
        if service == 'cms':
            return getattr(self._cms_stub, method)
        elif service == 'dynconfig':
            return getattr(self._dynconfig_stub, method)

    def invoke(self, request, method, service):
        retry = self.__retry_count
        while True:
            try:
                callee = self._get_invoke_callee(method, service)
                return callee(request, metadata=self._create_metadata())
            except (RuntimeError, grpc.RpcError) as e:
                print(f"Error invoking {method}: {e}")
                retry -= 1

                if not retry:
                    raise

                time.sleep(self.__retry_sleep_seconds)

    def close(self):
        self._channel.close()

    def __del__(self):
        self.close()



def fetch_dynconfig(client: CommonClientInvoker):
    def fetch():
        request = dynamic_config_api.GetConfigRequest()
        return client.invoke(request, 'GetConfig', 'dynconfig')

    fetch_config_response = fetch()
    assert_that(fetch_config_response.operation.status == StatusIds.SUCCESS)

    result = dynconfig.GetConfigResult()
    fetch_config_response.operation.result.Unpack(result)
    if result.config[0] == "":
        return None
    else:
        return result.config[0]

def alter_database(client: CommonClientInvoker, path):
    class EAction(Enum):
        CreateAttributes = 0
        DropAttributes = 1

    def alter(action: EAction):
        attrs = {}

        if action == EAction.CreateAttributes:
            attrs = {
                'one': '1',
                'two': 'two',
            }
        elif action == EAction.DropAttributes:
            attrs = {
                'one': '',
                'two': '',
            }

        request = cms_api.AlterDatabaseRequest()
        request.path = path
        request.alter_attributes.update(attrs)

        print(f'request.path = {request.path}')
        return client.invoke(request, 'AlterDatabase', 'cms')

    for action in EAction:
        print(f"Alter database action: {action}")
        create_database_response = alter(action)
        print(f"Alter database response status: {create_database_response.operation.status}")
        print(f"Alter database response issue: {create_database_response.operation.issues}")
        assert_that(create_database_response.operation.status == StatusIds.SUCCESS)
        print('Database altered successfully')

def main():
    client_options = parse_args()
    client = CommonClientInvoker(client_options)

    # fetched_dynconfig = fetch_dynconfig(client)
    # print(f"Fetched dynconfig:\n{fetched_dynconfig}")

    alter_database(client, '/Root/db2')

if __name__ == '__main__':
    main()
