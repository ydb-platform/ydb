# -*- coding: utf-8 -*-
import argparse
import grpc
import time
from enum import Enum

from ydb.public.api.grpc.draft import ydb_dynamic_config_v1_pb2_grpc as grpc_server
from ydb.public.api.grpc import ydb_cms_v1_pb2_grpc as cms_grpc_server
from ydb.public.api.grpc import ydb_scheme_v1_pb2_grpc as scheme_grpc_server

from ydb.public.api.protos.draft import ydb_dynamic_config_pb2 as dynamic_config_api
from ydb.public.api.protos import ydb_cms_pb2 as cms_api
from ydb.public.api.protos import ydb_scheme_pb2 as scheme_api

from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
import ydb.public.api.protos.draft.ydb_dynamic_config_pb2 as dynconfig


class EExpectedResult(Enum):
    Success = 0
    PermissionDenied = 1


class EAction(Enum):
    SchemeLs = 0
    AlterDatabase = 1
    Dynconfig = 2


class ParsedOptions:
    class DynconfigSpecialOptions:
        def __init__(self):
            self.quiet = False

    class SchemeSpecialOptions:
        def __init__(self):
            self.path = None
            self.quiet = False

    def __init__(self):
        self.endpoint = None
        self.port = None
        self.database = None
        self.token = None
        self.action = None
        self.expected_result = None
        self.dynconfig_special_options = ParsedOptions.DynconfigSpecialOptions()
        self.scheme_special_options = ParsedOptions.SchemeSpecialOptions()


# defaults:
# port = 2135
def parse_args() -> ParsedOptions:
    def mask_token(token):
        if token is None:
            return "None"

        if len(token) <= 10:
            return "****"

        return token[:5] + "****" + token[-5:]

    parser = argparse.ArgumentParser()

    action_parser = parser.add_subparsers(
        dest='command',
        help='Available actions',
        required=True
    )

    dynconfig_parser = action_parser.add_parser("dynconfig", help="Performs an operation that fetches the dynconfig file")
    dynconfig_parser.add_argument(
        '-q',
        '--quiet',
        action='store_true',
        help='Suppress output during dynconfig operation'
    )

    scheme_ls_parser = action_parser.add_parser("scheme_ls", help="Performs an operation similar to 'ydb scheme ls'")
    scheme_ls_parser.add_argument(
        'path',
        help="Path for 'scheme ls'"
    )
    scheme_ls_parser.add_argument(
        '-q',
        '--quiet',
        action='store_true',
        help="Suppress output during 'scheme ls' operation"
    )

    action_parser.add_parser("alter_database", help="Execute an 'alter database' that adds and immediately removes attributes for the database specified in the '--database' option")

    parser.add_argument(
        '-r',
        '--expected-result',
        choices=['success', 'permission_denied'],
        required=True,
        help='''
            Expected result of response.\n
            If "success" is specified, it is checked that the action was performed successfully.\n
            If "permission_denied" is specified, it is checked that the request crashes with an error.
        '''
    )
    parser.add_argument('-e', '--endpoint', required=True, help='Endpoint to connect')
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

    # Маппинг команд на enum
    command_to_action = {
        'scheme_ls': EAction.SchemeLs,
        'alter_database': EAction.AlterDatabase,
        'dynconfig': EAction.Dynconfig
    }

    # Маппинг expected_result на enum
    result_to_enum = {
        'success': EExpectedResult.Success,
        'permission_denied': EExpectedResult.PermissionDenied
    }

    result = ParsedOptions()
    result.endpoint = args.endpoint
    result.port = args.port
    result.database = args.database
    result.token = token
    result.action = command_to_action[args.command]
    result.expected_result = result_to_enum[args.expected_result]

    result.dynconfig_special_options.quiet = getattr(args, 'quiet', False)

    result.scheme_special_options.quiet = getattr(args, 'quiet', False)
    result.scheme_special_options.path = getattr(args, 'path', None)

    if result.action == EAction.AlterDatabase:
        assert result.database is not None, "Database is required for dynconfig"

    if result.action == EAction.SchemeLs:
        assert result.database is not None, "Database is required for scheme_ls"

    return result


class CommonClientInvoker(object):
    class EService(Enum):
        Cms = 0
        Dynconfig = 1
        Scheme = 2

    def __init__(self, endpoint, port, database, token, retry_count=1):
        self.endpoint = endpoint
        self.port = port
        self.database = database
        self.token = token

        self.__retry_count = retry_count
        self.__retry_sleep_seconds = 10
        self._options = [
            ('grpc.max_receive_message_length', 64 * 10 ** 6),
            ('grpc.max_send_message_length', 64 * 10 ** 6)
        ]

        is_secure = "ydb.mdb.cloud" in endpoint  # for dynamic nodes

        if is_secure:
            self._channel = grpc.secure_channel("%s:%s" % (self.endpoint, self.port), credentials=grpc.ssl_channel_credentials(), options=self._options)
        else:
            self._channel = grpc.insecure_channel("%s:%s" % (self.endpoint, self.port), options=self._options)

        self._dynconfig_stub = grpc_server.DynamicConfigServiceStub(self._channel)
        self._cms_stub = cms_grpc_server.CmsServiceStub(self._channel)
        self._scheme_stub = scheme_grpc_server.SchemeServiceStub(self._channel)

    def _create_metadata(self):
        metadata = []
        if self.token:
            metadata.append(('x-ydb-auth-ticket', self.token))
        if self.database:
            metadata.append(('x-ydb-database', self.database))
        return metadata

    def _get_invoke_callee(self, method: str, service: EService):
        if service == CommonClientInvoker.EService.Cms:
            return getattr(self._cms_stub, method)
        elif service == CommonClientInvoker.EService.Dynconfig:
            return getattr(self._dynconfig_stub, method)
        elif service == CommonClientInvoker.EService.Scheme:
            return getattr(self._scheme_stub, method)

    def invoke(self, request, method: str, service: EService):
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


def generate_error_message(action, status, issue):
    return f"{action} response. Status: {status}; issue: {issue}"


def fetch_dynconfig(client: CommonClientInvoker, expected_result: EExpectedResult):
    def make_request():
        request = dynamic_config_api.GetConfigRequest()
        return client.invoke(request, 'GetConfig', CommonClientInvoker.EService.Dynconfig)

    fetch_config_response = make_request()
    status = fetch_config_response.operation.status
    issue = fetch_config_response.operation.issues

    if expected_result == EExpectedResult.Success:
        assert status == StatusIds.SUCCESS, generate_error_message('GetConfig', status, issue)
    elif expected_result == EExpectedResult.PermissionDenied:
        assert status == StatusIds.UNAUTHORIZED, generate_error_message('GetConfig', status, issue)
        return None
    else:
        raise ValueError(f"Unknown expected_result: {expected_result}")

    result = dynconfig.GetConfigResult()
    fetch_config_response.operation.result.Unpack(result)
    return result.config[0]


def alter_database(client: CommonClientInvoker, path: str, expected_result: EExpectedResult):
    class EAlterDatabaseAction(Enum):
        CreateAttributes = 0
        DropAttributes = 1

    def make_request(action: EAlterDatabaseAction):
        attrs = {}

        if action == EAlterDatabaseAction.CreateAttributes:
            attrs = {
                'one': '1',
                'two': 'two',
            }
        elif action == EAlterDatabaseAction.DropAttributes:
            attrs = {
                'one': '',
                'two': '',
            }

        request = cms_api.AlterDatabaseRequest()
        request.path = path
        request.alter_attributes.update(attrs)

        return client.invoke(request, 'AlterDatabase', CommonClientInvoker.EService.Cms)

    for action in EAlterDatabaseAction:
        create_database_response = make_request(action)
        status = create_database_response.operation.status
        issue = create_database_response.operation.issues

        if expected_result == EExpectedResult.Success:
            assert status == StatusIds.SUCCESS, generate_error_message("Alter database", status, issue)
        elif expected_result == EExpectedResult.PermissionDenied:
            assert status == StatusIds.UNAUTHORIZED, generate_error_message("Alter database", status, issue)
        else:
            raise ValueError(f"Unknown expected_result: {expected_result}")


def scheme_ls(client: CommonClientInvoker, path: str, expected_result: EExpectedResult):
    def make_request():
        request = scheme_api.ListDirectoryRequest()
        request.path = path
        return client.invoke(request, 'ListDirectory', CommonClientInvoker.EService.Scheme)

    ls_response = make_request()

    status = ls_response.operation.status
    issue = ls_response.operation.issues

    if expected_result == EExpectedResult.Success:
        assert status == StatusIds.SUCCESS, generate_error_message("Scheme ls", status, issue)
    elif expected_result == EExpectedResult.PermissionDenied:
        assert status == StatusIds.UNAUTHORIZED, generate_error_message("Scheme ls", status, issue)
    else:
        raise ValueError(f"Unknown expected_result: {expected_result}")

    result = scheme_api.ListDirectoryResult()
    ls_response.operation.result.Unpack(result)
    return result.children


def main():
    parsed_args = parse_args()
    client = CommonClientInvoker(parsed_args.endpoint, parsed_args.port, parsed_args.database, parsed_args.token)

    try:
        match parsed_args.action:
            case EAction.SchemeLs:
                ls_result = scheme_ls(client, parsed_args.scheme_special_options.path, parsed_args.expected_result)

                if parsed_args.expected_result == EExpectedResult.Success:
                    if parsed_args.scheme_special_options.quiet:
                        print("Scheme ls result fetched successfully")
                    else:
                        print(f"Scheme ls result:\n{ls_result}")

            case EAction.AlterDatabase:
                alter_database(client, parsed_args.database, parsed_args.expected_result)

            case EAction.Dynconfig:
                fetched_dynconfig = fetch_dynconfig(client, parsed_args.expected_result)

                if parsed_args.expected_result == EExpectedResult.Success:
                    if parsed_args.dynconfig_special_options.quiet:
                        print("Config fetched successfully")
                    else:
                        print(f"Fetched dynconfig:\n{fetched_dynconfig}")

            case _:
                raise ValueError(f"Unknown action: {parsed_args.action}")

        print(f"Done successfully ({"permission denied" if parsed_args.expected_result == EExpectedResult.PermissionDenied else "no errors"})")
    except Exception as e:
        print(f"Something wrong. Issue: {e}")


if __name__ == '__main__':
    main()
