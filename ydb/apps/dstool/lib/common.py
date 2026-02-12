import urllib.parse
import urllib.request
import urllib.error
import random
import json
import sys
import grpc
import struct
import fnmatch
import os
import os.path
import ssl
import socket
from google.protobuf import text_format
from argparse import FileType
from functools import wraps
from inspect import signature
from operator import attrgetter, itemgetter
from collections import defaultdict
from itertools import cycle, islice
from types import SimpleNamespace
import ydb.core.protos.grpc_pb2_grpc as kikimr_grpc
import ydb.core.protos.msgbus_pb2 as kikimr_msgbus
import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.core.protos.blobstorage_base3_pb2 as kikimr_bs3
import ydb.core.protos.cms_pb2 as kikimr_cms
import ydb.public.api.protos.draft.ydb_bridge_pb2 as ydb_bridge
from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc as bridge_grpc_server
from ydb.public.api.grpc.draft import ydb_nbs_v1_pb2_grpc as nbs_grpc_server
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.apps.dstool.lib.arg_parser import print_error_with_usage
import ydb.apps.dstool.lib.table as table
import typing


bad_hosts = set()
cache = {}
name_cache = {}

EPDiskType = kikimr_bs3.EPDiskType
EVirtualGroupState = kikimr_bs3.EVirtualGroupState
TGroupDecommitStatus = kikimr_bs3.TGroupDecommitStatus


class InvalidParameterError(Exception):
    """Exception raised for invalid command line parameter."""
    def __init__(self, parser, parameter_name, parameter, message=""):
        self.parser = parser
        self.parameter = parameter
        self.parameter_name = parameter_name
        self.message = message
        super().__init__(self.message)

    def print(self):
        print_error_with_usage(self.parser, f"ERROR: invalid parameter '{self.parameter_name}' with value '{self.parameter}'. {self.message}")


class EndpointInfo:
    def __init__(self, protocol: str, host: str, grpc_port: int, mon_port: int):
        self.protocol = protocol
        self.grpc_port = grpc_port
        self.mon_port = mon_port
        self.host = host

    @property
    def full(self):
        if self.protocol in ('http', 'https'):
            return f'{self.protocol}://{self.host_with_mon_port}'
        else:
            return f'{self.protocol}://{self.host_with_grpc_port}'

    @property
    def host_with_port(self):
        if self.protocol in ('http', 'https'):
            return self.host_with_mon_port
        else:
            return self.host_with_grpc_port

    @property
    def host_with_grpc_port(self):
        return f'{self.host}:{self.grpc_port}'

    @property
    def host_with_mon_port(self):
        return f'{self.host}:{self.mon_port}'


class ConnectionParams:
    ENDPOINT_HELP = 'Endpoint is specified as PROTOCOL://HOST[:PORT]. Can be specified multiple times with different protocols.'

    def __init__(self):
        self.hosts = set()
        self.endpoints = dict()
        self.grpc_port = 2135
        self.mon_port = 8765
        self.grpc_protocol = 'grpc'
        self.mon_protocol = 'http'
        self.token_type = None
        self.token = None
        self.domain = None
        self.verbose = None
        self.quiet = None
        self.http_timeout = None
        self.cafile = None
        self.cadata = None
        self.insecure = None
        self.parser = None
        self.use_ip = None
        self.http_endpoints = dict()
        self.grpc_endpoints = dict()
        self.args = None
        self.printed_warning_about_not_assigned_http_protocol = False
        self.printed_warning_about_not_assigned_grpc_protocol = False

    def make_endpoint_info(self, endpoint: str):
        protocol, host, port = self.get_protocol_host_port(endpoint)
        grpc_port = self.grpc_port if protocol not in ('grpc', 'grpcs') else port
        mon_port = self.mon_port if protocol not in ('http', 'https') else port
        endpoint_info = EndpointInfo(protocol, host, grpc_port, mon_port)
        return endpoint_info

    def get_protocol_host_port(self, endpoint):
        protocol, sep, endpoint = endpoint.rpartition('://')
        if sep != '://':
            protocol = self.mon_protocol if self.mon_protocol is not None else 'http'
        endpoint, sep, port = endpoint.partition(':')
        if sep == ':':
            return protocol, endpoint, int(port)
        else:
            return protocol, endpoint, self.grpc_port if protocol in ('grpc', 'grpcs') else self.mon_port

    def get_cafile_data(self):
        if self.cafile is None:
            return None
        if self.cadata is None:
            with open(self.cafile, 'rb') as f:
                self.cadata = f.read()
        return self.cadata

    def get_netloc(self, host, port):
        netloc = '%s:%d' % (host, port)
        if netloc in name_cache:
            netloc = name_cache[netloc]
        else:
            for af, socktype, proto, canonname, sa in socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, socket.AI_PASSIVE):
                host, port = socket.getnameinfo(sa, socket.NI_NUMERICHOST | socket.NI_NUMERICSERV)
                if af == socket.AF_INET6:
                    host = '[%s]' % host
                new_netloc = '%s:%s' % (host, port)
                name_cache[netloc] = new_netloc
                netloc = new_netloc
        return netloc

    def make_url(self, endpoint, path, params):
        if self.use_ip:
            location = self.get_netloc(endpoint.host, endpoint.port)
        else:
            location = endpoint.host_with_port
        return urllib.parse.urlunsplit((endpoint.protocol, location, path, urllib.parse.urlencode(params), ''))

    def assign_token(self, typed_token):
        self.token_type, self.token = typed_token
        if self.token and self.token.endswith('@builtin'):
            self.token_type = None

    def parse_token(self, token_file, iam_token_file=None):
        if token_file:
            self.assign_token(self.read_token_from_file(token_file, 'OAuth'))
            token_file.close()
            return

        if iam_token_file:
            self.assign_token(self.read_token_from_file(iam_token_file, 'Bearer'))
            iam_token_file.close()
            return

        token_value = os.getenv('YDB_TOKEN')
        if token_value is not None:
            self.assign_token(self.parse_token_value(token_value, 'OAuth'))
            return

        token_value = os.getenv('IAM_TOKEN')
        if token_value is not None:
            self.assign_token(self.parse_token_value(token_value, 'Bearer'))
            return

        default_token_paths = [
            ('OAuth', os.path.expanduser(os.path.join('~', '.ydb', 'token'))),
            ('Bearer', os.path.expanduser(os.path.join('~', '.ydb', 'iam_token'))),
        ]
        for token_type, token_file_path in default_token_paths:
            self.assign_token(self.read_token_file(token_file_path, token_type))
            if self.token is not None:
                return

    def read_token_from_file(self, token_file, default_token_type):
        if token_file is None:
            return default_token_type, None
        token_value = token_file.readline().rstrip('\r\n')
        return self.parse_token_value(token_value, default_token_type)

    def read_token_file(self, token_file_path, default_token_type):
        if token_file_path is None:
            return default_token_type, None
        try:
            return self.read_token_from_file_and_close(open(token_file_path, 'r'), default_token_type)
        except Exception:
            return default_token_type, None

    def parse_token_value(self, token_value, default_token_type):
        if token_value is None:
            return default_token_type, None
        splitted = token_value.strip().split(' ')
        if len(splitted) == 2:
            return splitted
        else:
            return default_token_type, token_value

    def apply_args(self, args, with_localhost=True):
        self.args = args
        self.grpc_port = args.grpc_port
        self.mon_port = args.mon_port

        protocols = defaultdict(int)
        if args.endpoint:
            for endpoint in args.endpoint:
                endpoint_info = self.make_endpoint_info(endpoint)
                if endpoint_info.protocol not in ('http', 'https', 'grpc', 'grpcs'):
                    raise InvalidParameterError(self.parser, '--endpoint', endpoint, 'Invalid protocol specified for endpoint')
                protocols[endpoint_info.protocol] += 1
                host_with_port = endpoint_info.host_with_port
                self.hosts.add(endpoint_info.host)
                self.endpoints[endpoint_info.host] = endpoint_info
                self.endpoints[host_with_port] = endpoint_info
                if endpoint_info.protocol in ('http', 'https'):
                    self.http_endpoints[host_with_port] = endpoint_info
                else:
                    self.grpc_endpoints[host_with_port] = endpoint_info

        if 'grpc' not in protocols and 'grpcs' in protocols:
            self.grpc_protocol = 'grpcs'
        if 'http' not in protocols and 'https' in protocols:
            self.mon_protocol = 'https'

        self.parse_token(args.token_file, args.iam_token_file)
        self.domain = 1
        self.verbose = args.verbose or args.debug
        self.debug = args.debug
        self.quiet = args.quiet
        self.http_timeout = args.http_timeout
        self.cafile = args.cafile
        self.insecure = args.insecure

    def add_host_access_options(self, parser, with_endpoint=True):
        self.parser = parser
        parser.add_argument('--verbose', '-v', action='store_true', help='Be verbose during operation')
        parser.add_argument('--debug', '-d', action='store_true', help='Be very verbose during operation')
        parser.add_argument('--quiet', '-q', action='store_true', help="Don't show non-vital messages")
        g = parser.add_argument_group('Server access options')
        if with_endpoint:
            g.add_argument('--endpoint', '-e', metavar='[PROTOCOL://]HOST[:PORT]', type=str, required=True, action='append', help=ConnectionParams.ENDPOINT_HELP)
        g.add_argument('--grpc-port', type=int, default=2135, metavar='PORT', help='GRPC port to use for procedure invocation')
        g.add_argument('--mon-port', type=int, default=8765, metavar='PORT', help='HTTP monitoring port for viewer JSON access')
        token_group = g.add_mutually_exclusive_group()
        token_group.add_argument('--token-file', type=FileType(encoding='ascii'), metavar='PATH', help='Path to token file')
        token_group.add_argument('--iam-token-file', type=FileType(encoding='ascii'), metavar='PATH', help='Path to IAM token file')
        g.add_argument('--ca-file', metavar='PATH', dest='cafile', type=str, help='File containing PEM encoded root certificates for SSL/TLS connections. '
                                                                                  'If this parameter is empty, the default roots will be used.')
        g.add_argument('--http-timeout', type=int, default=5, help='Timeout for blocking socket I/O operations during HTTP(s) queries')
        g.add_argument('--insecure', action='store_true', help='Allow insecure HTTPS fetching')
        g.add_argument('--use-ip', action='store_true', help='Use IP addresses instead of hostnames when connecting to endpoints')


connection_params = ConnectionParams()


def set_connection_params_type(connection_params_type: type):
    global connection_params
    connection_params = connection_params_type()


get_pdisk_id = attrgetter('NodeId', 'PDiskId')
get_vslot_id = attrgetter('NodeId', 'PDiskId', 'VSlotId')
get_vslot_id_json = itemgetter('NodeId', 'PDiskId', 'VDiskSlotId')
get_vdisk_id = attrgetter('GroupId', 'GroupGeneration', 'FailRealmIdx', 'FailDomainIdx', 'VDiskIdx')
get_vdisk_id_json = itemgetter('GroupID', 'GroupGeneration', 'Ring', 'Domain', 'VDisk')
get_vdisk_id_short = attrgetter('FailRealmIdx', 'FailDomainIdx', 'VDiskIdx')


def get_vslot_extended_id(vslot):
    return *get_vslot_id(vslot.VSlotId), *get_vdisk_id(vslot)


def get_pdisk_inferred_settings(pdisk):
    if (pdisk.PDiskMetrics.HasField('SlotCount')):
        return pdisk.PDiskMetrics.SlotCount, pdisk.PDiskMetrics.SlotSizeInUnits
    else:
        return pdisk.ExpectedSlotCount, pdisk.PDiskConfig.SlotSizeInUnits


class Location(typing.NamedTuple):
    dc: int
    room: int
    rack: int
    body: int
    node: int
    disk: int

    _levels = [10, 20, 30, 40, 254, 255]

    def __str__(self):
        return ','.join(str(x) if x is not None else '' for x in self)

    def __repr__(self):
        return 'Location(%s)' % ', '.join(map(str, self))

    def subs(self, begin, end):
        return Location._make(value if begin <= level < end else None for level, value in zip(Location._levels, self))

    @staticmethod
    def from_fail_domain(fdom):
        return Location(*map(fdom.get, Location._levels))

    @staticmethod
    def from_physical_location(loc):
        return Location.from_fail_domain(deserialize_fail_domain(loc))

    @staticmethod
    def from_location(location, node_id):
        return Location(dc=location.DataCenter, room=location.Module, rack=location.Rack, body=location.Unit, node=node_id, disk=None)


def inmemcache(name, params=[], cache_enable_param=None):
    def flatten_type(value):
        if isinstance(value, dict):
            return tuple(sorted(value.items()))
        elif isinstance(value, set):
            return tuple(sorted(value))
        else:
            return value

    def wrapper(func):
        sig = signature(func)

        @wraps(func)
        def wrapped(*args, **kwargs):
            a = sig.bind(*args, **kwargs)
            key = (name,) + tuple(flatten_type(a.arguments.get(p)) for p in params)
            if not cache_enable_param or a.arguments.get(cache_enable_param):
                return cache[key] if key in cache else cache.setdefault(key, func(*args, **kwargs))
            else:
                return func(*args, **kwargs)
        return wrapped
    return wrapper


class ConnectionError(Exception):
    pass


class QueryError(Exception):
    pass


class GroupSelectionError(Exception):
    pass


def filter_good_endpoints(endpoints):
    return [endpoint for endpoint in endpoints if endpoint.host_with_port not in bad_hosts]


def get_random_endpoints_for_query(request_type=None, items_count=1, filter=None):
    if request_type == 'http':
        endpoints = connection_params.http_endpoints
    elif request_type == 'grpc':
        endpoints = connection_params.grpc_endpoints
    else:
        endpoints = connection_params.endpoints
    endpoints = list(endpoints.values())
    if filter:
        endpoints = filter(endpoints)
    random.shuffle(endpoints)
    return endpoints[:items_count]


def retry_query_with_endpoints(query, endpoints, request_type, query_name, max_retries=5):
    try_index = 0
    result = None
    for endpoint in endpoints:
        try:
            result = query(endpoint)
            break
        except Exception as e:
            try_index += 1
            if isinstance(e, urllib.error.URLError):
                bad_hosts.add(endpoint.host_with_port)
            if not connection_params.quiet:
                print(f'WARNING: failed to fetch data from host {endpoint.host_with_port} in {query_name}: {e} ({type(e).__module__}.{type(e).__name__})', file=sys.stderr)
                if request_type == 'http' and try_index == max_retries:
                    print('HINT: consider trying different protocol for endpoints when experiencing massive fetch failures from different hosts', file=sys.stderr)
            if try_index == max_retries:
                raise ConnectionError("Can't connect to specified addresses")
    return try_index, result


def query_random_host_with_retry(retries=5, request_type=None):
    def wrapper(func):
        sig = signature(func)

        @wraps(func)
        def wrapped(*args, **kwargs):
            binded = sig.bind(*args, **kwargs)
            explicit_host = binded.arguments.pop('explicit_host', None)
            host = binded.arguments.pop('host', None)
            endpoint = binded.arguments.pop('endpoint', None)
            endpoints = binded.arguments.pop('endpoints', None)

            if endpoint is not None or host is not None:
                return func(*args, **kwargs)

            if explicit_host is not None and explicit_host in connection_params.endpoints:
                explicit_endpoint = connection_params.endpoints[explicit_host]
            elif explicit_host is not None:
                explicit_endpoint = connection_params.make_endpoint_info(f'{connection_params.mon_protocol}://{explicit_host}')
            else:
                explicit_endpoint = None

            def send_query(endpoint):
                return func(*args, **kwargs, endpoint=endpoint)
            setattr(send_query, '_name', func.__name__)

            try_index = 0
            result = None
            if explicit_endpoint:
                try_index, result = retry_query_with_endpoints(send_query, [explicit_endpoint] * retries, request_type, func.__name__, retries)
                return result

            if endpoints:
                try_index, result = retry_query_with_endpoints(send_query, endpoints, request_type, func.__name__, retries)
                return result

            if result is not None:
                return result

            print_if_verbose(connection_params.args, 'INFO: using random hosts', file=sys.stderr)

            endpoints = get_random_endpoints_for_query(request_type=request_type, items_count=retries - try_index, filter=filter_good_endpoints)
            sub_try_index, result = retry_query_with_endpoints(send_query, endpoints, request_type, func.__name__, retries - try_index)
            try_index += sub_try_index

            if result is not None:
                return result

            if request_type == 'http' and connection_params.grpc_endpoints:
                if not connection_params.http_endpoints and not connection_params.printed_warning_about_not_assigned_http_protocol:
                    print_if_not_quiet(
                        connection_params.args,
                        'WARNING: endpoint for http requests is not specified, grpc endpoints will be used instead with conversion to http. '
                        'You can specify additional endpoints with "http" or "https" protocol.',
                        file=sys.stderr)
                    connection_params.printed_warning_about_not_assigned_http_protocol = True
                print_if_verbose(connection_params.args, 'INFO: failed with http endpoints, try to use grpc endpoints', file=sys.stderr)
                endpoints = get_random_endpoints_for_query(request_type='grpc', items_count=retries - try_index, filter=filter_good_endpoints)
                sub_try_index, result = retry_query_with_endpoints(send_query, endpoints, request_type, func.__name__, retries - try_index)
                try_index += sub_try_index

            if request_type == 'grpc' and connection_params.http_endpoints:
                if not connection_params.grpc_endpoints and not connection_params.printed_warning_about_not_assigned_grpc_protocol:
                    print_if_not_quiet(
                        connection_params.args,
                        'WARNING: endpoint for grpc requests is not specified, http endpoints will be used instead with conversion to grpc. '
                        'You can specify additional endpoints with "grpc" or "grpcs" protocol.',
                        file=sys.stderr)
                    connection_params.printed_warning_about_not_assigned_grpc_protocol = True
                print_if_verbose(connection_params.args, 'INFO: failed with grpc endpoints, try to use http endpoints', file=sys.stderr)
                endpoints = get_random_endpoints_for_query(request_type='http', items_count=retries - try_index, filter=filter_good_endpoints)
                sub_try_index, result = retry_query_with_endpoints(send_query, endpoints, request_type, func.__name__, retries - try_index)
                try_index += sub_try_index

            if result is not None:
                return result

            print_if_verbose(connection_params.args, 'INFO: failed with all endpoints, retry them', file=sys.stderr)

            endpoints = get_random_endpoints_for_query(request_type=None, items_count=retries - try_index, filter=None)
            endpoints = list(islice(cycle(endpoints), retries - try_index))
            sub_try_index, result = retry_query_with_endpoints(lambda endpoint: func(*args, **kwargs, endpoint=endpoint), endpoints, request_type, func.__name__, retries - try_index)
            return result

        return wrapped
    return wrapper


@inmemcache('fetch', ['path', 'params', 'explicit_host', 'fmt'], 'cache')
@query_random_host_with_retry(request_type='http')
def fetch(path, params={}, explicit_host=None, fmt='json', host=None, cache=True, method=None, data=None, content_type=None, accept=None, endpoint=None, endpoints=None):
    if endpoint is None and host is not None:
        endpoint = connection_params.make_endpoint_info(f'{connection_params.mon_protocol}://{host}')
    if endpoint.protocol not in ('http', 'https'):
        endpoint = connection_params.make_endpoint_info(f'{connection_params.mon_protocol}://{endpoint.host_with_mon_port}')
    url = connection_params.make_url(endpoint, path, params)
    if connection_params.debug:
        print('INFO: fetching %s' % url, file=sys.stderr)
    request = urllib.request.Request(url, data=data, method=method)
    if connection_params.token and url.startswith('http'):
        if connection_params.token_type:
            authorization = '%s %s' % (connection_params.token_type, connection_params.token)
        else:
            authorization = connection_params.token
        request.add_header('Authorization', authorization)
    if content_type is not None:
        request.add_header('Content-Type', content_type)
    if accept is not None:
        request.add_header('Accept', accept)
    ctx = ssl.create_default_context(cafile=connection_params.cafile)
    if connection_params.insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(request, timeout=connection_params.http_timeout, context=ctx) as stream:
        if fmt == 'json':
            return json.load(stream)
        elif fmt == 'raw':
            return stream.read()
        else:
            assert False, 'ERROR: invalid stream fmt specified: %s' % fmt


@query_random_host_with_retry(request_type='grpc')
def invoke_grpc(func, *params, explicit_host=None, endpoint=None, stub_factory=kikimr_grpc.TGRpcServerStub, endpoints=None):
    options = [
        ('grpc.max_receive_message_length', 256 << 20),  # 256 MiB
    ]
    if connection_params.debug:
        p = ', '.join('<<< %s >>>' % text_format.MessageToString(param, as_one_line=True) for param in params)
        print('INFO: issuing %s(%s) @%s:%d protocol %s' % (func, p, endpoint.host, endpoint.grpc_port,
              endpoint.protocol), file=sys.stderr)

    def work(channel):
        try:
            stub = stub_factory(channel)
            res = getattr(stub, func)(*params)
            if connection_params.debug:
                print('INFO: result <<< %s >>>' % text_format.MessageToString(res, as_one_line=True), file=sys.stderr)
            return res
        except Exception as e:
            if connection_params.debug:
                print('ERROR: exception %s' % e, file=sys.stderr)
            raise ConnectionError("Can't connect to specified addresses by gRPC protocol")

    hostport = endpoint.host_with_grpc_port
    retval = None
    if endpoint.protocol == 'grpcs':
        creds = grpc.ssl_channel_credentials(connection_params.get_cafile_data())
        with grpc.secure_channel(hostport, creds, options) as channel:
            retval = work(channel)
    else:
        with grpc.insecure_channel(hostport, options) as channel:
            retval = work(channel)
    return retval


def invoke_grpc_bsc_request(request, endpoint=None):
    bs_request = kikimr_msgbus.TBlobStorageConfigRequest(Domain=connection_params.domain, Request=request)
    if connection_params.token is not None:
        bs_request.SecurityToken = connection_params.token
    bs_response = invoke_grpc('BlobStorageConfig', bs_request, endpoint=endpoint)
    if bs_response.Status != 1:
        # remove security token from error message
        bs_request.SecurityToken = ''
        request_s = text_format.MessageToString(bs_request, as_one_line=True)
        response_s = text_format.MessageToString(bs_response, as_one_line=True)
        raise QueryError('Failed to gRPC-query blob storage controller; request: %s; response: %s' % (request_s, response_s))
    return bs_response.BlobStorageConfigResponse


def invoke_http_bsc_request(request, endpoint=None):
    tablet_id = 72057594037932033
    data = request.SerializeToString()
    res = fetch('tablets/app', params=dict(TabletID=tablet_id, exec=1), fmt='raw', cache=False, method='POST',
                data=data, content_type='application/x-protobuf', accept='application/x-protobuf', endpoint=endpoint)
    m = kikimr_bsconfig.TConfigResponse()
    m.MergeFromString(res)
    return m


@query_random_host_with_retry(request_type=None)
def invoke_bsc_request(request, explicit_host=None, endpoint=None):
    if endpoint.protocol in ('http', 'https'):
        return invoke_http_bsc_request(request, endpoint=endpoint)
    else:
        return invoke_grpc_bsc_request(request, endpoint=endpoint)


def cms_host_restart_request(user, host, reason, duration_usec, max_avail):
    cms_request = kikimr_msgbus.TCmsRequest()
    if connection_params.token is not None:
        cms_request.SecurityToken = connection_params.token
    cms_request.PermissionRequest.User = user
    action = cms_request.PermissionRequest.Actions.add()
    action.Type = kikimr_cms.TAction.EType.RESTART_SERVICES
    action.Host = host
    action.Services.append('storage')
    action.Duration = duration_usec
    cms_request.PermissionRequest.Reason = reason
    cms_request.PermissionRequest.Duration = duration_usec
    cms_request.PermissionRequest.AvailabilityMode = kikimr_cms.EAvailabilityMode.MODE_MAX_AVAILABILITY if max_avail else kikimr_cms.EAvailabilityMode.MODE_KEEP_AVAILABLE
    response = invoke_grpc('CmsRequest', cms_request)
    if response.Status.Code == kikimr_cms.TStatus.ECode.ALLOW:
        return None
    else:
        return '%s: %s' % (kikimr_cms.TStatus.ECode.Name(response.Status.Code), response.Status.Reason)


def get_piles_info():
    request = ydb_bridge.GetClusterStateRequest()
    response = invoke_grpc('GetClusterState', request, stub_factory=bridge_grpc_server.BridgeServiceStub)
    result = ydb_bridge.GetClusterStateResult()
    response.operation.result.Unpack(result)
    return result


def promote_pile(pile_id):
    request = ydb_bridge.UpdateClusterStateRequest()
    request.updates.add().CopyFrom(ydb_bridge.PileStateUpdate(
        pile_id=pile_id,
        state=ydb_bridge.PileState.PROMOTED
    ))
    invoke_grpc('UpdateClusterState', request, stub_factory=bridge_grpc_server.BridgeServiceStub)


def set_primary_pile(primary_pile_id, synchronized_piles):
    request = ydb_bridge.UpdateClusterStateRequest()
    request.updates.add().CopyFrom(ydb_bridge.PileStateUpdate(
        pile_id=primary_pile_id,
        state=ydb_bridge.PileState.PRIMARY
    ))
    for pile_id in synchronized_piles:
        request.updates.add().CopyFrom(ydb_bridge.PileStateUpdate(
            pile_id=pile_id,
            state=ydb_bridge.PileState.SYNCHRONIZED
        ))
    invoke_grpc('UpdateClusterState', request, stub_factory=bridge_grpc_server.BridgeServiceStub)


def disconnect_pile(pile_id, pile_to_endpoints):
    request = ydb_bridge.UpdateClusterStateRequest()
    request.updates.add().CopyFrom(ydb_bridge.PileStateUpdate(
        pile_id=pile_id,
        state=ydb_bridge.PileState.DISCONNECTED
    ))
    request.specific_pile_ids.append(pile_id)
    invoke_grpc('UpdateClusterState', request, stub_factory=bridge_grpc_server.BridgeServiceStub, endpoints=pile_to_endpoints[pile_id])
    other_pile_ids = [x for x in pile_to_endpoints.keys() if x != pile_id]
    request = ydb_bridge.UpdateClusterStateRequest()
    request.updates.add().CopyFrom(ydb_bridge.PileStateUpdate(
        pile_id=pile_id,
        state=ydb_bridge.PileState.DISCONNECTED,
    ))
    request.specific_pile_ids.extend(other_pile_ids)
    invoke_grpc('UpdateClusterState', request, stub_factory=bridge_grpc_server.BridgeServiceStub, endpoints=pile_to_endpoints[other_pile_ids[0]])


def connect_pile(pile_id, pile_to_endpoints):
    request = ydb_bridge.UpdateClusterStateRequest()
    request.updates.add().CopyFrom(ydb_bridge.PileStateUpdate(
        pile_id=pile_id,
        state=ydb_bridge.PileState.NOT_SYNCHRONIZED,
    ))
    request.specific_pile_ids.append(pile_id)
    invoke_grpc('UpdateClusterState', request, stub_factory=bridge_grpc_server.BridgeServiceStub, endpoints=pile_to_endpoints[pile_id])
    other_pile_ids = [x for x in pile_to_endpoints.keys() if x != pile_id]
    request = ydb_bridge.UpdateClusterStateRequest()
    request.updates.add().CopyFrom(ydb_bridge.PileStateUpdate(
        pile_id=pile_id,
        state=ydb_bridge.PileState.NOT_SYNCHRONIZED,
    ))
    request.specific_pile_ids.extend(other_pile_ids)
    invoke_grpc('UpdateClusterState', request, stub_factory=bridge_grpc_server.BridgeServiceStub, endpoints=pile_to_endpoints[other_pile_ids[0]])


def create_bsc_request(args):
    request = kikimr_bsconfig.TConfigRequest(Rollback=args.dry_run)

    if hasattr(args, 'allow_unusable_pdisks') and args.allow_unusable_pdisks:
        request.AllowUnusableDisks = True
    if hasattr(args, 'ignore_degraded_group_check') and args.ignore_degraded_group_check:
        request.IgnoreDegradedGroupsChecks = True
    if hasattr(args, 'ignore_disintegrated_group_check') and args.ignore_disintegrated_group_check:
        request.IgnoreDisintegratedGroupsChecks = args.ignore_disintegrated_group_check
    if hasattr(args, 'ignore_failure_model_group_check') and args.ignore_failure_model_group_check:
        request.IgnoreGroupFailModelChecks = True
    if hasattr(args, 'ignore_vslot_quotas') and args.ignore_vslot_quotas:
        request.IgnoreVSlotQuotaCheck = True
    if hasattr(args, 'move_only_to_operational_pdisks') and args.move_only_to_operational_pdisks:
        request.SettleOnlyOnOperationalDisks = True

    return request


def create_wipe_request(args, vslot):
    request = create_bsc_request(args)
    cmd = request.Command.add().WipeVDisk
    cmd.VSlotId.NodeId = vslot.VSlotId.NodeId
    cmd.VSlotId.PDiskId = vslot.VSlotId.PDiskId
    cmd.VSlotId.VSlotId = vslot.VSlotId.VSlotId
    cmd.VDiskId.GroupID = vslot.GroupId
    cmd.VDiskId.GroupGeneration = vslot.GroupGeneration
    cmd.VDiskId.Ring = vslot.FailRealmIdx
    cmd.VDiskId.Domain = vslot.FailDomainIdx
    cmd.VDiskId.VDisk = vslot.VDiskIdx
    return request


def create_readonly_request(args, vslot, value):
    request = create_bsc_request(args)
    cmd = request.Command.add().SetVDiskReadOnly
    cmd.VSlotId.NodeId = vslot.VSlotId.NodeId
    cmd.VSlotId.PDiskId = vslot.VSlotId.PDiskId
    cmd.VSlotId.VSlotId = vslot.VSlotId.VSlotId
    cmd.VDiskId.GroupID = vslot.GroupId
    cmd.VDiskId.GroupGeneration = vslot.GroupGeneration
    cmd.VDiskId.Ring = vslot.FailRealmIdx
    cmd.VDiskId.Domain = vslot.FailDomainIdx
    cmd.VDiskId.VDisk = vslot.VDiskIdx
    cmd.Value = value
    return request


def invoke_wipe_request(request):
    return invoke_bsc_request(request)


def invoke_nbs_request(request_type, request):
    return invoke_grpc(request_type, request, stub_factory=nbs_grpc_server.NbsServiceStub)


def print_nbs_request_result(args, request, response):
    success = response.operation.ready and response.operation.status == StatusIds.SUCCESS
    error_reason = 'Request has failed: \n{0}\n{1}\n'.format(request, response)
    print_status_if_verbose(args, success, error_reason)


@inmemcache('base_config_and_storage_pools', cache_enable_param='cache')
def fetch_base_config_and_storage_pools(retrieveDevices=False, virtualGroupsOnly=False, cache=True):
    request = kikimr_bsconfig.TConfigRequest(Rollback=True)
    request.Command.add().QueryBaseConfig.CopyFrom(kikimr_bsconfig.TQueryBaseConfig(RetrieveDevices=retrieveDevices, VirtualGroupsOnly=virtualGroupsOnly))
    request.Command.add().ReadStoragePool.BoxId = (1 << 64) - 1
    response = invoke_bsc_request(request)
    assert not response.Success
    assert len(response.Status) == 2
    assert response.Status[0].Success, 'QueryBaseConfig failed with error: %s' % response.Status[0].ErrorDescription
    assert response.Status[1].Success, 'ReadStoragePool failed with error: %s' % response.Status[1].ErrorDescription
    return dict(BaseConfig=response.Status[0].BaseConfig, StoragePools=response.Status[1].StoragePool)


def fetch_base_config(retrieveDevices=False, virtualGroupsOnly=False, cache=True):
    return fetch_base_config_and_storage_pools(retrieveDevices, virtualGroupsOnly, cache)['BaseConfig']


def fetch_storage_pools():
    return fetch_base_config_and_storage_pools()['StoragePools']


def fetch_node_mapping():
    base_config = fetch_base_config()
    return build_node_fqdn_maps(base_config)


def fetch_node_to_fqdn_map():
    return {node.NodeId: node.HostKey.Fqdn for node in fetch_base_config().Node}


def remove_and_pop_if_zero(m, key, value):
    x = m[key]
    x.remove(value)
    if not x:
        del m[key]


def map_fqdns(fqdns, node_ids, allowed_node_ids=None):
    # we have to query nodes and translate them to node ids
    _, fqdn_to_node_ids = fetch_node_mapping()
    while fqdns:
        name = fqdns.pop()
        fqdn, sep, port = name.partition(':')
        matching_ids = fqdn_to_node_ids.get(fqdn)
        if matching_ids and allowed_node_ids is not None:
            matching_ids = {
                node_id: port
                for node_id, port in matching_ids.items()
                if node_id in allowed_node_ids
            }
        if sep:
            matching_ids = {
                node_id: matching_port
                for node_id, matching_port in (matching_ids or dict()).items()
                if matching_port == int(port)
            }
        if not matching_ids:
            print('ERROR: FQDN %s not found' % (fqdn + sep + port), file=sys.stderr)
            sys.exit(1)
        elif len(matching_ids) > 1:
            print('ERROR: ambiguous FQDN %s matches nodes %s' % (name, ', '.join(map(str, sorted(matching_ids)))), file=sys.stderr)
            sys.exit(1)
        else:
            node_ids += matching_ids


def bytes_to_string(num, round, suffix):
    res_num = num / round
    if res_num < 10:
        s = f'{res_num:.2f}'
    elif res_num < 100:
        s = f'{res_num:.1f}'
    else:
        s = f'{res_num:.0f}'
    left, _, right = s.partition('.')
    if right == '00' or not right:
        right = ''
    else:
        right = f'.{right}'
    res = ''
    while left:
        subs, left = left[-3:], left[:-3]
        comma = "\'" if res else ''
        res = subs + comma + res
    return f'{res}{right}{suffix}'


def gib_string(num):
    return bytes_to_string(num, 1024 ** 3, '')


def bytes_string(num):
    if num > 1024 ** 5:
        return bytes_to_string(num, 1024 ** 5, ' PiB')
    if num > 1024 ** 4:
        return bytes_to_string(num, 1024 ** 4, ' TiB')
    if num > 1024 ** 3:
        return bytes_to_string(num, 1024 ** 3, ' GiB')
    if num > 1024 ** 2:
        return bytes_to_string(num, 1024 ** 2, ' MiB')
    if num > 1024:
        return bytes_to_string(num, 1024, ' kiB')
    return bytes_to_string(num, 1, '')


def convert_tristate_bool(tsb):
    if tsb == kikimr_bsconfig.ETriStateBool.kTrue:
        return True
    elif tsb == kikimr_bsconfig.ETriStateBool.kFalse:
        return False
    elif tsb == kikimr_bsconfig.ETriStateBool.kNotSet:
        return None
    print('ERROR: incorrect value for ETriStateBool', file=sys.stderr)
    sys.exit(1)


def deserialize_fail_domain(s):
    fmt = '=BI'
    step = struct.calcsize(fmt)
    res = {}
    for offset in range(0, len(s), step):
        key, value = struct.unpack_from(fmt, s, offset)
        res[key] = value
    return res


def pdisk_matches_storage_pool(pdisk, sp):
    if pdisk.BoxId != sp.BoxId:
        return False

    for pdisk_filter in sp.PDiskFilter:
        for prop in pdisk_filter.Property:
            if prop.HasField('Type'):
                if prop.Type != pdisk.Type:
                    break
            elif prop.HasField('SharedWithOs'):
                if prop.SharedWithOs != convert_tristate_bool(pdisk.SharedWithOs):
                    break
            elif prop.HasField('ReadCentric'):
                if prop.ReadCentric != convert_tristate_bool(pdisk.ReadCentric):
                    break
            elif prop.HasField('Kind'):
                if prop.Kind != pdisk.Kind:
                    break
            else:
                print('ERROR: unknown property in StoragePool filter', file=sys.stderr)
                sys.exit(1)
        else:
            return True

    return False


def select_groups(base_config, group_ids=None):
    if group_ids is not None:
        group_ids = set(group_ids)
    else:
        group_ids = set()

    known_groups = {
        group.GroupId
        for group in base_config.Group
        if is_dynamic_group(group.GroupId)
    }

    for group_id in group_ids:
        if not is_dynamic_group(group_id):
            raise Exception(False, 'Group {group_id} is static')
        if group_id not in known_groups:
            raise Exception('Unknown group with id {group_id}')

    if not group_ids:
        group_ids = known_groups
    return group_ids


def create_pdisk_map():
    base_config = fetch_base_config()
    node_to_location = {
        node.NodeId: Location.from_physical_location(node.PhysicalLocation)
        for node in base_config.Node
    }
    res = {}
    for pdisk in base_config.PDisk:
        location = node_to_location[pdisk.NodeId]._replace(node=pdisk.NodeId, disk=pdisk.PDiskId)
        res[location] = pdisk
    return res


def vslots_of_group(group, vslot_map):
    return map(vslot_map.__getitem__, map(get_vslot_id, group.VSlotId))


def build_group_slot_size_map(base_config, vslot_map):
    return {
        group.GroupId: max(vslot.VDiskMetrics.AllocatedSize for vslot in vslots_of_group(group, vslot_map))
        for group in base_config.Group
    }


def build_group_map(base_config):
    group_map = {
        group.GroupId: group
        for group in base_config.Group
        if is_dynamic_group(group.GroupId)
    }
    return group_map


def build_node_fqdn_map(base_config):
    node_fqdn_map = {
        node.NodeId: node.HostKey.Fqdn
        for node in base_config.Node
    }
    return node_fqdn_map


def build_node_fqdn_maps(base_config):
    node_id_to_host = {}
    host_to_node_id = {}
    for node in base_config.Node:
        node_id_to_host[node.NodeId] = (node.HostKey.Fqdn, node.HostKey.IcPort)
        host_to_node_id.setdefault(node.HostKey.Fqdn, {})[node.NodeId] = node.HostKey.IcPort
    return node_id_to_host, host_to_node_id


def build_pile_to_node_id_map(base_config):
    pile_to_node_id_map = defaultdict(list)
    for node in base_config.Node:
        pile_to_node_id_map[node.Location.BridgePileName].append(node.NodeId)
    return pile_to_node_id_map


def build_pdisk_map(base_config):
    pdisk_map = {
        get_pdisk_id(pdisk): pdisk
        for pdisk in base_config.PDisk
    }
    return pdisk_map


def build_donors_per_pdisk_map(base_config):
    donors_per_vdisk = defaultdict(int)
    for vslot in base_config.VSlot:
        for donor in vslot.Donors:
            pdisk_id = get_pdisk_id(donor.VSlotId)
            donors_per_vdisk[pdisk_id] += 1
    return donors_per_vdisk


def build_pdisk_static_slots_map(base_config):
    pdisk_static_slots_map = {
        get_pdisk_id(pdisk): pdisk.NumStaticSlots
        for pdisk in base_config.PDisk
    }
    return pdisk_static_slots_map


def build_pdisk_usage_map(base_config, count_donors=False, storage_pool=None):
    pdisk_usage_map = {}

    for pdisk in base_config.PDisk:
        if storage_pool is not None and not pdisk_matches_storage_pool(pdisk, storage_pool):
            continue
        pdisk_id = get_pdisk_id(pdisk)
        pdisk_usage_map[pdisk_id] = pdisk.NumStaticSlots

    for vslot in base_config.VSlot:
        if not (vslot.GroupId & 0x80000000):  # don't count vslots from static groups twice
            continue
        pdisk_id = get_pdisk_id(vslot.VSlotId)
        if pdisk_id not in pdisk_usage_map:
            continue
        pdisk_usage_map[pdisk_id] += 1
        for donor in vslot.Donors if count_donors else []:
            donor_pdisk_id = get_pdisk_id(donor.VSlotId)
            pdisk_usage_map[donor_pdisk_id] += 1

    return pdisk_usage_map


def build_storage_pools_map(storage_pools):
    storage_pools_map = {
        (sp.BoxId, sp.StoragePoolId): sp
        for sp in storage_pools
    }
    return storage_pools_map


def build_storage_pool_groups_map(base_config, group_ids):
    storage_pool_groups_map = defaultdict(list)
    for group in base_config.Group:
        if group.GroupId in group_ids:
            storage_pool_groups_map[group.BoxId, group.StoragePoolId].append(group)

    known_groups = {
        group.GroupId
        for group in base_config.Group
    }

    for group_id in group_ids:
        if group_id not in known_groups:
            raise Exception('Unknown group with id %u' % group_id)

    return storage_pool_groups_map


def build_storage_pool_names_map(storage_pools):
    storage_pool_names_map = {
        (sp.BoxId, sp.StoragePoolId): sp.Name
        for sp in storage_pools
    }
    return storage_pool_names_map


def build_vslot_map(base_config):
    vslot_map = {
        get_vslot_id(vslot.VSlotId): vslot
        for vslot in base_config.VSlot
    }
    return vslot_map


def message_to_string(m):
    return text_format.MessageToString(m, as_one_line=True)


def add_pdisk_select_options(parser, specification=None):
    types = EPDiskType.keys()
    name = 'PDisk selection options'
    if specification is not None:
        name += ' for ' + specification
    g = parser.add_argument_group(name)
    g.add_argument('--node-id', type=int, nargs='+', metavar='NODE', help='filter only PDisks on a node(s) with specific number')
    g.add_argument('--fqdn', type=str, nargs='+', metavar='FQDN', help='filter only PDisks on a node with specific FQDN(s)')
    g.add_argument('--pdisk-id', type=int, nargs='+', metavar='PDISK', help='filter only PDisks with specific id')
    g.add_argument('--path', type=str, metavar='PATH', help='filter only PDisks with a specific path on a system')
    g.add_argument('--type', type=str, choices=types, metavar='TYPE', help='filter only PDisks with a specific media type')


def add_format_options(parser, formats: list, default=None):
    help_lines = ['Output format. Available options:']
    for format_type, description_lines in formats:
        help_lines.append('  ' + format_type)
        for line in description_lines:
            help_lines.append('    ' + line)
    help = '\n'.join(help_lines)
    choices = [format_type for format_type, _ in formats]
    parser.add_argument('--format', type=str, choices=choices, default=default, help=help)


def add_basic_format_options(parser):
    basic_formats = [
        ('pretty', ['Human readable output']),
        ('json', ['Output in json format'])
    ]
    add_format_options(parser, basic_formats, default='pretty')


def get_selected_pdisks(args, base_config):
    node_id_to_host = {
        node.NodeId: node.HostKey.Fqdn
        for node in base_config.Node
    }
    return {
        (pdisk.NodeId, pdisk.PDiskId)
        for pdisk in base_config.PDisk
        if args.node_id is None or pdisk.NodeId in args.node_id
        if args.fqdn is None or any(fnmatch.fnmatchcase(node_id_to_host[pdisk.NodeId], fqdn) for fqdn in args.fqdn)
        if args.pdisk_id is None or pdisk.PDiskId in args.pdisk_id
        if args.path in [None, pdisk.Path]
        if args.type in [None, EPDiskType.Name(pdisk.Type)]
    }


def fetch_json_info(entity, nodes=None, enums=1):
    merge = None
    if entity == 'pdiskinfo':
        section, keycols = 'PDiskStateInfo', ['NodeId', 'PDiskId']
    elif entity == 'sysinfo':
        section, keycols = 'SystemStateInfo', ['NodeId']
    elif entity == 'vdiskinfo':
        section, keycols = 'VDiskStateInfo', ['NodeId', 'PDiskId', 'VDiskSlotId']

        def merge_fn(x, y):
            return max([x, y], key=lambda x: x.get('GroupGeneration', 0))
        merge = merge_fn
    elif entity == 'tabletinfo':
        section, keycols = 'TabletStateInfo', ['TabletId']

        def merge_fn(x, y):
            return max([x, y], key=lambda x: x.get('Generation', 0))
        merge = merge_fn
    elif entity == 'bsgroupinfo':
        section, keycols = 'BSGroupStateInfo', ['GroupID']

        def merge_fn(x, y):
            return x if x.get('GroupGeneration', 0) > y.get('GroupGeneration', 0) else \
                y if y.get('GroupGeneration', 0) > x.get('GroupGeneration', 0) else \
                x if x.get('VDiskIds', []) else y
        merge = merge_fn
    else:
        assert False
    res = {}
    key_getter = itemgetter(*keycols)
    max_nodes_at_once = 128
    if nodes is None:
        nodes = map(attrgetter('NodeId'), fetch_base_config().Node)
    remaining_retry_count = {node_id: 5 for node_id in nodes}
    missing_node_ids = set()
    while remaining_retry_count:
        node_ids = sorted(remaining_retry_count, key=lambda x: (-remaining_retry_count[x], x))[:max_nodes_at_once]
        node_id = ','.join(map(str, node_ids))
        for node_id_str, j in fetch('viewer/json/%s' % entity, dict(enums=enums, merge=0, node_id=node_id), cache=False).items():
            if section in j:
                remaining_retry_count.pop(int(node_id_str), None)
                for item in j[section]:
                    item['NodeId'] = int(node_id_str)
                    key = key_getter(item)
                    if not merge and key in res:
                        print('non-callable merge entity=%s key=%s item=%s prev=%s' % (entity, key, item, res[key]), file=sys.stderr)
                    res[key] = merge(res[key], item) if key in res else item
        for node_id in node_ids:
            if node_id in remaining_retry_count:
                remaining_retry_count[node_id] -= 1
                if not remaining_retry_count[node_id]:
                    del remaining_retry_count[node_id]
                    missing_node_ids.add(node_id)
    if missing_node_ids:
        print('WARNING: missing NodeId# %s' % ', '.join(map(str, sorted(missing_node_ids))), file=sys.stderr)
    return res


def fetch_node_mon_map(nodes=None):
    return {
        node_id: sysinfo['Host'] + ep['Address']
        for node_id, sysinfo in fetch_json_info('sysinfo', nodes).items()
        for ep in sysinfo.get('Endpoints', [])
        if ep['Name'] == 'http-mon'
    }


def fetch_node_to_endpoint_map(nodes=None):
    res = {}
    for node_id, sysinfo in fetch_json_info('sysinfo', nodes).items():
        grpc_port = None
        mon_port = None
        for ep in sysinfo.get('Endpoints', []):
            if ep['Name'] == 'grpc':
                grpc_port = int(ep['Address'][1:])
            elif ep['Name'] == 'http-mon':
                mon_port = int(ep['Address'][1:])
        res[node_id] = EndpointInfo('grpc', sysinfo['Host'], grpc_port, mon_port)
    return res


def get_vslots_by_vdisk_ids(base_config, vdisk_ids):
    vdisk_vslot_map = {}
    for v in base_config.VSlot:
        vdisk_vslot_map['[%08x:_:%u:%u:%u]' % (v.GroupId, v.FailRealmIdx, v.FailDomainIdx, v.VDiskIdx)] = v
        vdisk_vslot_map['[%08x:%u:%u:%u:%u]' % (v.GroupId, v.GroupGeneration, v.FailRealmIdx, v.FailDomainIdx, v.VDiskIdx)] = v
        vdisk_vslot_map['(%d-%u-%u-%u-%u)' % (v.GroupId, v.GroupGeneration, v.FailRealmIdx, v.FailDomainIdx, v.VDiskIdx)] = v

    res = []
    for string in vdisk_ids:
        for vdisk_id in string.split():
            if vdisk_id not in vdisk_vslot_map:
                raise Exception('VDisk with id %s not found' % vdisk_id)
            vslot = vdisk_vslot_map[vdisk_id]
            res.append(vslot)
    return res


def filter_healthy_groups(groups, base_config, vslot_map):
    res = {
        group.GroupId: len(group.VSlotId)
        for group in base_config.Group
        if group.GroupId in groups
        if all(vslot.Status == 'READY' for vslot in vslots_of_group(group, vslot_map))
    }
    check_set = {
        (*vslot_id, *attrgetter('GroupId', 'FailRealmIdx', 'FailDomainIdx', 'VDiskIdx')(vslot))
        for vslot_id, vslot in vslot_map.items()
        if vslot.GroupId in res
    }
    for vdisk_id, j in fetch_json_info('vdiskinfo', {node_id for node_id, _, _, _, _, _, _ in check_set}).items():
        if j.get('Replicated') and j.get('VDiskState') == 'OK':
            check_item = *vdisk_id, *itemgetter('GroupID', 'Ring', 'Domain', 'VDisk')(j['VDiskId'])
            if check_item in check_set:
                check_set.remove(check_item)
                res[j['VDiskId']['GroupID']] -= 1
    return {group_id for group_id, count in res.items() if not count}


def add_host_access_options(parser):
    connection_params.add_host_access_options(parser)


def add_vdisk_ids_option(g, required=False):
    help_text = (
        'Space separated list of vdisk ids in formats: '
        '[GroupId(hex):_:FailRealm:FailDomain:VDiskIdx], '
        '[GroupId(hex):GroupGen:FailRealm:FailDomain:VDiskIdx], '
        'or (GroupId(dec)-GroupGen-FailRealm-FailDomain-VDiskIdx)'
    )
    g.add_argument('--vdisk-ids', type=str, nargs='+', required=required, help=help_text)


def add_pdisk_ids_option(p, required=False):
    p.add_argument('--pdisk-ids', type=str, nargs='+', required=required,
                   help='Space separated list of pdisk ids in format [NodeId:PDiskId] (brackets optional)')


def add_group_ids_option(p, required=False):
    p.add_argument('--group-ids', type=int, nargs='+', action='append', required=required, help='Space separated list of group ids')


def add_allow_unusable_pdisks_option(p):
    p.add_argument('--allow-unusable-pdisks', action='store_true', help='Allow unusable PDisks to stay in place while replacing other ones')


def add_ignore_degraded_group_check_option(p):
    p.add_argument('--ignore-degraded-group-check', action='store_true', help='Ignore results of DEGRADED group checks')


def add_ignore_disintegrated_group_check_option(p):
    p.add_argument('--ignore-disintegrated-group-check', action='store_true', help='Ignore results of DISINTEGRATED group checks')


def add_ignore_failure_model_group_check_option(p):
    p.add_argument('--ignore-failure-model-group-check', action='store_true', help='Ignore results of failure model group checks')


def add_ignore_vslot_quotas_option(p):
    p.add_argument('--ignore-vslot-quotas', action='store_true', help='Ignore results of VSlot quota checks')


def apply_args(args):
    connection_params.apply_args(args)


def flush_cache():
    cache.clear()


def print_json_result(status: str, description: str = None, file=None):
    if file is None:
        file = sys.stdout
    d = {'status': status}
    if description is not None:
        d['description'] = description
    print(json.dumps(d), file=file)


def print_result(format: str, status: str, description: str = None, file=None):
    if file is None:
        file = sys.stderr
    if format == 'json':
        print_json_result(status, description)
    else:
        if description is not None:
            print('{0}, {1}'.format(status, description), file=file)
        else:
            print(status, file=file)


def print_request_result(args, request, response):
    success = is_successful_bsc_response(response)
    error_reason = 'Request has failed: \n{0}\n{1}\n'.format(request, response)
    print_status_if_verbose(args, success, error_reason)


def print_status_if_verbose(args, success, error_reason):
    format = getattr(args, 'format', 'pretty')
    verbose = getattr(args, 'verbose', False)
    if success:
        print_result(format, 'success')
    else:
        if verbose:
            print_result(format, 'error', error_reason)
        else:
            print_result(format, 'error', 'add --verbose for more info')


def print_status_if_not_quiet(args, success, error_reason):
    format = getattr(args, 'format', 'pretty')
    quiet = getattr(args, 'quiet', False)
    if success:
        print_result(format, 'success')
    else:
        if not quiet:
            print_result(format, 'error', error_reason)


def print_status(args, success, error_reason):
    print_status_if_not_quiet(args, success, error_reason)


def print_if_verbose(args, message, file=sys.stderr):
    verbose = getattr(args, 'verbose', False)
    format = getattr(args, 'format', 'pretty')
    if verbose:
        print_result(format, status=message, description=None, file=file)


def print_if_not_quiet(args, message, file=sys.stderr):
    quiet = getattr(args, 'quiet', False)
    format = getattr(args, 'format', 'pretty')
    if not quiet:
        print_result(format, status=message, description=None, file=file)


def is_dynamic_group(groupId):
    return groupId >= 0x80000000


def is_successful_bsc_response(response):
    return response.Success or 'transaction rollback' in response.ErrorDescription


def dump_group_mapper_error(response: kikimr_bsconfig.TConfigResponse, args):
    verbose = getattr(args, 'verbose', False)
    err: kikimr_bsconfig.TGroupMapperError | None = None

    if (len(response.Status) == 1) and verbose:
        for fail_param in response.Status[0].FailParam:
            if fail_param.HasField("GroupMapperError"):
                err = fail_param.GroupMapperError

    if err is None:
        return

    table_args = SimpleNamespace(sort_by=None, columns=None, format=args.format, no_header=None)

    def table_generator(data: typing.Iterable[kikimr_bsconfig.TGroupMapperError.TStats], print_domain: bool = True):
        all_columns = []
        if print_domain:
            all_columns += ['Domain']
        all_columns += [
            'All slots are occupied',
            'Not enough space',
            'Not accepting new slots',
            'Not operational',
            'Decommission',
        ]
        table_output = table.TableOutput(all_columns)
        rows = []
        for st in data:
            row = {}
            if print_domain:
                row['Domain'] = f"{st.Domain}"
            row['All slots are occupied'] = str(st.AllSlotsAreOccupied)
            row['Not enough space'] = str(st.NotEnoughSpace)
            row['Not accepting new slots'] = str(st.NotAcceptingNewSlots)
            row['Not operational'] = str(st.NotOperational)
            row['Decommission'] = str(st.Decommission)
            rows.append(row)

        table_output.dump(rows, table_args)

    print("Total stats")
    table_generator([err.TotalStats], print_domain=False)
    if len(err.MatchingDomainsStats) > 0:
        print("Matching domains")
        table_generator(err.MatchingDomainsStats)
    else:
        print("No matching domains")
    print(f"OK Discs Count: {err.OkDisksCount}")
    print(
        f"Missing {err.RealmLocationKey}s Count: {err.MissingFailRealmsCount}\n"
        f"{err.RealmLocationKey}s With Missing {err.DomainLocationKey}s Count: {err.FailRealmsWithMissingDomainsCount}\n"
        f"{err.DomainLocationKey}s With Missing Disks Count: {err.DomainsWithMissingDisksCount}"
    )
