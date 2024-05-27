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
import ydb.core.protos.grpc_pb2_grpc as kikimr_grpc
import ydb.core.protos.msgbus_pb2 as kikimr_msgbus
import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.core.protos.blobstorage_base3_pb2 as kikimr_bs3
import ydb.core.protos.cms_pb2 as kikimr_cms
import typing


bad_hosts = set()
cache = {}
name_cache = {}

EPDiskType = kikimr_bs3.EPDiskType
EVirtualGroupState = kikimr_bs3.EVirtualGroupState
TGroupDecommitStatus = kikimr_bs3.TGroupDecommitStatus


class EndpointInfo:
    def __init__(self, protocol: str, host: str, port: int):
        self.protocol = protocol
        self.port = port
        self.host = host


class ConnectionParams:
    ENDPOINT_HELP = 'Default protocol is http, default port is 8765'

    def __init__(self):
        self.hosts = set()
        self.endpoints = dict()
        self.grpc_port = None
        self.mon_port = None
        self.mon_protocol = None
        self.token_type = None
        self.token = None
        self.domain = None
        self.verbose = None
        self.quiet = None
        self.http_timeout = None
        self.cafile = None
        self.cadata = None
        self.insecure = None
        self.http = None

    def make_endpoint_info(self, endpoint: str):
        return EndpointInfo(*self.get_protocol_host_port(endpoint))

    def get_protocol_host_port(self, endpoint):
        protocol, sep, endpoint = endpoint.rpartition('://')
        if sep != '://':
            protocol = self.mon_protocol if self.mon_protocol is not None else 'http'
        endpoint, sep, port = endpoint.partition(':')
        if sep == ':':
            return protocol, endpoint, int(port)
        else:
            return protocol, endpoint, self.mon_port

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

    def make_url(self, host, path, params):
        endpoint_info = self.endpoints[host] if host in self.endpoints else self.make_endpoint_info(host)
        netloc = self.get_netloc(endpoint_info.host, endpoint_info.port)
        return urllib.parse.urlunsplit((endpoint_info.protocol, netloc, path, urllib.parse.urlencode(params), ''))

    def parse_token(self, token_file):
        if token_file:
            self.token = token_file.readline().rstrip('\r\n')
            token_file.close()
        if self.token is None:
            self.token = os.getenv('YDB_TOKEN')
            if self.token is not None:
                self.token = self.token.strip()
        if self.token is None:
            try:
                path = os.path.expanduser(os.path.join('~', '.ydb', 'token'))
                with open(path) as f:
                    self.token = f.readline().strip('\r\n')
            except Exception:
                pass

        if self.token is not None and len(self.token.split(' ')) == 2:
            self.token_type, self.token = self.token.split(' ')
        else:
            self.token_type = 'OAuth'

    def apply_args(self, args, with_localhost=True):
        self.grpc_port = args.grpc_port
        self.mon_port = args.mon_port
        self.mon_protocol = args.mon_protocol

        if args.endpoint:
            for endpoint in args.endpoint:
                endpoint_info = self.make_endpoint_info(endpoint)
                if self.mon_protocol is None:
                    self.mon_protocol = endpoint_info.protocol
                host_with_port = '{0}:{1}'.format(endpoint_info.host, endpoint_info.port)
                self.hosts.add(endpoint_info.host)
                self.endpoints[endpoint_info.host] = endpoint_info
                self.endpoints[host_with_port] = endpoint_info

        if self.mon_protocol is None:
            self.mon_protocol = 'http'

        self.parse_token(args.token_file)
        self.domain = 1
        self.verbose = args.verbose
        self.quiet = args.quiet
        self.http_timeout = args.http_timeout
        self.cafile = args.cafile
        self.insecure = args.insecure
        self.http = args.http

    def add_host_access_options(self, parser, with_endpoint=True):
        parser.add_argument('--verbose', '-v', action='store_true', help='Be verbose during operation')
        parser.add_argument('--quiet', '-q', action='store_true', help="Don't show non-vital messages")
        g = parser.add_argument_group('Server access options')
        if with_endpoint:
            g.add_argument('--endpoint', '-e', metavar='[PROTOCOL://]HOST[:PORT]', type=str, required=True, action='append', help=ConnectionParams.ENDPOINT_HELP)
        g.add_argument('--grpc-port', type=int, default=2135, metavar='PORT', help='GRPC port to use for procedure invocation')
        g.add_argument('--mon-port', type=int, default=8765, metavar='PORT', help='HTTP monitoring port for viewer JSON access')
        g.add_argument('--mon-protocol', type=str, metavar='PROTOCOL', choices=('http', 'https'), help='HTTP monitoring protocol for viewer JSON access')
        g.add_argument('--token-file', type=FileType(encoding='ascii'), metavar='PATH', help='Path to token file')
        g.add_argument('--ca-file', metavar='PATH', dest='cafile', type=str, help='Path to a file containing the PEM encoding of the server root certificates for tls connections.')
        g.add_argument('--http', action='store_true', help='Use HTTP to connect to blob storage controller instead of GRPC')
        g.add_argument('--http-timeout', type=int, default=5, help='Timeout for blocking socket I/O operations during HTTP(s) queries')
        g.add_argument('--insecure', action='store_true', help='Allow insecure HTTPS fetching')


connection_params = ConnectionParams()


def set_connection_params_type(connection_params_type: type):
    global connection_params
    connection_params = connection_params_type()


def make_url(host, path, params):
    return connection_params.make_url(host, path, params)


get_pdisk_id = attrgetter('NodeId', 'PDiskId')
get_vslot_id = attrgetter('NodeId', 'PDiskId', 'VSlotId')
get_vslot_id_json = itemgetter('NodeId', 'PDiskId', 'VDiskSlotId')
get_vdisk_id = attrgetter('GroupId', 'GroupGeneration', 'FailRealmIdx', 'FailDomainIdx', 'VDiskIdx')
get_vdisk_id_json = itemgetter('GroupID', 'GroupGeneration', 'Ring', 'Domain', 'VDisk')
get_vdisk_id_short = attrgetter('FailRealmIdx', 'FailDomainIdx', 'VDiskIdx')


def get_vslot_extended_id(vslot):
    return *get_vslot_id(vslot.VSlotId), *get_vdisk_id(vslot)


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


def query_random_host_with_retry(retries=5, explicit_host_param=None, http=False):
    def wrapper(func):
        sig = signature(func)

        @wraps(func)
        def wrapped(*args, **kwargs):
            explicit_host = None
            if explicit_host_param is not None:
                explicit_host = sig.bind(*args, **kwargs).arguments.get(explicit_host_param)

            allowed_hosts = {explicit_host} if explicit_host is not None else connection_params.hosts
            hosts_to_query = []

            try_index = 0
            while True:
                # regenerate host list if it got empty
                if not hosts_to_query:
                    hosts_to_query = list(allowed_hosts - bad_hosts) or list(allowed_hosts)
                    random.shuffle(hosts_to_query)

                host = hosts_to_query.pop()
                try:
                    return func(*args, **kwargs, host=host)
                except Exception as e:
                    try_index += 1
                    if isinstance(e, urllib.error.URLError):
                        bad_hosts.add(host)
                    if not connection_params.quiet:
                        print(f'WARNING: failed to fetch data from host {host} in {func.__name__}: {e}', file=sys.stderr)
                        if http and try_index == retries:
                            print('HINT: consider trying different protocol for endpoints when experiencing massive fetch failures from different hosts', file=sys.stderr)
                    if try_index == retries:
                        raise ConnectionError("Can't connect to specified addresses")

        return wrapped
    return wrapper


@inmemcache('fetch', ['path', 'params', 'explicit_host', 'fmt'], 'cache')
@query_random_host_with_retry(explicit_host_param='explicit_host', http=True)
def fetch(path, params={}, explicit_host=None, fmt='json', host=None, cache=True, method=None, data=None, content_type=None, accept=None):
    url = connection_params.make_url(host, path, params)
    if connection_params.verbose:
        print('INFO: fetching %s' % url, file=sys.stderr)
    request = urllib.request.Request(url, data=data, method=method)
    if connection_params.token and url.startswith('http'):
        request.add_header('Authorization', '%s %s' % (connection_params.token_type, connection_params.token))
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


@query_random_host_with_retry(explicit_host_param='explicit_host')
def invoke_grpc(func, *params, explicit_host=None, host=None):
    options = [
        ('grpc.max_receive_message_length', 256 << 20),  # 256 MiB
    ]
    if connection_params.verbose:
        p = ', '.join('<<< %s >>>' % text_format.MessageToString(param, as_one_line=True) for param in params)
        print('INFO: issuing %s(%s) @%s:%d protocol %s' % (func, p, host, connection_params.grpc_port,
              connection_params.mon_protocol), file=sys.stderr)

    def work(channel):
        try:
            stub = kikimr_grpc.TGRpcServerStub(channel)
            res = getattr(stub, func)(*params)
            if connection_params.verbose:
                print('INFO: result <<< %s >>>' % text_format.MessageToString(res, as_one_line=True), file=sys.stderr)
            return res
        except Exception as e:
            if connection_params.verbose:
                print('ERROR: exception %s' % e, file=sys.stderr)
            raise ConnectionError("Can't connect to specified addresses by gRPC protocol")

    hostport = '%s:%d' % (host, connection_params.grpc_port)
    retval = None
    if connection_params.mon_protocol == 'grpcs':
        creds = grpc.ssl_channel_credentials(connection_params.get_cafile_data())
        with grpc.secure_channel(hostport, creds, options) as channel:
            retval = work(channel)
    else:
        with grpc.insecure_channel(hostport, options) as channel:
            retval = work(channel)
    return retval


def invoke_bsc_request(request):
    if connection_params.http:
        tablet_id = 72057594037932033
        data = request.SerializeToString()
        res = fetch('tablets/app', params=dict(TabletID=tablet_id, exec=1), fmt='raw', cache=False, method='POST',
                    data=data, content_type='application/x-protobuf', accept='application/x-protobuf')
        m = kikimr_bsconfig.TConfigResponse()
        m.MergeFromString(res)
        return m

    bs_request = kikimr_msgbus.TBlobStorageConfigRequest(Domain=connection_params.domain, Request=request)
    if connection_params.token is not None:
        bs_request.SecurityToken = connection_params.token
    bs_response = invoke_grpc('BlobStorageConfig', bs_request)
    if bs_response.Status != 1:
        # remove security token from error message
        bs_request.SecurityToken = ''
        request_s = text_format.MessageToString(bs_request, as_one_line=True)
        response_s = text_format.MessageToString(bs_response, as_one_line=True)
        raise QueryError('Failed to gRPC-query blob storage controller; request: %s; response: %s' % (request_s, response_s))
    return bs_response.BlobStorageConfigResponse


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


def build_pdisk_map(base_config):
    pdisk_map = {
        get_pdisk_id(pdisk): pdisk
        for pdisk in base_config.PDisk
    }
    return pdisk_map


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

        def merge(x, y):
            return max([x, y], key=lambda x: x.get('GroupGeneration', 0))
    elif entity == 'tabletinfo':
        section, keycols = 'TabletStateInfo', ['TabletId']

        def merge(x, y):
            return max([x, y], key=lambda x: x.get('Generation', 0))
    elif entity == 'bsgroupinfo':
        section, keycols = 'BSGroupStateInfo', ['GroupID']

        def merge(x, y):
            return x if x.get('GroupGeneration', 0) > y.get('GroupGeneration', 0) else \
                y if y.get('GroupGeneration', 0) > x.get('GroupGeneration', 0) else \
                x if x.get('VDiskIds', []) else y
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


def get_vslots_by_vdisk_ids(base_config, vdisk_ids):
    vdisk_vslot_map = {}
    for v in base_config.VSlot:
        vdisk_vslot_map['[%08x:_:%u:%u:%u]' % (v.GroupId, v.FailRealmIdx, v.FailDomainIdx, v.VDiskIdx)] = v
        vdisk_vslot_map['[%08x:%u:%u:%u:%u]' % (v.GroupId, v.GroupGeneration, v.FailRealmIdx, v.FailDomainIdx, v.VDiskIdx)] = v

    res = []
    for string in vdisk_ids:
        for vdisk_id in string.split():
            if vdisk_id not in vdisk_vslot_map:
                raise Exception('VDisk with id %s not found' % vdisk_id)
            vslot = vdisk_vslot_map[vdisk_id]
            res.append(vslot)
    return res


def fetch_vdisk_status(hostname):
    res = []
    try:
        j = fetch('viewer/json/vdiskinfo', dict(enums=1, node_id=0), hostname, cache=False)
    except Exception:
        return []
    for v in j.get('VDiskStateInfo', []):
        try:
            res.append((hostname, *get_vslot_id_json(v), *get_vdisk_id_json(v['VDiskId']), v['VDiskState'], v['Replicated']))
        except KeyError:
            pass
    return res


def filter_healthy_groups(groups, node_mon_map, base_config, vslot_map):
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
    g.add_argument('--vdisk-ids', type=str, nargs='+', required=required, help='Space separated list of vdisk ids in format [GroupId:_:FailRealm:FailDomain:VDiskIdx]')


def add_pdisk_ids_option(p, required=False):
    p.add_argument('--pdisk-ids', type=str, nargs='+', required=required, help='Space separated list of pdisk ids in format [NodeId:PDiskId]')


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


def print_json_result(status: str, description: str = None, file=sys.stdout):
    d = {'status': status}
    if description is not None:
        d['description'] = description
    print(json.dumps(d), file=file)


def print_result(format: str, status: str, description: str = None, file=sys.stderr):
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
