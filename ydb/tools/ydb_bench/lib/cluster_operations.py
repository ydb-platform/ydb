"""Bounded, non-retrying administrative requests for a live dedicated cluster."""

from datetime import datetime, timezone
import json
import re
import uuid

import grpc
from google.protobuf.json_format import MessageToDict

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json

LIMIT = 64 * 1024


def validate(value):
    if not isinstance(value, dict) or set(value) != {'id', 'command', 'parameters'}:
        raise BenchmarkError('Expected id, command and parameters')
    try:
        request_id = str(uuid.UUID(value['id']))
    except (ValueError, TypeError, AttributeError):
        raise BenchmarkError('Invalid operation ID')
    command, params = value['command'], value['parameters']
    fields = {
        'create-pool': {'name', 'box', 'groups', 'domains', 'domain_begin', 'domain_end', 'disk_type'},
        'create-partition': {'disk_id', 'pool', 'block_size', 'blocks_count', 'media', 'batch_size'},
        'write': {'disk_id', 'start', 'blocks_count', 'block_size', 'pattern'},
        'read': {'disk_id', 'start', 'blocks_count'},
    }
    if not isinstance(command, str) or command not in fields:
        raise BenchmarkError('Unsupported cluster operation')
    if not isinstance(params, dict) or set(params) != fields[command]:
        raise BenchmarkError('Invalid operation parameters')
    for key, item in params.items():
        if key in ('name', 'pool', 'disk_id'):
            if not isinstance(item, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', item):
                raise BenchmarkError('Invalid ' + key)
        elif key in ('disk_type', 'media'):
            choices = ('ROT', 'SSD', 'NVME') if key == 'disk_type' else ('ssd', 'mem')
            if item not in choices:
                raise BenchmarkError('Invalid ' + key)
        elif key == 'pattern':
            if not isinstance(item, str) or not 1 <= len(item.encode()) <= 4096:
                raise BenchmarkError('Pattern must contain 1–4096 UTF-8 bytes')
        else:
            maximum = {
                'box': 2**64 - 1,
                'start': 2**64 - 1,
                'blocks_count': 2**32 - 1,
                'domain_begin': 255,
                'domain_end': 255,
                'groups': 64,
                'domains': 64,
                'block_size': LIMIT,
                'batch_size': 10000,
            }[key]
            minimum = 0 if key in ('start', 'domain_begin') else 1
            if type(item) is not int or not minimum <= item <= maximum:
                raise BenchmarkError('Invalid ' + key)
    if command == 'create-pool' and params['domain_begin'] >= params['domain_end']:
        raise BenchmarkError('Failure domain begin must be below end')
    if command == 'write' and params['blocks_count'] * params['block_size'] > LIMIT:
        raise BenchmarkError('Write is limited to 64 KiB')
    if command == 'read' and params['blocks_count'] > 16:
        raise BenchmarkError('Read is limited to 16 blocks and a 128 KiB response')
    if command in ('read', 'write') and params['start'] + params['blocks_count'] > 2**64:
        raise BenchmarkError('Block range overflows')
    return {'id': request_id, 'command': command, 'parameters': dict(params)}


def read_history(root):
    path = root / 'cluster-operations.json'
    if not path.exists():
        return []
    if path.is_symlink() or path.stat().st_size > 64 * 1024 * 1024:
        raise BenchmarkError('Invalid operations history')
    rows = json.loads(path.read_text())
    # Never replay an interrupted mutation after process recovery.
    return [dict(row, status='unknown') if row['status'] == 'running' else row for row in rows]


class ClusterOperations:
    def __init__(self, root, cluster):
        self.root, self.cluster = root, cluster
        self.rows = read_history(root)

    def execute(self, value):
        request = validate(value)
        for row in self.rows:
            if row['id'] == request['id']:
                if any(row[key] != request[key] for key in ('command', 'parameters')):
                    raise BenchmarkError('Operation ID already used for a different request')
                return row
        if len(self.rows) >= 200:
            raise BenchmarkError('This run has reached its limit of 200 operations')
        self.cluster._check()
        nodes = [node for host in self.cluster.hosts for node in host['nodes'] if node['role'] == 'static']
        if not nodes:
            raise BenchmarkError('No live static endpoint')
        node = nodes[0]
        host = node['hostname']
        endpoint = '{}:{}'.format('[' + host + ']' if ':' in host else host, node['ports']['grpc_port'])
        row = dict(request, status='running', started_at=datetime.now(timezone.utc).isoformat(), node=node['name'])
        self.rows.append(row)
        self._save()
        try:
            domains = self.cluster.template.get('ydb_config', {}).get('domains_config', {}).get('domain', [])
            domain_id = int(domains[0].get('domain_id', 1)) if domains else 1
            row['response'] = invoke(endpoint, domain_id, request)
            row['status'] = 'succeeded'
        except BenchmarkError as error:
            row.update(status='failed', error=str(error)[:2048])
        except Exception as error:
            # A timeout/disconnect cannot establish whether a mutation was applied.
            row.update(status='unknown', error=str(error)[:2048])
        row['finished_at'] = datetime.now(timezone.utc).isoformat()
        self._save()
        return row

    def _save(self):
        atomic_write_json(self.root / 'cluster-operations.json', self.rows)


def invoke(endpoint, domain, operation):
    from ydb.core.nbs.cloud.blockstore.public.api.protos import io_pb2 as nbs_io
    from ydb.core.protos import blobstorage_config_pb2 as bsc, grpc_pb2_grpc as legacy, msgbus_pb2 as msgbus
    from ydb.public.api.grpc.draft import ydb_nbs_v1_pb2_grpc as nbs_grpc
    from ydb.public.api.protos.draft import ydb_nbs_pb2 as nbs
    from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

    command, p = operation['command'], operation['parameters']
    with grpc.insecure_channel(endpoint, options=[('grpc.max_receive_message_length', 128 * 1024)]) as channel:
        if command == 'create-pool':
            request = bsc.TConfigRequest()
            pool = request.Command.add().DefineDDiskPool
            pool.BoxId, pool.Name, pool.NumDDiskGroups = p['box'], p['name'], p['groups']
            pool.Geometry.RealmLevelBegin, pool.Geometry.RealmLevelEnd = 10, 20
            pool.Geometry.DomainLevelBegin, pool.Geometry.DomainLevelEnd = p['domain_begin'], p['domain_end']
            pool.Geometry.NumFailRealms = pool.Geometry.NumVDisksPerFailDomain = 1
            pool.Geometry.NumFailDomainsPerFailRealm = p['domains']
            prop = pool.PDiskFilter.add().Property.add()
            enum = prop.DESCRIPTOR.fields_by_name['Type'].enum_type
            prop.Type = enum.values_by_name[p['disk_type']].number
            response = legacy.TGRpcServerStub(channel).BlobStorageConfig(
                msgbus.TBlobStorageConfigRequest(Domain=domain, Request=request), timeout=10
            )
            if response.Status != 1 or not response.BlobStorageConfigResponse.Success:
                raise BenchmarkError('DDisk pool rejected: ' + response.BlobStorageConfigResponse.ErrorDescription)
            return {'pool': p['name'], 'status': 'SUCCESS'}
        stub = nbs_grpc.NbsServiceStub(channel)
        actor_id = None
        if command in ('read', 'write'):
            resolved = stub.GetLoadActorAdapterActorId(
                nbs.GetLoadActorAdapterActorIdRequest(DiskId=p['disk_id']), timeout=10
            )
            if not resolved.operation.ready or resolved.operation.status != StatusIds.SUCCESS:
                raise BenchmarkError(
                    'Cannot resolve disk actor; I/O was not sent: ' + str(resolved.operation.issues)[:1024]
                )
            actor = nbs.GetLoadActorAdapterActorIdResult()
            if not resolved.operation.result.Unpack(actor) or not actor.ActorId:
                raise BenchmarkError('Invalid disk actor response; I/O was not sent')
            actor_id = actor.ActorId
        if command == 'create-partition':
            request = nbs.CreatePartitionRequest(
                DiskId=p['disk_id'],
                StoragePoolName=p['pool'],
                BlockSize=p['block_size'],
                BlocksCount=p['blocks_count'],
                SyncRequestsBatchSize=p['batch_size'],
                StorageMedia=nbs.STORAGE_MEDIA_MEMORY if p['media'] == 'mem' else nbs.STORAGE_MEDIA_DEFAULT,
            )
            response, result = stub.CreatePartition(request, timeout=10), nbs.CreatePartitionResult()
        elif command == 'write':
            pattern = p['pattern'].encode()
            size = p['block_size']
            block = (pattern * ((size + len(pattern) - 1) // len(pattern)))[:size]
            request = nbs.WriteBlocksRequest(
                DiskId=actor_id, StartIndex=p['start'], Blocks=nbs.IOVector(Buffers=[block] * p['blocks_count'])
            )
            response, result = stub.WriteBlocks(request, timeout=10), nbs.WriteBlocksResult()
        else:
            request = nbs.ReadBlocksRequest(DiskId=actor_id, StartIndex=p['start'], BlocksCount=p['blocks_count'])
            response, result = stub.ReadBlocks(request, timeout=10), nbs.ReadBlocksResult()
        if not response.operation.ready:
            raise RuntimeError('Operation is not ready; outcome is unknown. No automatic retry.')
        if response.operation.status != StatusIds.SUCCESS:
            error = '{}: {}'.format(
                StatusIds.StatusCode.Name(response.operation.status), str(response.operation.issues)[:1024]
            )
            # CreatePartition includes a describe after creation. A failure there
            # does not prove that the volume was not created.
            if command == 'create-partition' and response.operation.status not in (
                StatusIds.BAD_REQUEST,
                StatusIds.UNAUTHORIZED,
                StatusIds.UNSUPPORTED,
                StatusIds.ALREADY_EXISTS,
            ):
                raise RuntimeError('Creation outcome is unknown; no automatic retry. ' + error)
            raise BenchmarkError(error)
        if command == 'write' and response.operation.result.Is(nbs_io.TWriteBlocksResponse.DESCRIPTOR):
            result = nbs_io.TWriteBlocksResponse()
            response.operation.result.Unpack(result)
            if result.Error.Code & (1 << 31):
                raise BenchmarkError('NBS write failed: ' + str(result.Error)[:1024])
            return MessageToDict(result, preserving_proto_field_name=True)
        if not response.operation.result.Unpack(result):
            raise RuntimeError('Unexpected response type; outcome is unknown')
        return MessageToDict(result, preserving_proto_field_name=True)
