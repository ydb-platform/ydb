import json
import tempfile
import threading
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from ydb.core.nbs.cloud.blockstore.public.api.protos import io_pb2 as nbs_io
from ydb.core.protos import grpc_pb2_grpc as legacy, msgbus_pb2 as msgbus
from ydb.public.api.grpc.draft import ydb_nbs_v1_pb2_grpc as nbs_grpc
from ydb.public.api.protos.draft import ydb_nbs_pb2 as nbs
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tools.ydb_bench.lib import cluster_operations as operations, hosts, web
from ydb.tools.ydb_bench.lib.common import BenchmarkError


class ClusterOperationsTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.cluster = mock.Mock(
            template={},
            hosts=[{'nodes': [{'name': 's1', 'role': 'static', 'hostname': '::1', 'ports': {'grpc_port': 2135}}]}],
        )
        self.manager = operations.ClusterOperations(self.root, self.cluster)

    def request(self, command='read', **parameters):
        return {
            'id': str(uuid.uuid4()),
            'command': command,
            'parameters': parameters or {'disk_id': 'test', 'start': 0, 'blocks_count': 1},
        }

    def test_history_and_retry_are_not_replayed(self):
        request = self.request()
        with mock.patch.object(operations, 'invoke', return_value={'Blocks': {'Buffers': ['dGVzdA==']}}) as invoke:
            first = self.manager.execute(request)
            self.assertEqual(first, self.manager.execute(request))
            invoke.assert_called_once_with('[::1]:2135', 1, request)
            restored = operations.ClusterOperations(self.root, self.cluster)
            self.assertEqual(first, restored.execute(request))
            self.assertEqual(1, invoke.call_count)
            request['parameters']['start'] = 1
            with self.assertRaises(BenchmarkError):
                restored.execute(request)
        self.assertEqual('succeeded', operations.read_history(self.root)[0]['status'])

    def test_transport_failure_is_unknown_and_never_retried(self):
        request = self.request()
        with mock.patch.object(operations, 'invoke', side_effect=TimeoutError('timeout')) as invoke:
            self.assertEqual('unknown', self.manager.execute(request)['status'])
            self.manager.execute(request)
            self.assertEqual(1, invoke.call_count)

    def test_server_rejection_and_interrupted_history(self):
        with mock.patch.object(operations, 'invoke', side_effect=BenchmarkError('unsupported')):
            self.assertEqual('failed', self.manager.execute(self.request())['status'])
        rows = json.loads((self.root / 'cluster-operations.json').read_text())
        rows[0]['status'] = 'running'
        (self.root / 'cluster-operations.json').write_text(json.dumps(rows))
        self.assertEqual('unknown', operations.read_history(self.root)[0]['status'])

    def test_validation_limits_and_endpoint_injection(self):
        bad = [
            self.request(blocks_count=17, disk_id='test', start=0),
            self.request(start=True, disk_id='test', blocks_count=1),
            self.request('write', disk_id='test', start=0, blocks_count=17, block_size=4096, pattern='a'),
            self.request('write', disk_id='test', start=0, blocks_count=1, block_size=4096, pattern=''),
            self.request('shell'),
        ]
        extra = self.request()
        extra['endpoint'] = 'other-host:1234'
        bad.append(extra)
        for request in bad:
            with self.subTest(request=request), self.assertRaises(BenchmarkError):
                operations.validate(request)

    def test_rpc_create_pool(self):
        response = msgbus.TResponse(Status=1)
        response.BlobStorageConfigResponse.Success = True
        request = self.request(
            'create-pool', name='pool', box=1, groups=2, domains=3, domain_begin=10, domain_end=40, disk_type='SSD'
        )
        with mock.patch.object(operations.grpc, 'insecure_channel'), mock.patch.object(
            legacy, 'TGRpcServerStub'
        ) as stub:
            stub.return_value.BlobStorageConfig.return_value = response
            result = operations.invoke('host:1', 7, request)
            sent = stub.return_value.BlobStorageConfig.call_args.args[0]
            self.assertEqual(7, sent.Domain)
            self.assertEqual(2, sent.Request.Command[0].DefineDDiskPool.NumDDiskGroups)
            self.assertEqual(3, sent.Request.Command[0].DefineDDiskPool.Geometry.NumFailDomainsPerFailRealm)
            self.assertEqual('SUCCESS', result['status'])
            self.assertEqual(10, stub.return_value.BlobStorageConfig.call_args.kwargs['timeout'])

    def test_rpc_nbs_write_and_read(self):
        write_response = nbs.WriteBlocksResponse()
        write_response.operation.ready = True
        write_response.operation.status = StatusIds.SUCCESS
        write_response.operation.result.Pack(nbs.WriteBlocksResult())
        read_response = nbs.ReadBlocksResponse()
        read_response.operation.CopyFrom(write_response.operation)
        read_response.operation.result.Pack(nbs.ReadBlocksResult(Blocks=nbs.IOVector(Buffers=[b'abab'])))
        with mock.patch.object(operations.grpc, 'insecure_channel'), mock.patch.object(
            nbs_grpc, 'NbsServiceStub'
        ) as stub:
            resolved = nbs.GetLoadActorAdapterActorIdResponse()
            resolved.operation.ready, resolved.operation.status = True, StatusIds.SUCCESS
            resolved.operation.result.Pack(nbs.GetLoadActorAdapterActorIdResult(ActorId='[50000:123:456]'))
            stub.return_value.GetLoadActorAdapterActorId.return_value = resolved
            stub.return_value.WriteBlocks.return_value = write_response
            stub.return_value.ReadBlocks.return_value = read_response
            operations.invoke(
                'host:1',
                'Root',
                self.request('write', disk_id='test', start=3, blocks_count=2, block_size=4, pattern='ab'),
            )
            sent = stub.return_value.WriteBlocks.call_args.args[0]
            self.assertEqual([b'abab', b'abab'], list(sent.Blocks.Buffers))
            self.assertEqual(3, sent.StartIndex)
            self.assertEqual('[50000:123:456]', sent.DiskId)
            result = operations.invoke('host:1', 'Root', self.request())
            self.assertEqual(['YWJhYg=='], result['Blocks']['Buffers'])
            self.assertEqual('[50000:123:456]', stub.return_value.ReadBlocks.call_args.args[0].DiskId)
            self.assertEqual(2, stub.return_value.GetLoadActorAdapterActorId.call_count)
            self.assertEqual('test', stub.return_value.GetLoadActorAdapterActorId.call_args.args[0].DiskId)
            write_response.operation.result.Pack(nbs_io.TWriteBlocksResponse())
            request = self.request('write', disk_id='test', start=0, blocks_count=1, block_size=4, pattern='ab')
            self.assertEqual({}, operations.invoke('host:1', 1, request))
            rejected = nbs_io.TWriteBlocksResponse()
            rejected.Error.Code = 0x80000001
            rejected.Error.Message = 'rejected'
            write_response.operation.result.Pack(rejected)
            with self.assertRaisesRegex(BenchmarkError, 'rejected'):
                operations.invoke('host:1', 1, request)

    def test_resolution_failure_never_sends_io(self):
        with mock.patch.object(operations.grpc, 'insecure_channel'), mock.patch.object(
            nbs_grpc, 'NbsServiceStub'
        ) as stub:
            response = nbs.GetLoadActorAdapterActorIdResponse()
            stub.return_value.GetLoadActorAdapterActorId.return_value = response
            for ready, status, actor_id in [
                (False, StatusIds.SUCCESS, ''),
                (True, StatusIds.NOT_FOUND, ''),
                (True, StatusIds.SUCCESS, ''),
            ]:
                response.operation.ready, response.operation.status = ready, status
                response.operation.result.Pack(nbs.GetLoadActorAdapterActorIdResult(ActorId=actor_id))
                for request in [
                    self.request(),
                    self.request('write', disk_id='test', start=0, blocks_count=1, block_size=4096, pattern='x'),
                ]:
                    with self.assertRaises(BenchmarkError):
                        operations.invoke('host:1', 1, request)
            stub.return_value.ReadBlocks.assert_not_called()
            stub.return_value.WriteBlocks.assert_not_called()

    def test_rpc_nbs_create(self):
        response = nbs.CreatePartitionResponse()
        response.operation.ready, response.operation.status = True, StatusIds.SUCCESS
        response.operation.result.Pack(nbs.CreatePartitionResult(TabletId='123'))
        request = self.request(
            'create-partition',
            disk_id='test',
            pool='pool',
            block_size=4096,
            blocks_count=100,
            media='ssd',
            batch_size=100,
        )
        with mock.patch.object(operations.grpc, 'insecure_channel'), mock.patch.object(
            nbs_grpc, 'NbsServiceStub'
        ) as stub:
            stub.return_value.CreatePartition.return_value = response
            self.assertEqual({'TabletId': '123'}, operations.invoke('host:1', 'Root', request))
            self.assertEqual('pool', stub.return_value.CreatePartition.call_args.args[0].StoragePoolName)
            response.operation.ready = False
            with self.assertRaises(RuntimeError):
                operations.invoke('host:1', 'Root', request)

    def test_create_ambiguous_status_is_persisted_and_not_replayed(self):
        with mock.patch.object(operations.grpc, 'insecure_channel'), mock.patch.object(
            nbs_grpc, 'NbsServiceStub'
        ) as stub:
            response = nbs.CreatePartitionResponse()
            response.operation.ready = True
            stub.return_value.CreatePartition.return_value = response
            for status, expected in [
                (StatusIds.UNAVAILABLE, 'unknown'),
                (StatusIds.GENERIC_ERROR, 'unknown'),
                (StatusIds.TIMEOUT, 'unknown'),
                (StatusIds.NOT_FOUND, 'unknown'),
                (StatusIds.BAD_REQUEST, 'failed'),
                (StatusIds.UNAUTHORIZED, 'failed'),
                (StatusIds.UNSUPPORTED, 'failed'),
                (StatusIds.ALREADY_EXISTS, 'failed'),
            ]:
                with self.subTest(status=status):
                    response.operation.status = status
                    request = self.request(
                        'create-partition',
                        disk_id='test',
                        pool='pool',
                        block_size=4096,
                        blocks_count=100,
                        media='ssd',
                        batch_size=100,
                    )
                    row = self.manager.execute(request)
                    self.assertEqual(expected, row['status'])
                    count = stub.return_value.CreatePartition.call_count
                    restored = operations.ClusterOperations(self.root, self.cluster)
                    self.assertEqual(row, restored.execute(request))
                    self.assertEqual(count, stub.return_value.CreatePartition.call_count)

    def test_read_rpc_error_is_not_success(self):
        with mock.patch.object(operations.grpc, 'insecure_channel'), mock.patch.object(
            nbs_grpc, 'NbsServiceStub'
        ) as stub:
            resolved = nbs.GetLoadActorAdapterActorIdResponse()
            resolved.operation.ready, resolved.operation.status = True, StatusIds.SUCCESS
            resolved.operation.result.Pack(nbs.GetLoadActorAdapterActorIdResult(ActorId='[50000:123:456]'))
            stub.return_value.GetLoadActorAdapterActorId.return_value = resolved
            response = nbs.ReadBlocksResponse()
            response.operation.ready, response.operation.status = True, StatusIds.GENERIC_ERROR
            response.operation.issues.add().message = 'cross-stripe read'
            stub.return_value.ReadBlocks.return_value = response
            row = self.manager.execute(self.request(disk_id='test', start=127, blocks_count=2))
            self.assertEqual('failed', row['status'])
            self.assertIn('cross-stripe read', row['error'])
            self.assertNotIn('response', row)

    def test_release_rejects_new_requests_but_keeps_history(self):
        root = self.root / 'run'
        root.mkdir()
        run = {
            'lock': threading.RLock(),
            'cluster_operations': self.manager,
            'finalized': False,
            'cancel': threading.Event(),
            'release_cluster': threading.Event(),
            'store': SimpleNamespace(manifest={'deployment': {'phase': 'cluster-ready'}}),
        }
        service = SimpleNamespace(output=self.root, _lock=threading.RLock(), _runs={'run': run})
        self.assertTrue(web.RunService.cluster_operation(service, 'run')['active'])
        run['release_cluster'].set()
        with mock.patch.object(operations, 'invoke') as invoke:
            with self.assertRaises(BenchmarkError):
                web.RunService.cluster_operation(service, 'run', self.request())
            invoke.assert_not_called()
        self.assertFalse(web.RunService.cluster_operation(service, 'run')['active'])
        self.assertTrue(hosts.allowed_post_path('/api/runs/run/cluster-operations'))
