import time

import grpc
import pytest

from ydb.core.nbs.nbs1_compat_api.cloud.blockstore.public.api.protos import mount_pb2
from ydb.tests.functional.nbs.lib.common import NbsTestBase
from ydb.tests.functional.nbs.lib.fixtures.cluster import NbsCluster
from ydb.tests.functional.nbs.lib.fixtures.geometry import DEFAULT_BLOCK_SIZE, REGION_SIZE
from ydb.tests.functional.nbs.F6_classic_nbs_grpc.grpc_client import ClassicNbsGrpcClient, E_NOT_FOUND

pytest_plugins = ['ydb.tests.functional.nbs.lib.fixtures.pytest_timeout_conf']


@pytest.fixture(scope='session')
def nbs_cluster():
    cluster = NbsCluster(enable_frontend=True)
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture(scope='session')
def grpc_client(nbs_cluster):
    slot = nbs_cluster.slots[0]
    # A 32 MiB I/O payload needs room for protobuf framing as well.
    options = [
        ('grpc.enable_retries', 0),
        ('grpc.max_send_message_length', 64 * 1024 * 1024),
        ('grpc.max_receive_message_length', 64 * 1024 * 1024),
    ]
    with grpc.insecure_channel(f'{slot.host}:{slot.grpc_port}', options=options) as channel:
        grpc.channel_ready_future(channel).result(timeout=30)
        yield ClassicNbsGrpcClient(channel)


def wait_for_registration(client, disk_id, present):
    # Create/DeletePartition can return before the frontend publishes/revokes it.
    deadline = time.monotonic() + 60
    while True:
        response = client.call(
            'MountVolume',
            mount_pb2.TMountVolumeRequest(DiskId=disk_id),
            expected_code=None,
        )
        assert response.Error.Code in (0, E_NOT_FOUND), response.Error
        if (response.Error.Code == 0) == present:
            return response
        assert time.monotonic() < deadline, f'Frontend registration {disk_id}: present={present}'
        time.sleep(0.1)


@pytest.fixture
def mounted_volume(request, nbs_cluster, grpc_client):
    block_size = getattr(request, 'param', DEFAULT_BLOCK_SIZE)
    blocks_count = REGION_SIZE // block_size
    helper = NbsTestBase()
    helper.cluster = nbs_cluster.cluster
    helper.ddisk_pool_name = nbs_cluster.ddisk_pool_name
    nbs_cluster.assert_healthy()
    disk_id = helper.generate_disk_id()
    helper.create_disk(disk_id, blocks_count=blocks_count, block_size=block_size)
    try:
        mounted = wait_for_registration(grpc_client, disk_id, present=True)
        assert mounted.Volume.DiskId == disk_id
        assert mounted.Volume.BlockSize == block_size
        assert mounted.Volume.BlocksCount == blocks_count
        assert mounted.SessionId
        yield mounted
    finally:
        # All RPCs are synchronous; deletion also revokes any remaining session.
        # Wait for revocation before the next case reuses cluster resources.
        helper.delete_disk(disk_id)
        wait_for_registration(grpc_client, disk_id, present=False)


@pytest.fixture
def two_mounted_volumes(nbs_cluster, grpc_client, mounted_volume):
    helper = NbsTestBase()
    helper.cluster = nbs_cluster.cluster
    helper.ddisk_pool_name = nbs_cluster.ddisk_pool_name
    disk_id = helper.generate_disk_id()
    block_size = mounted_volume.Volume.BlockSize * 2
    helper.create_disk(disk_id, blocks_count=REGION_SIZE // block_size, block_size=block_size)
    try:
        second = wait_for_registration(grpc_client, disk_id, present=True)
        yield mounted_volume, second
    finally:
        helper.delete_disk(disk_id)
        wait_for_registration(grpc_client, disk_id, present=False)
