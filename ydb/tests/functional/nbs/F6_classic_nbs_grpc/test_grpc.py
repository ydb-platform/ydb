"""Classic wire API against a real NBS2 partition, without an NBS1 daemon."""

import struct

import pytest

from ydb.core.nbs.nbs1_compat_api.cloud.blockstore.public.api.protos import io_pb2, mount_pb2, ping_pb2
from ydb.tests.functional.nbs.lib.fixtures.geometry import SUPPORTED_BLOCK_SIZES, blocks_per_stripe
from ydb.tests.functional.nbs.F6_classic_nbs_grpc.grpc_client import (
    E_ARGUMENT,
    E_BS_INVALID_SESSION,
    E_NOT_FOUND,
)


def pattern(start, count, block_size):
    # Different nonzero contents per block expose offset/order mistakes.
    return b''.join(
        struct.pack('<Q', index ^ 0xA5A5A5A5A5A5A5A5) * (block_size // 8) for index in range(start, start + count)
    )


def read_request(mounted, start=0, count=1):
    return io_pb2.TReadBlocksRequest(
        DiskId=mounted.Volume.DiskId,
        SessionId=mounted.SessionId,
        BlockSize=mounted.Volume.BlockSize,
        StartIndex=start,
        BlocksCount=count,
    )


def write_request(mounted, data, start=0):
    request = io_pb2.TWriteBlocksRequest(
        DiskId=mounted.Volume.DiskId,
        SessionId=mounted.SessionId,
        BlockSize=mounted.Volume.BlockSize,
        StartIndex=start,
    )
    request.Blocks.Buffers.append(data)
    return request


def assert_read(client, mounted, start, expected):
    block_size = mounted.Volume.BlockSize
    count = len(expected) // block_size
    response = client.call('ReadBlocks', read_request(mounted, start, count))
    assert len(response.Blocks.Buffers) == count
    assert all(len(buffer) == block_size for buffer in response.Blocks.Buffers)
    assert b''.join(response.Blocks.Buffers) == expected


def test_ping(grpc_client):
    grpc_client.call('Ping', ping_pb2.TPingRequest())


def test_mount_is_idempotent(grpc_client, mounted_volume):
    response = grpc_client.call(
        'MountVolume',
        mount_pb2.TMountVolumeRequest(DiskId=mounted_volume.Volume.DiskId),
    )
    assert response.Volume == mounted_volume.Volume
    assert response.SessionId == mounted_volume.SessionId
    assert response.InactiveClientsTimeout == 0


def test_independent_sessions_and_io_per_disk(grpc_client, two_mounted_volumes):
    first, second = two_mounted_volumes
    assert first.SessionId != second.SessionId
    first_data = b'a' * first.Volume.BlockSize
    second_data = b'b' * second.Volume.BlockSize
    grpc_client.call('WriteBlocks', write_request(first, first_data))
    grpc_client.call('WriteBlocks', write_request(second, second_data))
    assert_read(grpc_client, first, 0, first_data)
    assert_read(grpc_client, second, 0, second_data)

    # A valid token from another disk grants neither I/O nor unmount access.
    request = read_request(second)
    request.SessionId = first.SessionId
    grpc_client.call('ReadBlocks', request, E_BS_INVALID_SESSION)
    grpc_client.call(
        'UnmountVolume',
        mount_pb2.TUnmountVolumeRequest(DiskId=second.Volume.DiskId, SessionId=first.SessionId),
        E_BS_INVALID_SESSION,
    )
    grpc_client.call(
        'UnmountVolume',
        mount_pb2.TUnmountVolumeRequest(DiskId=first.Volume.DiskId, SessionId=first.SessionId),
    )
    grpc_client.call('ReadBlocks', read_request(first), E_BS_INVALID_SESSION)
    assert_read(grpc_client, second, 0, second_data)


def test_unmount_and_remount(grpc_client, mounted_volume):
    disk_id = mounted_volume.Volume.DiskId
    data = pattern(0, 1, mounted_volume.Volume.BlockSize)
    grpc_client.call('WriteBlocks', write_request(mounted_volume, data))
    grpc_client.call(
        'UnmountVolume',
        mount_pb2.TUnmountVolumeRequest(
            DiskId=disk_id,
            SessionId=mounted_volume.SessionId,
        ),
    )
    grpc_client.call('ReadBlocks', read_request(mounted_volume), E_BS_INVALID_SESSION)
    grpc_client.call('WriteBlocks', write_request(mounted_volume, b'!' * len(data)), E_BS_INVALID_SESSION)

    remounted = grpc_client.call('MountVolume', mount_pb2.TMountVolumeRequest(DiskId=disk_id))
    assert remounted.SessionId
    assert remounted.SessionId != mounted_volume.SessionId
    grpc_client.call('ReadBlocks', read_request(mounted_volume), E_BS_INVALID_SESSION)
    assert_read(grpc_client, remounted, 0, data)
    replacement = b'w' * len(data)
    grpc_client.call('WriteBlocks', write_request(remounted, replacement))
    assert_read(grpc_client, remounted, 0, replacement)
    grpc_client.call(
        'UnmountVolume',
        mount_pb2.TUnmountVolumeRequest(
            DiskId=disk_id,
            SessionId=remounted.SessionId,
        ),
    )


@pytest.mark.parametrize('mounted_volume', SUPPORTED_BLOCK_SIZES, indirect=True)
def test_native_block_sizes(grpc_client, mounted_volume):
    data = pattern(0, 3, mounted_volume.Volume.BlockSize)
    grpc_client.call('WriteBlocks', write_request(mounted_volume, data))
    assert_read(grpc_client, mounted_volume, 0, data)


@pytest.mark.timeout(120, func_only=True)
@pytest.mark.parametrize('layout', ['one-block', 'several-blocks', 'stripe-crossing', 'fragmented', '32-MiB'])
def test_io_layouts(grpc_client, mounted_volume, layout):
    block_size = mounted_volume.Volume.BlockSize
    start, count = {
        'one-block': (0, 1),
        'several-blocks': (3, 7),
        'stripe-crossing': (blocks_per_stripe(block_size) - 1, 2),
        'fragmented': (3, 1),
        '32-MiB': (1024, 32 * 1024 * 1024 // block_size),
    }[layout]
    data = pattern(start, count, block_size)
    request = write_request(mounted_volume, data, start)
    if layout == 'fragmented':
        # Buffer boundaries need not match logical block boundaries.
        del request.Blocks.Buffers[:]
        request.Blocks.Buffers.extend((data[: block_size // 2], data[block_size // 2 :]))
    grpc_client.call('WriteBlocks', request)
    assert_read(grpc_client, mounted_volume, start, data)


@pytest.mark.parametrize('identity', ['client', 'session'])
def test_invalid_io_identity(grpc_client, mounted_volume, identity):
    data = pattern(0, 1, mounted_volume.Volume.BlockSize)
    grpc_client.call('WriteBlocks', write_request(mounted_volume, data))
    for method, request in (
        ('ReadBlocks', read_request(mounted_volume)),
        ('WriteBlocks', write_request(mounted_volume, b'!' * len(data))),
    ):
        if identity == 'session':
            request.SessionId = 'unknown-session'
            grpc_client.call(method, request, E_BS_INVALID_SESSION)
        else:
            grpc_client.call(method, request, E_BS_INVALID_SESSION, client_id='another-client')
    assert_read(grpc_client, mounted_volume, 0, data)


def test_unknown_disk(grpc_client, mounted_volume):
    data = pattern(0, 1, mounted_volume.Volume.BlockSize)
    for method, request in (
        ('MountVolume', mount_pb2.TMountVolumeRequest()),
        ('UnmountVolume', mount_pb2.TUnmountVolumeRequest(SessionId=mounted_volume.SessionId)),
        ('ReadBlocks', read_request(mounted_volume)),
        ('WriteBlocks', write_request(mounted_volume, data)),
    ):
        request.DiskId = mounted_volume.Volume.DiskId + '-missing'
        grpc_client.call(method, request, E_NOT_FOUND)


def test_out_of_range_io(grpc_client, mounted_volume):
    last = mounted_volume.Volume.BlocksCount - 1
    data = pattern(last, 1, mounted_volume.Volume.BlockSize)
    grpc_client.call('WriteBlocks', write_request(mounted_volume, data, last))
    grpc_client.call('ReadBlocks', read_request(mounted_volume, last, 2), E_ARGUMENT)
    grpc_client.call('WriteBlocks', write_request(mounted_volume, b'!' * (2 * len(data)), last), E_ARGUMENT)
    assert_read(grpc_client, mounted_volume, last, data)


def test_invalid_write_preserves_data(grpc_client, mounted_volume):
    data = pattern(0, 1, mounted_volume.Volume.BlockSize)
    grpc_client.call('WriteBlocks', write_request(mounted_volume, data))
    grpc_client.call('WriteBlocks', write_request(mounted_volume, b'!' * (len(data) - 1)), E_ARGUMENT)
    assert_read(grpc_client, mounted_volume, 0, data)
