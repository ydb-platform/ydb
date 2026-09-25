"""Direct classic RPCs: no implicit mount, I/O retries or error translation."""

from itertools import count

from ydb.core.nbs.nbs1_compat_api.cloud.blockstore.public.api.protos import io_pb2, mount_pb2, ping_pb2

CLIENT_ID = 'classic-nbs-grpc-functional'
# Wire result codes from cloud/storage/core/libs/common/error.h.
E_ARGUMENT = 0x80000001
E_NOT_FOUND = 0x80000006
E_BS_INVALID_SESSION = 0x80050001


class ClassicNbsGrpcClient:
    def __init__(self, channel):
        self.request_ids = count(1)
        # The isolated protobuf package is not the public classic service name.
        self.methods = {
            method: channel.unary_unary(
                '/NCloud.NBlockStore.NProto.TBlockStoreService/' + method,
                request_serializer=lambda request: request.SerializeToString(),
                response_deserializer=response_type.FromString,
            )
            for method, response_type in (
                ('Ping', ping_pb2.TPingResponse),
                ('MountVolume', mount_pb2.TMountVolumeResponse),
                ('UnmountVolume', mount_pb2.TUnmountVolumeResponse),
                ('ReadBlocks', io_pb2.TReadBlocksResponse),
                ('WriteBlocks', io_pb2.TWriteBlocksResponse),
            )
        }

    def call(self, method, request, expected_code=0, client_id=CLIENT_ID):
        request.Headers.ClientId = client_id
        request.Headers.RequestId = next(self.request_ids)
        response = self.methods[method](request, timeout=30)
        if expected_code is not None:
            assert response.Error.Code == expected_code, (
                f'{method} #{request.Headers.RequestId}: ' f'expected {expected_code:#x}, got {response.Error}'
            )
        return response
