#include "request_helpers.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IOutputStream& operator<<(IOutputStream& out, const TRequestInfo& info)
{
    if (info.DiskId) {
        out << "[d:" << info.DiskId << "] ";
    }

    if (info.ClientId) {
        out << "[c:" << info.ClientId << "] ";
    }

    if (info.InstanceId) {
        out << "[i:" << info.InstanceId << "] ";
    }

    out << GetBlockStoreRequestName(info.Request);

    if (info.RequestId) {
        out << " #" << info.RequestId;
    }

    return out;
}

ui64 CreateRequestId()
{
    return NYdb::NBS::RandInt<ui64, 1>();
}


bool IsReadWriteMode(const NProto::EVolumeAccessMode mode)
{
    switch (mode) {
        case NProto::VOLUME_ACCESS_READ_ONLY:
        case NProto::VOLUME_ACCESS_USER_READ_ONLY:
            return false;
        case NProto::VOLUME_ACCESS_READ_WRITE:
        case NProto::VOLUME_ACCESS_REPAIR:
            return true;
        default:
            Y_DEBUG_ABORT_UNLESS(false, "Unknown EVolumeAccessMode: %d", mode);
            return false;
    }
}

TString GetIpcTypeString(NProto::EClientIpcType ipcType)
{
    switch (ipcType) {
        case NProto::IPC_GRPC:
            return "grpc";
        case NProto::IPC_NBD:
            return "nbd";
        case NProto::IPC_VHOST:
            return "vhost";
        case NProto::IPC_NVME:
            return "nvme";
        case NProto::IPC_SCSI:
            return "scsi";
        case NProto::IPC_RDMA:
            return "rdma";
        default:
            return "undefined";
    }
}

ui64 CalculateBytesCount(
    const NProto::TWriteBlocksRequest& request,
    const ui32 blockSize)
{
    Y_UNUSED(blockSize);

    ui64 bytesInBuffers = 0;
    for (const auto& buffer: request.GetBlocks().GetBuffers()) {
        bytesInBuffers += buffer.size();
    }
    return bytesInBuffers;
}


ui32 CalculateWriteRequestBlockCount(
    const NProto::TWriteBlocksRequest& request,
    const ui32 blockSize)
{
    ui32 bytes = 0;
    for (const auto& buffer: request.GetBlocks().GetBuffers()) {
        bytes += buffer.size();
    }
    if (bytes % blockSize) {
        Y_ABORT("bytes %u not divisible by blockSize %u", bytes, blockSize);
    }

    return bytes / blockSize;
}

ui32 CalculateWriteRequestBlockCount(
    const NProto::TWriteBlocksLocalRequest& request,
    const ui32 blockSize)
{
    if (blockSize != request.GetBlockSize()) {
        [[maybe_unused]] ui64 sglistSize = 0;
        [[maybe_unused]] ui64 sglistBlockSize = blockSize;
        {
            auto guard = request.Sglist.Acquire();
            if (guard && guard.Get().size()) {
                sglistSize = guard.Get().size();
                sglistBlockSize = guard.Get()[0].Size();
            }
        }

        Y_DEBUG_ABORT_UNLESS(
            false,
            "blockSize %u != request.BlockSize %u, request.BlocksCount=%u"
            ", sglist size=%lu, sglist buffer size=%lu",
            blockSize,
            request.GetBlockSize(),
            request.BlocksCount,
            sglistSize,
            sglistBlockSize);
    }

    return request.BlocksCount;
}

ui32 CalculateWriteRequestBlockCount(
    const NProto::TZeroBlocksRequest& request,
    const ui32 blockSize)
{
    Y_UNUSED(blockSize);

    return request.GetBlocksCount();
}


}   // namespace NCloud::NBlockStore
