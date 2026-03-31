#include "request.h"

namespace NYdb::NBS::NBlockStore {

TRequestHeaders TRequestHeaders::Clone(TBlockRange64 range) const
{
    return TRequestHeaders{
        .VolumeConfig = VolumeConfig,
        .ClientId = ClientId,
        .RequestId = RequestId,
        .Range = range,
        .Timestamp = Timestamp};
}

size_t TRequestHeaders::GetRequestSize() const
{
    return Range.Size() * VolumeConfig->BlockSize;
}

TStringBuf ToStringBuf(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ReadBlocks:
            return "ReadBlocks";
        case EBlockStoreRequest::WriteBlocks:
            return "WriteBlocks";
        case EBlockStoreRequest::ZeroBlocks:
            return "ZeroBlocks";
        case EBlockStoreRequest::MAX:
            break;
    }
    Y_ABORT_UNLESS(false);
}

ui64 CreateRequestId()
{
    static std::atomic<ui64> RequestId(0);
    return RequestId.fetch_add(1);
}

}   // namespace NYdb::NBS::NBlockStore
