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

ui64 CreateRequestId()
{
    static std::atomic<ui64> RequestId(0);
    return RequestId.fetch_add(1);
}

}   // namespace NYdb::NBS::NBlockStore
