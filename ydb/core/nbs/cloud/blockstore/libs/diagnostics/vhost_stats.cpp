#include "vhost_stats.h"

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

TBlockRange64 GetBlockRange(ui64 start, ui64 size, ui32 blockSize)
{
    const ui64 startBlock = start / blockSize;
    const ui64 endBlock = (start + size + blockSize - 1) / blockSize;
    return TBlockRange64::WithLength(startBlock, endBlock - startBlock);
}

bool IsUnaligned(ui64 start, ui64 size, ui32 blockSize)
{
    return start % blockSize != 0 || size % blockSize != 0;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TMetricRequest::TMetricRequest(
    EBlockStoreRequest requestType,
    const TString& clientId,
    const TString& diskId,
    ui64 start,
    ui64 size,
    ui32 blockSize)
    : RequestType(requestType)
    , ClientId(clientId)
    , DiskId(diskId)
    , BlockSize(blockSize)
    , Range(GetBlockRange(start, size, blockSize))
    , Unaligned(IsUnaligned(start, size, blockSize))
{}

ui32 TMetricRequest::GetRequestBytes() const
{
    return Range.Size() * BlockSize;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
