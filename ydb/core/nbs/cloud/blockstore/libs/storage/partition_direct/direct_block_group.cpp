#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/ic_storage_transport.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TDBGWriteBlocksToManyPBuffersResponse
TDBGWriteBlocksToManyPBuffersResponse::MakeOverallError(
    EWellKnownResultCodes code,
    TString reason)
{
    TDBGWriteBlocksToManyPBuffersResponse result;
    result.OverallError = MakeError(code, std::move(reason));
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool TDDiskIdLess::operator()(const TDDiskId& lh, const TDDiskId& rh) const
{
    auto makeTuple = [](const TDDiskId& item)
    {
        return std::make_tuple(
            item.GetNodeId(),
            item.GetPDiskId(),
            item.GetDDiskSlotId());
    };
    return makeTuple(lh) < makeTuple(rh);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
