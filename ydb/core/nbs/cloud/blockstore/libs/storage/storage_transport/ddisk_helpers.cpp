#include "ddisk_helpers.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error_utils.h>

namespace NYdb::NBS::NBlockStore::NStorage {

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

std::unique_ptr<NKikimr::NDDisk::TEvWritePersistentBuffersResult>
MakeWritePersistentBuffersResult(
    NKikimrBlobStorage::NDDisk::TReplyStatus_E status,
    TStringBuf reason,
    std::span<const NKikimrBlobStorage::NDDisk::TDDiskId> pbufferIds)
{
    auto errorResponse =
        std::make_unique<NKikimr::NDDisk::TEvWritePersistentBuffersResult>();
    for (const auto& pbufferId: pbufferIds) {
        auto* res = errorResponse->Record.AddResult();
        *res->MutablePersistentBufferId() = pbufferId;
        SetErrorStatus(status, reason, *res->MutableResult());
    }
    return errorResponse;
}

}   // namespace NYdb::NBS::NBlockStore::NStorage
