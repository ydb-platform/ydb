#pragma once

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/protos/blobstorage_ddisk.pb.h>

#include <span>

namespace NYdb::NBS::NBlockStore::NStorage {

struct TDDiskIdLess
{
    using TDDiskId = NKikimrBlobStorage::NDDisk::TDDiskId;
    bool operator()(const TDDiskId& lhs, const TDDiskId& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] std::unique_ptr<NKikimr::NDDisk::TEvWritePersistentBuffersResult>
MakeWritePersistentBuffersResult(
    NKikimrBlobStorage::NDDisk::TReplyStatus_E status,
    TStringBuf reason,
    std::span<const NKikimrBlobStorage::NDDisk::TDDiskId> pbufferIds);

}   // namespace NYdb::NBS::NBlockStore::NStorage
