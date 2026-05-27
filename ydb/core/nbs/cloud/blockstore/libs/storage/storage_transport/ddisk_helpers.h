#pragma once

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage {

struct TDDiskIdLess
{
    using TDDiskId = NKikimrBlobStorage::NDDisk::TDDiskId;
    bool operator()(const TDDiskId& lhs, const TDDiskId& rhs) const;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage
