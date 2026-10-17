#pragma once

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr::NVPatch {

bool HasMirror3dcQuorum(const TBlobStorageGroupInfo& info, ui32 successfulSubgroupMask);
ui32 SelectMirror3dcQuorum(const TBlobStorageGroupInfo& info, ui32 availableSubgroupMask);

} // namespace NKikimr::NVPatch
