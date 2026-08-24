#pragma once

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr::NVPatch {

bool HasMirror3dcQuorum(const TBlobStorageGroupInfo& info, ui32 successfulSubgroupMask);

} // namespace NKikimr::NVPatch
