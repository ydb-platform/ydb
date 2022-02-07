#pragma once
#include "defs.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    struct TVDiskConfig;
    class TBlobStorageGroupInfo;
    class TVDiskContext;
    using TVDiskContextPtr = TIntrusivePtr<TVDiskContext>;

    IActor* CreateVDiskSkeleton(const TIntrusivePtr<TVDiskConfig> &cfg,
                                const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                                const TActorId &skeletonFrontID,
                                const TVDiskContextPtr &vctx);

} // NKikimr
