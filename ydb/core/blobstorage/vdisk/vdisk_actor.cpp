#include "vdisk_actor.h"
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_skeletonfront.h>

namespace NKikimr {

    IActor* CreateVDisk(const TIntrusivePtr<TVDiskConfig> &cfg,
                        const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                        const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters) {
        // some global checks, it's better to VERIFY now than later
        IngressGlobalCheck(info.Get());

        // create front actor
        return CreateVDiskSkeletonFront(cfg, info, counters);
    }

} // NKikimr
