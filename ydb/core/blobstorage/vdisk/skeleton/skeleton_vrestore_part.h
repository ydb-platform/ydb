#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace NKikimr {

    IActor* CreateVRestorePartActor(const TVDiskID& selfVDiskId, const TActorId& notifyId,
        const TActorId& skeletonId, TIntrusivePtr<TVDiskConfig> cfg,
        std::shared_ptr<TBlobStorageGroupInfo::TTopology> top, TEvVRestorePart::TPtr& ev);

} // NKikimr
