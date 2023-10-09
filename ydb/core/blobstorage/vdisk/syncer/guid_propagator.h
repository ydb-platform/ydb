#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/blobstorage_vdisk_guids.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // SYNCER GUID PROPAGATOR
    // This actor is responsible for writing our Guid to some other node;
    // node may go offline, be formatted, etc, we would continuously try
    // to save our Guid their
    ////////////////////////////////////////////////////////////////////////////
    class TVDiskContext;
    struct TVDiskID;
    IActor* CreateSyncerGuidPropagator(TIntrusivePtr<TVDiskContext> vctx,
                                       const TVDiskID &selfVDiskId,
                                       const TVDiskID &targetVDiskId,
                                       const TActorId &targetServiceId,
                                       NKikimrBlobStorage::TSyncGuidInfo::EState state,
                                       TVDiskEternalGuid guid);

} // NKikimr
