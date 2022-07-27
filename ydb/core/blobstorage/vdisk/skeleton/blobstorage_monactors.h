#pragma once
#include "defs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/protos/node_whiteboard.pb.h>

namespace NKikimr {

    class TDb;
    struct TVDiskConfig;
    IActor *CreateSkeletonMonRequestHandler(TIntrusivePtr<TDb> &db,
                                            NMon::TEvHttpInfo::TPtr &ev,
                                            const TActorId notifyId,
                                            const TActorId &localRecovActorID);

    IActor *CreateFrontSkeletonMonRequestHandler(const TVDiskID &selfVDiskId,
                                                 const TActorId &notifyId,
                                                 const TActorId &skeletonID,
                                                 const TActorId &skeletonFrontID,
                                                 TIntrusivePtr<TVDiskConfig> cfg,
                                                 const std::shared_ptr<TBlobStorageGroupInfo::TTopology> &top,
                                                 NMon::TEvHttpInfo::TPtr &ev,
                                                 const TString &frontHtml);

    IActor *CreateFrontSkeletonGetLogoBlobRequestHandler(const TVDiskID &selfVDiskId,
                                                         const TActorId &notifyId,
                                                         const TActorId &skeletonID,
                                                         TIntrusivePtr<TVDiskConfig> cfg,
                                                         const std::shared_ptr<TBlobStorageGroupInfo::TTopology> &top,
                                                         TEvGetLogoBlobRequest::TPtr &ev);


} // NKikimr

