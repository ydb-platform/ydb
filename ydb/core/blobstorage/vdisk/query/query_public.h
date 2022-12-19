#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace NKikimr {

    struct THullCtx;
    class TPDiskCtx;


    //////////////////////////////////////////////////////////////////////////////////////
    // TQueryCtx - a context for VDisk get queries
    //////////////////////////////////////////////////////////////////////////////////////
    struct TQueryCtx {
        TIntrusivePtr<THullCtx> HullCtx;
        TPDiskCtxPtr PDiskCtx;
        NMonGroup::TInterfaceGroup MonGroup;
        std::atomic<ui64> PDiskReadBytes;
        TActorId SkeletonId;

        TQueryCtx(TIntrusivePtr<THullCtx> hullCtx, TPDiskCtxPtr pdiskCtx, TActorId skeletonId)
            : HullCtx(std::move(hullCtx))
            , PDiskCtx(std::move(pdiskCtx))
            , MonGroup(HullCtx->VCtx->VDiskCounters, "subsystem", "interface")
            , PDiskReadBytes(0)
            , SkeletonId(skeletonId)
        {}
    };

    // a function for fast check of keep/don't keep
    using TReadQueryKeepChecker = std::function<bool (const TLogoBlobID& id,
        bool keepByIngress,
        TString *explanation)>;

    //////////////////////////////////////////////////////////////////////////////////////
    // CreateLevelIndexQueryActor
    // Handle a TEvVGet query on Hull database snapshot
    //////////////////////////////////////////////////////////////////////////////////////
    IActor *CreateLevelIndexQueryActor(
                std::shared_ptr<TQueryCtx> &queryCtx,
                TReadQueryKeepChecker &&keepChecker,
                const TActorContext &ctx,
                THullDsSnap &&fullSnap,
                const TActorId &parentId,
                TEvBlobStorage::TEvVGet::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetResult> result,
                TActorId replSchedulerId);

    //////////////////////////////////////////////////////////////////////////////////////
    // TEvVGet query check
    //////////////////////////////////////////////////////////////////////////////////////
    bool CheckVGetQuery(const NKikimrBlobStorage::TEvVGet &record);


    ////////////////////////////////////////////////////////////////////////////
    // CreateLevelIndexBarrierQuery
    // Handle a TEvVGet query on Hull database snapshot
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateLevelIndexBarrierQueryActor(
                TIntrusivePtr<THullCtx> &hullCtx,
                const TActorId &parentId,
                TBarriersSnapshot &&barriersSnap,
                TEvBlobStorage::TEvVGetBarrier::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetBarrierResult> result);

    ////////////////////////////////////////////////////////////////////////////
    // Check Barrier Query
    ////////////////////////////////////////////////////////////////////////////
    bool CheckVGetBarrierQuery(const NKikimrBlobStorage::TEvVGetBarrier &record);


    ////////////////////////////////////////////////////////////////////////////
    // CreateDbStatActor
    // Handle a TEvVDbStat query on Hull database snapshot
    ////////////////////////////////////////////////////////////////////////////
    class THugeBlobCtx;
    IActor *CreateDbStatActor(
            const TIntrusivePtr<THullCtx> &hullCtx,
            const std::shared_ptr<THugeBlobCtx> &hugeBlobCtx,
            const TActorContext &ctx,
            THullDsSnap &&fullSnap,
            const TActorId &parentId,
            TEvBlobStorage::TEvVDbStat::TPtr &ev,
            std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result);

    IActor *CreateDbStatActor(
            const TIntrusivePtr<THullCtx> &hullCtx,
            const std::shared_ptr<THugeBlobCtx> &hugeBlobCtx,
            const TActorContext &ctx,
            THullDsSnap &&fullSnap,
            const TActorId &parentId,
            TEvGetLogoBlobIndexStatRequest::TPtr &ev,
            std::unique_ptr<TEvGetLogoBlobIndexStatResponse> result);

    IActor *CreateMonStreamActor(THullDsSnap&& fullSnap, TEvBlobStorage::TEvMonStreamQuery::TPtr& ev);

} // NKikimr
