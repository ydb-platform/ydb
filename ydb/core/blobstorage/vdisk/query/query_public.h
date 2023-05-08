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
    template<typename TProtoEvVGet>
    bool CheckVGetQuery(const TProtoEvVGet &record) {
        bool hasRange = record.HasRangeQuery();
        bool hasExtreme = (record.ExtremeQueriesSize() > 0);

        // only one field must be non empty
        if (int(hasRange) + int(hasExtreme) != 1) {
            std::cout << "[CheckVGetQuery] int(hasRange) + int(hasExtreme) != 1\n";
            return false;
        }

        if (hasRange) {
            // check range query
            if (record.ExtremeQueriesSize() > 0) {
                std::cout << "[CheckVGetQuery] hasRange && record.ExtremeQueriesSize() > 0 \n";
                return false; // can't have both range and extreme
            }

            const auto &query = record.GetRangeQuery();
            if (!query.HasFrom() || !query.HasTo()) {
                std::cout << "[CheckVGetQuery] !query.HasFrom() || !query.HasTo() \n";
                return false;
            }

            if (query.HasMaxResults() && query.GetMaxResults() == 0) {
                std::cout << "[CheckVGetQuery] hasRange && query.HasMaxResults() && query.GetMaxResults() == 0\n";
                return false;
            }

            return true;
        }

        if (hasExtreme) {
            // check extreme queries
            if (record.ExtremeQueriesSize() == 0)
                return false; // we need to have one

            return true;
        }

        return false;
    }

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
