#include "query_public.h"
#include "query_dumpdb.h"
#include "query_statdb.h"
#include "query_stathuge.h"
#include "query_stattablet.h"
#include "query_stream.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>

using namespace NKikimrServices;

namespace NKikimr {

    // Extreme Query Declaration
    IActor *CreateLevelIndexExtremeQueryActor(
                std::shared_ptr<TQueryCtx> &queryCtx,
                const TActorId &parentId,
                TLogoBlobsSnapshot &&logoBlobsSnapshot,
                TBarriersSnapshot &&barrierSnapshot,
                TEvBlobStorage::TEvVGet::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetResult> result,
                TActorId replSchedulerId);

    // Range Query Declaration
    IActor *CreateLevelIndexRangeQueryActor(
                std::shared_ptr<TQueryCtx> &queryCtx,
                const TActorId &parentId,
                TLogoBlobsSnapshot &&logoBlobsSnapshot,
                TBarriersSnapshot &&barrierSnapshot,
                TEvBlobStorage::TEvVGet::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetResult> result,
                TActorId replSchedulerId);


    // NOTES
    // We have the following scenarios for the TEvVGet message:
    // 1. We are reading log. We don't know exact keys, so we are making range queries.
    //    We can read index only or with data. Suppose we don't ask for exact parts.
    // 2. We are reading log, but we do know exact keys. One TEvVGet contains multiple
    //    queries with exact keys. Keys are not necessary consecutive, but are close to
    //    each other. In this case we need to glue ChunkRead requests to optimize disk IO.
    //    A good way to implement it is to analyze index and glue requests to the same
    //    chunkIdx.
    // 3. We are reading data from large logoblobs. We know exact keys, we actively
    //    use shift/size.
    //
    // TODO: for a group of ExtremeQueries look at neighbours instead of LowerBound

    //////////////////////////////////////////////////////////////////////////////////////
    // CreateLevelIndexQueryActor
    //////////////////////////////////////////////////////////////////////////////////////
    static void ValidateReadQuery(
            TReadQueryKeepChecker &&keepChecker,
            const TIntrusivePtr<THullCtx> &hullCtx,
            const TActorContext& ctx,
            const ::google::protobuf::RepeatedPtrField<::NKikimrBlobStorage::TExtremeQuery> &extremeQueries,
            const TLogoBlobsSnapshot *snapshot,
            bool suppressBarrierCheck)
    {
        TLogoBlobsSnapshot::TIndexForwardIterator it(hullCtx, snapshot);
        for (const auto& item : extremeQueries) {
            Y_ABORT_UNLESS(item.HasId());
            const TLogoBlobID& id = LogoBlobIDFromLogoBlobID(item.GetId());
            const TLogoBlobID& full = id.FullID();

            it.Seek(full);
            if (it.Valid() && it.GetCurKey() == full) {
                const TIngress& ingress = it.GetMemRec().GetIngress();
                const bool keep = ingress.KeepUnconditionally(TIngress::IngressMode(hullCtx->VCtx->Top->GType));
                TString explanation;
                if (!suppressBarrierCheck && !keepChecker(full, keep, &explanation)) {
                    LOG_INFO(ctx, NKikimrServices::BS_HULLRECS,
                            VDISKP(hullCtx->VCtx->VDiskLogPrefix,
                                "Db# LogoBlobs getting blob beyond the barrier id# %s ingress# %s barrier# %s",
                                id.ToString().data(), ingress.ToString(hullCtx->VCtx->Top.get(),
                                hullCtx->VCtx->ShortSelfVDisk, id).data(), explanation.data()));
                }
            }
        }
    }

    IActor *CreateLevelIndexQueryActor(
                    std::shared_ptr<TQueryCtx> &queryCtx,
                    TReadQueryKeepChecker &&keepChecker,
                    const TActorContext &ctx,
                    THullDsSnap &&fullSnap,
                    const TActorId &parentId,
                    TEvBlobStorage::TEvVGet::TPtr &ev,
                    std::unique_ptr<TEvBlobStorage::TEvVGetResult> result,
                    TActorId replSchedulerId) {

        const auto& record = ev->Get()->Record;
        if (queryCtx->HullCtx->BarrierValidation) {
            ValidateReadQuery(std::move(keepChecker), fullSnap.HullCtx, ctx, record.GetExtremeQueries(),
                &fullSnap.LogoBlobsSnap, record.GetSuppressBarrierCheck());
        }

        if (record.HasRangeQuery()) {
            // we pass barriers snap to the query actor when we are doing range read -- we need barriers
            // to ensure that no blobs that are subject to GC are reported to the request origin actor
            return CreateLevelIndexRangeQueryActor(queryCtx, parentId,
                    std::move(fullSnap.LogoBlobsSnap), std::move(fullSnap.BarriersSnap), ev, std::move(result), replSchedulerId);
        } else if (record.ExtremeQueriesSize() > 0) {
            return CreateLevelIndexExtremeQueryActor(queryCtx, parentId,
                    std::move(fullSnap.LogoBlobsSnap), std::move(fullSnap.BarriersSnap), ev, std::move(result), replSchedulerId);
        } else {
            Y_ABORT("Impossible case");
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////
    // Check query correctness
    //////////////////////////////////////////////////////////////////////////////////////
    bool CheckVGetQuery(const NKikimrBlobStorage::TEvVGet &record) {
        bool hasRange = record.HasRangeQuery();
        bool hasExtreme = (record.ExtremeQueriesSize() > 0);

        // only one field must be non empty
        if (int(hasRange) + int(hasExtreme) != 1)
            return false;

        if (hasRange) {
            // check range query
            if (record.ExtremeQueriesSize() > 0)
                return false; // can't have both range and extreme

            const NKikimrBlobStorage::TRangeQuery &query = record.GetRangeQuery();
            if (!query.HasFrom() || !query.HasTo())
                return false;

            if (query.HasMaxResults() && query.GetMaxResults() == 0)
                return false;

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
    // CreateDbStatActor HELPERS
    ////////////////////////////////////////////////////////////////////////////
    static inline void DbStatError(
            const TVDiskContextPtr &vctx,
            const TActorContext &ctx,
            TEvBlobStorage::TEvVDbStat::TPtr &ev,
            std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result)
    {
        result->SetError();
        LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_OTHER,
                VDISKP(vctx->VDiskLogPrefix,
                    "TEvVDbStatResult: %s", result->ToString().data()));
        SendVDiskResponse(ctx, ev->Sender, result.release(), ev->Cookie, vctx, {});
    }

    template <class TKey, class TMemRec>
    static inline IActor *RunDbStatAction(
            const TIntrusivePtr<THullCtx> &hullCtx,
            const TActorContext &ctx,
            TLevelIndexSnapshot<TKey, TMemRec> &&levelSnap,
            const TActorId &parentId,
            TEvBlobStorage::TEvVDbStat::TPtr &ev,
            std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result)
    {
        const NKikimrBlobStorage::TEvVDbStat &record = ev->Get()->Record;
        switch (record.GetAction()) {
            case NKikimrBlobStorage::DumpDb: {
                using TDumpActor = TLevelIndexDumpActor<TKey, TMemRec>;
                return new TDumpActor(hullCtx, parentId, std::move(levelSnap), ev, std::move(result));
            }
            case NKikimrBlobStorage::StatDb: {
                using TStatActor = TLevelIndexStatActor<TKey, TMemRec>;
                return new TStatActor(hullCtx, parentId, std::move(levelSnap), ev, std::move(result));
            }
            default: {
                DbStatError(hullCtx->VCtx, ctx, ev, std::move(result));
                return nullptr;
            }
        }
    }

    template <class TKey, class TMemRec>
    static inline IActor *RunDbStatAction(
            const TIntrusivePtr<THullCtx> &hullCtx,
            const TActorContext &,
            TLevelIndexSnapshot<TKey, TMemRec> &&levelSnap,
            const TActorId &parentId,
            TEvGetLogoBlobIndexStatRequest::TPtr &ev,
            std::unique_ptr<TEvGetLogoBlobIndexStatResponse> result)
    {
        using TStatActorEx = TLevelIndexStatActor<TKey, TMemRec,
                TEvGetLogoBlobIndexStatRequest, TEvGetLogoBlobIndexStatResponse>;
        return new TStatActorEx(hullCtx, parentId, std::move(levelSnap), ev, std::move(result));
    }

    ////////////////////////////////////////////////////////////////////////////
    // CreateDbStatActor
    // Handle a TEvVDbStat query on Hull database snapshot
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateDbStatActor(
            const TIntrusivePtr<THullCtx> &hullCtx,
            const std::shared_ptr<THugeBlobCtx> &hugeBlobCtx,
            const TActorContext &ctx,
            THullDsSnap &&fullSnap,
            const TActorId &parentId,
            TEvBlobStorage::TEvVDbStat::TPtr &ev,
            std::unique_ptr<TEvBlobStorage::TEvVDbStatResult> result) {
        const NKikimrBlobStorage::TEvVDbStat &record = ev->Get()->Record;
        switch (record.GetType()) {
            case NKikimrBlobStorage::StatLogoBlobs:
                return RunDbStatAction(hullCtx, ctx, std::move(fullSnap.LogoBlobsSnap), parentId, ev, std::move(result));

            case NKikimrBlobStorage::StatBlocks:
                return RunDbStatAction(hullCtx, ctx, std::move(fullSnap.BlocksSnap), parentId, ev, std::move(result));

            case NKikimrBlobStorage::StatBarriers:
                return RunDbStatAction(hullCtx, ctx, std::move(fullSnap.BarriersSnap), parentId, ev, std::move(result));

            case NKikimrBlobStorage::StatTabletType:
                return CreateTabletStatActor(hullCtx, parentId, std::move(fullSnap), ev, std::move(result));

            case NKikimrBlobStorage::StatHugeType:
                return CreateHugeStatActor(hullCtx, hugeBlobCtx, parentId, std::move(fullSnap), ev, std::move(result));

            default:
                DbStatError(hullCtx->VCtx, ctx, ev, std::move(result));
                return nullptr;
        }
    }

    IActor *CreateDbStatActor(
            const TIntrusivePtr<THullCtx> &hullCtx,
            const std::shared_ptr<THugeBlobCtx> &,
            const TActorContext &ctx,
            THullDsSnap &&fullSnap,
            const TActorId &parentId,
            TEvGetLogoBlobIndexStatRequest::TPtr &ev,
            std::unique_ptr<TEvGetLogoBlobIndexStatResponse> result)
    {
        return RunDbStatAction(hullCtx, ctx, std::move(fullSnap.LogoBlobsSnap), parentId, ev, std::move(result));
    }

    IActor *CreateMonStreamActor(THullDsSnap&& fullSnap, TEvBlobStorage::TEvMonStreamQuery::TPtr& ev) {
        return new TLevelIndexStreamActor(std::move(fullSnap), ev);
    }

} // NKikimr
