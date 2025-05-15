#include "query_public.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>

using namespace NKikimrServices;

namespace NKikimr {

    // FIXME: add a limitation on data size, final message could be too large

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexBarrierQuery
    ////////////////////////////////////////////////////////////////////////////
    class TLevelIndexBarrierQuery : public TActorBootstrapped<TLevelIndexBarrierQuery> {
        typedef ::NKikimr::TIndexRecordMerger<TKeyBarrier, TMemRecBarrier> TIndexRecordMerger;

        TIntrusivePtr<THullCtx> HullCtx;
        const TActorId ParentId;
        TBarriersSnapshot BarriersSnap;
        TEvBlobStorage::TEvVGetBarrier::TPtr Ev;
        NKikimrBlobStorage::TEvVGetBarrier &Record;
        std::unique_ptr<TEvBlobStorage::TEvVGetBarrierResult> Result;

        friend class TActorBootstrapped<TLevelIndexBarrierQuery>;

        void Bootstrap(const TActorContext &ctx) {
            ExecuteQuery();
            Finish(ctx);
        }

        void Finish(const TActorContext &ctx) {
            // send response
            LOG_DEBUG(ctx, BS_VDISK_GC,
                    VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                            "TEvVGetBarrierResult: %s", Result->ToString().data()));
            SendVDiskResponse(ctx, Ev->Sender, Result.release(), Ev->Cookie, HullCtx->VCtx, {});
            ctx.Send(ParentId, new TEvents::TEvActorDied);
            Die(ctx);
        }

        void ExecuteQuery() {
            const TKeyBarrier from = TKeyBarrier(Record.GetFrom());
            const TKeyBarrier to = TKeyBarrier(Record.GetTo());
            ui32 counter = Record.HasMaxResults() ? Record.GetMaxResults() : Max<ui32>();
            const bool showInternals = Record.GetShowInternals();

            TBarriersSnapshot::TIndexForwardIterator it(HullCtx, &BarriersSnap);
            it.Seek(from);
            while (counter > 0 && it.Valid() && it.GetCurKey() <= to) {
                Result->AddResult(it.GetCurKey(), it.GetMemRec(), showInternals);
                it.Next();
                counter--;
            }
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_LEVEL_INDEX_BARRIER;
        }

        TLevelIndexBarrierQuery(
                TIntrusivePtr<THullCtx> &hullCtx,
                const TActorId &parentId,
                TBarriersSnapshot &&barriersSnap,
                TEvBlobStorage::TEvVGetBarrier::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetBarrierResult> result)
            : TActorBootstrapped<TLevelIndexBarrierQuery>()
            , HullCtx(hullCtx)
            , ParentId(parentId)
            , BarriersSnap(std::move(barriersSnap))
            , Ev(ev)
            , Record(Ev->Get()->Record)
            , Result(std::move(result))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // Create TLevelIndexBarrierQuery
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateLevelIndexBarrierQueryActor(
                TIntrusivePtr<THullCtx> &hullCtx,
                const TActorId &parentId,
                TBarriersSnapshot &&barriersSnap,
                TEvBlobStorage::TEvVGetBarrier::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetBarrierResult> result) {
        return new TLevelIndexBarrierQuery(hullCtx, parentId, std::move(barriersSnap), ev, std::move(result));
    }


    ////////////////////////////////////////////////////////////////////////////
    // Check Barrier Query
    ////////////////////////////////////////////////////////////////////////////
    bool CheckVGetBarrierQuery(const NKikimrBlobStorage::TEvVGetBarrier &record) {
        if (!record.HasFrom()) {
            return false;
        }
        if (!record.HasTo()) {
            return false;
        }

        TKeyBarrier from(record.GetFrom());
        TKeyBarrier to(record.GetTo());

        if (from <= to)
            return true;
        else {
            return false;
        }
    }

} // NKikimr
