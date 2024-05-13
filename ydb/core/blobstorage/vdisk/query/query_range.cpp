#include "query_base.h"

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexRangeQueryViaBatcherBase
    ////////////////////////////////////////////////////////////////////////////
    class TLevelIndexRangeQueryViaBatcherBase : public TLevelIndexQueryBase {
    protected:
        TLogoBlobsSnapshot::TForwardIterator ForwardIt;
        TLogoBlobsSnapshot::TBackwardIterator BackwardIt;
        // range query info
        TLogoBlobID First;
        TLogoBlobID Last;
        ui32 FirstPartId;
        ui32 LastPartId;
        ui64 CookieVal;
        ui64 *CookiePtr;
        bool DirectionForward;
        ui32 Counter;
        TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> BarriersEssence;

        void Prepare() {
            const NKikimrBlobStorage::TRangeQuery &query = Record.GetRangeQuery();
            First = LogoBlobIDFromLogoBlobID(query.GetFrom());
            FirstPartId = First.PartId();
            First = TLogoBlobID(First, 0);
            Last = LogoBlobIDFromLogoBlobID(query.GetTo());
            LastPartId = Last.PartId();
            Last = TLogoBlobID(Last, 0);
            CookieVal = query.GetCookie();
            CookiePtr = query.HasCookie() ? &CookieVal : nullptr;
            Counter = query.HasMaxResults() ? query.GetMaxResults() : Max<ui32>();

            if (First <= Last) {
                DirectionForward = true;
                ForwardIt.Seek(First);
            } else {
                DirectionForward = false;
                DoSwap(First, Last);
                DoSwap(FirstPartId, LastPartId);
                BackwardIt.Seek(Last);
            }

            const ui64 firstTabletId = Min(First.TabletID(), Last.TabletID());
            const ui64 lastTabletId = Max(First.TabletID(), Last.TabletID());
            BarriersEssence = BarriersSnapshot.CreateEssence(QueryCtx->HullCtx, firstTabletId, lastTabletId, 0);
            BarriersSnapshot.Destroy();
        }

        TLevelIndexRangeQueryViaBatcherBase(
                std::shared_ptr<TQueryCtx> &queryCtx,
                const TActorId &parentId,
                TLogoBlobsSnapshot &&logoBlobsSnapshot,
                TBarriersSnapshot &&barriersSnapshot,
                TEvBlobStorage::TEvVGet::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetResult> result,
                TActorId replSchedulerId,
                const char* name)
            : TLevelIndexQueryBase(queryCtx, parentId, std::move(logoBlobsSnapshot), std::move(barriersSnapshot),
                    ev, std::move(result), replSchedulerId, name)
            , ForwardIt(QueryCtx->HullCtx, &LogoBlobsSnapshot)
            , BackwardIt(QueryCtx->HullCtx, &LogoBlobsSnapshot)
            , First()
            , Last()
            , FirstPartId(0)
            , LastPartId(0)
            , CookieVal(0)
            , CookiePtr(nullptr)
            , DirectionForward(false)
            , Counter(0)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexRangeQueryViaBatcherIndexOnly
    ////////////////////////////////////////////////////////////////////////////
    class TLevelIndexRangeQueryViaBatcherIndexOnly : public TLevelIndexRangeQueryViaBatcherBase,
            public TActorBootstrapped<TLevelIndexRangeQueryViaBatcherIndexOnly> {

        using TIndexRecordMerger = ::NKikimr::TIndexRecordMerger<TKeyLogoBlob, TMemRecLogoBlob>;
        friend class TLevelIndexQueryBase;
        friend class TActorBootstrapped<TLevelIndexRangeQueryViaBatcherIndexOnly>;
        friend class TLevelIndexRangeQueryViaBatcherBase;

        TIndexRecordMerger Merger;

        void Bootstrap(const TActorContext &ctx) {
            Prepare();
            Y_DEBUG_ABORT_UNLESS(!Merger.HaveToMergeData());
            MainCycleIndexOnly(ctx);
        }

        void MainCycleIndexOnly(const TActorContext &ctx) {
            if (DirectionForward) {
                // forward direction
                while (Counter > 0 && !ResultSize.IsOverflow() && ForwardIt.Valid() && ForwardIt.GetCurKey() <= Last) {
                    ForwardIt.PutToMerger(&Merger);
                    Merger.Finish();
                    AddIndexOnly(ForwardIt.GetCurKey().LogoBlobID(), Merger);
                    Merger.Clear();
                    ForwardIt.Next();
                }
            } else {
                // backward direction
                while (Counter > 0 && !ResultSize.IsOverflow() && BackwardIt.Valid() && BackwardIt.GetCurKey() >= First) {
                    BackwardIt.PutToMerger(&Merger);
                    Merger.Finish();
                    AddIndexOnly(BackwardIt.GetCurKey().LogoBlobID(), Merger);
                    Merger.Clear();
                    BackwardIt.Prev();
                }
            }
            if (Counter == 0 || ResultSize.IsOverflow()) {
                Result->MarkRangeOverflow();
            }

            // send response and die
            SendResponseAndDie(ctx, this);
        }

        template<typename TMerger>
        void AddIndexOnly(const TLogoBlobID &logoBlobId, const TMerger &merger) {
            const auto &status = BarriersEssence->Keep(logoBlobId, merger.GetMemRec(), {},
                QueryCtx->HullCtx->AllowKeepFlags, true /*allowGarbageCollection*/);
            if (status.KeepData) {
                const TIngress &ingress = merger.GetMemRec().GetIngress();
                ui64 ingr = ingress.Raw();
                ui64 *pingr = (ShowInternals ? &ingr : nullptr);
                Y_ABORT_UNLESS(logoBlobId.PartId() == 0); // Index-only response must contain a single record for the blob
                const NMatrix::TVectorType local = ingress.LocalParts(QueryCtx->HullCtx->VCtx->Top->GType);

                const int mode = ingress.GetCollectMode(TIngress::IngressMode(QueryCtx->HullCtx->VCtx->Top->GType));
                const bool keep = (mode & CollectModeKeep) && !(mode & CollectModeDoNotKeep);
                const bool doNotKeep = mode & CollectModeDoNotKeep;

                Result->AddResult(NKikimrProto::OK, logoBlobId, CookiePtr, pingr, &local, keep, doNotKeep);
                --Counter;
                ResultSize.AddLogoBlobIndex();
            }
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULLQUERY_RANGE_INDEX_ONLY;
        }

        TLevelIndexRangeQueryViaBatcherIndexOnly(
                std::shared_ptr<TQueryCtx> &queryCtx,
                const TActorId &parentId,
                TLogoBlobsSnapshot &&logoBlobsSnapshot,
                TBarriersSnapshot &&barriersSnapshot,
                TEvBlobStorage::TEvVGet::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetResult> result,
                TActorId replSchedulerId)
            : TLevelIndexRangeQueryViaBatcherBase(queryCtx, parentId,
                    std::move(logoBlobsSnapshot), std::move(barriersSnapshot), ev, std::move(result), replSchedulerId,
                    "VDisk.LevelIndexRangeQueryViaBatcherBase")
            , TActorBootstrapped<TLevelIndexRangeQueryViaBatcherIndexOnly>()
            , Merger(QueryCtx->HullCtx->VCtx->Top->GType)
        {
            Y_DEBUG_ABORT_UNLESS(Record.GetIndexOnly());
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateLevelIndexRangeQueryActor
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateLevelIndexRangeQueryActor(
                    std::shared_ptr<TQueryCtx> &queryCtx,
                    const TActorId &parentId,
                    TLogoBlobsSnapshot &&logoBlobsSnapshot,
                    TBarriersSnapshot &&barriersSnapshot,
                    TEvBlobStorage::TEvVGet::TPtr &ev,
                    std::unique_ptr<TEvBlobStorage::TEvVGetResult> result,
                    TActorId replSchedulerId) {
        bool indexOnly = ev->Get()->Record.GetIndexOnly();
        if (indexOnly) {
            return new TLevelIndexRangeQueryViaBatcherIndexOnly(queryCtx, parentId,
                    std::move(logoBlobsSnapshot), std::move(barriersSnapshot), ev, std::move(result), replSchedulerId);
        } else {
            return nullptr;
        }
    }


} // NKikimr
