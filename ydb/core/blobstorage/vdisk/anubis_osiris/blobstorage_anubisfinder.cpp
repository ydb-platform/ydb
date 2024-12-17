#include "blobstorage_anubisfinder.h"
// FIXME: snapshot is enough
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullmergeits.h>

namespace NKikimr {
    ////////////////////////////////////////////////////////////////////////////
    // TLevelSliceSnapshotForwardIteratorWithStopper
    // It's a TLevelSliceSnapshot::TForwardIterator with ability to stop
    // before the end of the database via provided TStopper. We use it split
    // a whole job to a number of quantums.
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec, class TStopper>
    class TLevelSliceSnapshotForwardIteratorWithStopper
        : public ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>::TForwardIterator
    {
    public:
        using TLevelIndexSnapshot = typename ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
        using TBase = typename TLevelIndexSnapshot::TForwardIterator;

        TLevelSliceSnapshotForwardIteratorWithStopper(TStopper *stopper,
                                                      const THullCtxPtr &settings,
                                                      const TLevelIndexSnapshot *snap)
            : TBase(settings, snap)
            , Stopper(stopper)
        {}

        bool Valid() const {
            return TBase::Valid() && !Stopper->Enough();
        }

        void Next() {
            TBase::Next();
            if (TBase::Valid()) {
                Stopper->SetPos(TBase::GetCurKey());
            }
        }

    private:
        TStopper *Stopper;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TAnubisCandidatesFinder
    ////////////////////////////////////////////////////////////////////////////
    class TAnubisCandidatesFinder {
    public:
        using TLogoBlobsIt = typename TLogoBlobsSnapshot::TForwardIterator;
        using TIndexRecordMerger = ::NKikimr::TIndexRecordMerger<TKeyLogoBlob, TMemRecLogoBlob>;


        TAnubisCandidatesFinder(const TIntrusivePtr<THullCtx> &hullCtx,
                                const TLogoBlobID &pos,
                                TLogoBlobsSnapshot &&logoBlobsSnap,
                                TBarriersSnapshot &&barriersSnap)
            : HullCtx(hullCtx)
            , Pos(pos)
            , LogoBlobsSnap(std::move(logoBlobsSnap))
            , BarriersSnap(std::move(barriersSnap))
        {}

        TAnubisCandidates FindCandidates() {
            TAnubisCandidates candidates;
            candidates.StartTime = TAppData::TimeProvider->Now();
            // build barriers essence
            TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> brs = BarriersSnap.CreateEssence(HullCtx);
            candidates.BarriersTime = TAppData::TimeProvider->Now();
            // free barriers snapshot
            BarriersSnap.Destroy();

            // the subset we processing
            TLogoBlobsIt subsIt(HullCtx, &LogoBlobsSnap);
            subsIt.Seek(Pos);
            // for the whole level index
            TLogoBlobsIt dbIt(HullCtx, &LogoBlobsSnap);


            //////////////// HANDLERS /////////////////////////////////////////
            auto newItem = [] (const TLogoBlobsIt &subsIt, const TIndexRecordMerger &subsMerger) {
                Y_UNUSED(subsIt);
                Y_UNUSED(subsMerger);
            };

            auto doMerge = [this, &brs, &candidates] (const TLogoBlobsIt &subsIt,
                                                      const TLogoBlobsIt &dbIt,
                                                      const TIndexRecordMerger &subsMerger,
                                                      const TIndexRecordMerger &dbMerger) {
                Y_UNUSED(subsIt);
                Y_UNUSED(subsMerger);
                // calculate keep status
                bool allowKeepFlags = HullCtx->AllowKeepFlags;
                NGc::TKeepStatus keep = brs->Keep(dbIt.GetCurKey(), dbMerger.GetMemRec(),
                    {subsMerger, dbMerger}, allowKeepFlags, true /*allowGarbageCollection*/);
                if (keep.KeepIndex && !keep.KeepByBarrier) {
                    // we keep this record because of keep flags
                    candidates.AddCandidate(dbIt.GetCurKey().LogoBlobID());
                }
            };

            auto crash = [this] (const TLogoBlobsIt &subsIt, const TLogoBlobsIt &dbIt) {
                TStringStream str;
                str << MergeIteratorWithWholeDbDefaultCrashReport(HullCtx->VCtx->VDiskLogPrefix,
                                                                  subsIt, dbIt);
                return str.Str();
            };

            // run process of finding candidates
            MergeIteratorWithWholeDb<TLogoBlobsIt, TLogoBlobsIt, TIndexRecordMerger>(HullCtx->VCtx->Top->GType,
                                                                                     subsIt,
                                                                                     dbIt,
                                                                                     newItem,
                                                                                     doMerge,
                                                                                     crash);
            candidates.FinishTime = TAppData::TimeProvider->Now();
            return candidates;
        }

    private:
        TIntrusivePtr<THullCtx> HullCtx;
        TLogoBlobID Pos;
        TLogoBlobsSnapshot LogoBlobsSnap;
        TBarriersSnapshot BarriersSnap;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TAnubisCandidatesFinderActor
    ////////////////////////////////////////////////////////////////////////////
    class TAnubisCandidatesFinderActor : public TActorBootstrapped<TAnubisCandidatesFinderActor> {
        TIntrusivePtr<THullCtx> HullCtx;
        TActiveActors ActiveActors;
        const TActorId ParentId;
        TLogoBlobID Pos; // start position
        TLogoBlobsSnapshot LogoBlobsSnap;
        TBarriersSnapshot BarriersSnap;

        friend class TActorBootstrapped<TAnubisCandidatesFinderActor>;

        void Bootstrap(const TActorContext &ctx) {
            TAnubisCandidatesFinder finder(HullCtx, Pos, std::move(LogoBlobsSnap), std::move(BarriersSnap));
            TAnubisCandidates res = finder.FindCandidates();

            LOG_INFO(ctx, NKikimrServices::BS_SYNCER,
                     VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                        "TAnubisCandidatesFinderActor actor: %s", res.ToString().data()));

            ctx.Send(ParentId, new TEvAnubisCandidates(std::move(res)));
            TThis::Die(ctx);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_ANUBIS;
        }

        TAnubisCandidatesFinderActor(const TIntrusivePtr<THullCtx> &hullCtx,
                                     const TActorId &parentId,
                                     const TLogoBlobID &pos,
                                     TLogoBlobsSnapshot &&logoBlobsSnap,
                                     TBarriersSnapshot &&barriersSnap)
            : TActorBootstrapped<TAnubisCandidatesFinderActor>()
            , HullCtx(hullCtx)
            , ParentId(parentId)
            , Pos(pos)
            , LogoBlobsSnap(std::move(logoBlobsSnap))
            , BarriersSnap(std::move(barriersSnap))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateAnubisCandidatesFinder
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateAnubisCandidatesFinder(
                const TIntrusivePtr<THullCtx> &hullCtx,
                const TActorId &parentId,
                const TLogoBlobID &pos,
                TLogoBlobsSnapshot &&logoBlobsSnap,
                TBarriersSnapshot &&barriersSnap) {
        return new TAnubisCandidatesFinderActor(hullCtx, parentId, pos, std::move(logoBlobsSnap),
                std::move(barriersSnap));
    }


} // NKikimr
