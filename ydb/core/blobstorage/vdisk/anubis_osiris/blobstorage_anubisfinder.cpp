#include "blobstorage_anubisfinder.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BS_SYNCER

namespace NKikimr {
    ////////////////////////////////////////////////////////////////////////////
    // TAnubisCandidatesFinder
    ////////////////////////////////////////////////////////////////////////////
    class TAnubisCandidatesFinder {
    public:
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

            TLogoBlobsSnapshot::TIndexForwardIterator it(HullCtx, &LogoBlobsSnap);
            for (it.Seek(Pos); it.Valid() && !candidates.Enough(); it.Next()) {
                const auto key = it.GetCurKey();
                const auto keep = brs->Keep(key, it.GetMemRec(), {}, HullCtx->AllowKeepFlags, true);
                if (keep.KeepData && !keep.KeepByBarrier) {
                    candidates.AddCandidate(key.LogoBlobID());
                }
            }
            candidates.SetPos(it.Valid() ? it.GetCurKey().LogoBlobID() : Max<TLogoBlobID>());
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

            YDB_LOG_INFO_CTX(ctx, VDISKP(HullCtx->VCtx->VDiskLogPrefix, "TAnubisCandidatesFinderActor actor: %s", res.ToString().data()));

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
