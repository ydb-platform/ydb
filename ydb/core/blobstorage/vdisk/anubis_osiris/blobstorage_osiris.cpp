#include "blobstorage_osiris.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/protos/blobstorage.pb.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THullOsirisActor
    ////////////////////////////////////////////////////////////////////////////
    struct TLogoBlobFilterForOsiris {
        TLogoBlobFilterForOsiris(TIntrusivePtr<THullCtx> hullCtx, TBarriersSnapshot&& bsnap)
            : HullCtx(hullCtx)
            , BarriersSnap(std::move(bsnap))
        {}

        void BuildBarriersEssence() {
            BarriersEssence = BarriersSnap.CreateEssence(HullCtx);
        }

        bool Check(const TKeyLogoBlob &key, const TMemRecLogoBlob &memRec, bool allowKeepFlags) const {
            return BarriersEssence->Keep(key, memRec, {}, allowKeepFlags, true).KeepData;
        }

        TIntrusivePtr<THullCtx> HullCtx;
        TBarriersSnapshot BarriersSnap;
        TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> BarriersEssence;
    };


    ////////////////////////////////////////////////////////////////////////////
    // TSaviour
    ////////////////////////////////////////////////////////////////////////////
    template <class TFilter>
    class TSaviour {
    public:
        using TIndexForwardIterator = typename TLogoBlobsSnapshot::TIndexForwardIterator;

        TSaviour(const TIntrusivePtr<THullCtx> &hullCtx,
                 TLogoBlobsSnapshot &&snapshot,
                 const std::shared_ptr<TFilter> &filter)
            : HullCtx(hullCtx)
            , Snapshot(std::move(snapshot))
            , Filter(filter)
            , CurIt(HullCtx, &Snapshot)
        {
            CurIt.SeekToFirst();
        }

        void SeekToFirst() {
            CurIt.SeekToFirst();
            // position ourself to the item to resurrect
            ScanUntilItemToResurrect();
        }

        bool Valid() const {
            return CurIt.Valid();
        }

        void Next() {
            CurIt.Next();
            // position ourself to the item to resurrect
            ScanUntilItemToResurrect();
        }

        TLogoBlobID LogoBlobID() const {
            return CurKey;
        }

        NMatrix::TVectorType GetPartsToResurrect() const {
            return PartsToResurrect;
        }

    private:
        TIntrusivePtr<THullCtx> HullCtx;
        TLogoBlobsSnapshot Snapshot;
        const std::shared_ptr<TFilter> Filter;

        // As an iterator we position at some element, below are data extracted from this element
        TIndexForwardIterator CurIt; // current iterator we have
        TLogoBlobID CurKey; // current key we have
        TIngress CurIngress; // merged ingress we have
        NMatrix::TVectorType PartsToResurrect; // parts to resurrect we have

        bool ResurrectCur() {
            auto &self = HullCtx->VCtx->ShortSelfVDisk; // VDiskId we have
            const auto& topology = *HullCtx->VCtx->Top; // topology we have
            Y_ABORT_UNLESS(topology.BelongsToSubgroup(self, CurKey.Hash())); // check that blob belongs to subgroup

            if (!Filter->Check(CurKey, CurIt.GetMemRec(), HullCtx->AllowKeepFlags)) {
                // filter check returned false
                return false;
            }

            const TSubgroupPartLayout layout = TSubgroupPartLayout::CreateFromIngress(CurIngress, topology.GType);
            const ui32 idxInSubgroup = topology.GetIdxInSubgroup(self, CurKey.Hash());
            const ui32 partsMask = topology.GetQuorumChecker().GetPartsToResurrect(layout, idxInSubgroup);
            if (!partsMask) {
                return false;
            }

            PartsToResurrect = NMatrix::TVectorType(0, topology.GType.TotalPartCount());
            for (ui32 i = 0; i < PartsToResurrect.GetSize(); ++i) {
                if (partsMask & (1 << i)) {
                    PartsToResurrect.Set(i);
                }
            }

            return true;
        }

        void ScanUntilItemToResurrect() {
            while (CurIt.Valid()) {
                // save values at current position
                CurKey = CurIt.GetCurKey().LogoBlobID();
                CurIngress = CurIt.GetMemRec().GetIngress();

                if (ResurrectCur()) {
                    break;
                }

                CurIt.Next();
            }
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // THullOsirisActor
    ////////////////////////////////////////////////////////////////////////////
    class THullOsirisActor : public TActorBootstrapped<THullOsirisActor> {

        TIntrusivePtr<THullCtx> HullCtx;
        const ui64 ConfirmedLsn;
        const TActorId NotifyId;
        const TActorId ParentId;
        const TActorId SkeletonId;
        std::shared_ptr<TLogoBlobFilterForOsiris> LogoBlobFilter;
        TSaviour<TLogoBlobFilterForOsiris> Saviour;
        ui64 InFly = 0;
        ui64 BlobsResurrected = 0;
        ui64 PartsResurrected = 0;
        const ui64 MaxInFly;


        friend class TActorBootstrapped<THullOsirisActor>;

        void Bootstrap(const TActorContext &ctx) {
            LOG_INFO(ctx, BS_SYNCER,
                VDISKP(HullCtx->VCtx->VDiskLogPrefix, "THullOsirisActor: START"));
            Become(&TThis::StateFunc);
            // prepare filter
            LogoBlobFilter->BuildBarriersEssence();
            // position iterator to the beginning
            Saviour.SeekToFirst();
            // scan Hull database and send messages up to MaxInFly
            ScanAndSend(ctx);
            // check if we need to finish
            FinishIfRequired(ctx);
        }

        void ScanAndSend(const TActorContext& ctx) {
            // send up to MaxInFly + epsilon
            while (Saviour.Valid() && InFly < MaxInFly) {
                // find parts to resurrect
                TLogoBlobID lb = Saviour.LogoBlobID();
                NMatrix::TVectorType v = Saviour.GetPartsToResurrect();

                // for every part send a message to skeleton, we can send more than
                // MaxInFly if we need to resurrect several parts
                for (ui8 i = v.FirstPosition(); i != v.GetSize(); i = v.NextPosition(i)) {
                    auto partId = i + 1;
                    TLogoBlobID id(lb, partId);
                    LOG_ERROR(ctx, BS_SYNCER,
                            VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "THullOsirisActor: RESURRECT: id# %s", id.ToString().data()));
                    ctx.Send(SkeletonId, new TEvAnubisOsirisPut(id));
                    ++InFly;
                    ++PartsResurrected;
                }
                ++BlobsResurrected;
                Saviour.Next();
            }
        }

        void Handle(TEvAnubisOsirisPutResult::TPtr& ev, const TActorContext& ctx) {
            Y_ABORT_UNLESS(ev->Get()->Status == NKikimrProto::OK, "Status# %d", ev->Get()->Status);
            --InFly;
            // scan and send messages up to MaxInFly
            ScanAndSend(ctx);
            // check if we need to finish
            FinishIfRequired(ctx);
        }

        void FinishIfRequired(const TActorContext& ctx) {
            if (InFly == 0) {
                LOG_ERROR(ctx, BS_SYNCER,
                         VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                            "THullOsirisActor: FINISH: BlobsResurrected# %" PRIu64 " PartsResurrected# %" PRIu64,
                            BlobsResurrected, PartsResurrected));
                ctx.Send(NotifyId, new TEvOsirisDone(ConfirmedLsn));
                ctx.Send(ParentId, new TEvents::TEvActorDied);
                Die(ctx);
            }
        }

        void HandlePoison(const TActorContext &ctx) {
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvAnubisOsirisPutResult, Handle)
            CFunc(TEvents::TSystem::PoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_OSIRIS;
        }

        THullOsirisActor(
                const TActorId &notifyId,
                const TActorId &parentId,
                const TActorId &skeletonId,
                THullDsSnap &&fullSnap,
                ui64 confirmedLsn,
                ui64 anubisOsirisMaxInFly)
            : TActorBootstrapped<THullOsirisActor>()
            , HullCtx(fullSnap.HullCtx)
            , ConfirmedLsn(confirmedLsn)
            , NotifyId(notifyId)
            , ParentId(parentId)
            , SkeletonId(skeletonId)
            , LogoBlobFilter(std::make_shared<TLogoBlobFilterForOsiris>(HullCtx, std::move(fullSnap.BarriersSnap)))
            , Saviour(HullCtx, std::move(fullSnap.LogoBlobsSnap), LogoBlobFilter)
            , MaxInFly(anubisOsirisMaxInFly)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////////
    // CreateHullOsiris
    ////////////////////////////////////////////////////////////////////////////////
    IActor *CreateHullOsiris(
            const TActorId &notifyId,
            const TActorId &parentId,
            const TActorId &skeletonId,
            THullDsSnap &&fullSnap,
            ui64 confirmedLsn,
            ui64 maxInFly) {
        return new THullOsirisActor(notifyId, parentId, skeletonId, std::move(fullSnap), confirmedLsn, maxInFly);
    }

} // NKikimr
