#include "localrecovery_readbulksst.h"
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hullload.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TOneDbBulkSstLoader
    ////////////////////////////////////////////////////////////////////////////
    template <class TLevelSegment>
    class TOneDbBulkSstLoader : public TActorBootstrapped<TOneDbBulkSstLoader<TLevelSegment>> {
        using TThis = TOneDbBulkSstLoader<TLevelSegment>;
        using TAddition = TAddBulkSstEssence::TSstAndRecsNum<TLevelSegment>;
        using THullSegLoaded = ::NKikimr::THullSegLoaded<TLevelSegment>;
        using TLevelSegmentLoader = ::NKikimr::TLevelSegmentLoader<typename TLevelSegment::TKeyType,
                    typename TLevelSegment::TMemRecType>;

        const TVDiskContextPtr VCtx;
        const TPDiskCtxPtr PDiskCtx;
        TActorId Recipient;
        const ui64 RecoveryLogRecLsn;
        const TString Origin;
        // Index of sst being loaded
        ui32 Index = 0;
        // Final loaded SSTables
        TVector<TAddition> Addition;
        // Active Actors to kill on PoisonPill
        TActiveActors ActiveActors;

        friend class TActorBootstrapped<TThis>;

        void Process(const TActorContext &ctx) {
            if (Index == Addition.size()) {
                Finish(ctx);
            } else {
                auto actor = std::make_unique<TLevelSegmentLoader>(VCtx, PDiskCtx, Addition[Index++].Sst.Get(),
                        ctx.SelfID, Origin);
                auto aid = ctx.Register(actor.release());
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            }
        }

        void Bootstrap(const TActorContext &ctx) {
            TThis::Become(&TThis::StateFunc);
            Process(ctx);
        }

        void Finish(const TActorContext &ctx) {
            Y_ABORT_UNLESS(Index == Addition.size());
            auto msg = std::make_unique<TEvBulkSstEssenceLoaded>(RecoveryLogRecLsn);
            msg->Essence.Replace(std::move(Addition));
            ctx.Send(Recipient, msg.release());
            TThis::Die(ctx);
        }

        void Handle(typename THullSegLoaded::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            Process(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            TThis::Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HTemplFunc(THullSegLoaded, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_DB_LOCAL_RECOVERY;
        }

        TOneDbBulkSstLoader(
                const TVDiskContextPtr &vctx,
                const TPDiskCtxPtr &pdiskCtx,
                const google::protobuf::RepeatedPtrField<
                    NKikimrVDiskData::TAddBulkSstRecoveryLogRec_TSstAndRecsNum> &proto,
                const TActorId &recipient,
                const ui64 recoveryLogRecLsn,
                const TString &origin)
            : TActorBootstrapped<TThis>()
            , VCtx(vctx)
            , PDiskCtx(pdiskCtx)
            , Recipient(recipient)
            , RecoveryLogRecLsn(recoveryLogRecLsn)
            , Origin(origin)
        {
            for (const auto &x : proto) {
                auto sst = MakeIntrusive<TLevelSegment>(VCtx, x.GetSst());
                Y_ABORT_UNLESS(!sst->GetEntryPoint().Empty());
                const ui32 recsNum = x.GetRecsNum();
                Addition.emplace_back(std::move(sst), recsNum);
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TBulkSstLoader
    // Load SSTables from the corresponding recovery log record into memory
    // before applying them to THull
    ////////////////////////////////////////////////////////////////////////////
    class TBulkSstLoader : public TActorBootstrapped<TBulkSstLoader> {
        const TVDiskContextPtr VCtx;
        const TPDiskCtxPtr PDiskCtx;
        // Recovery Log Record
        NKikimrVDiskData::TAddBulkSstRecoveryLogRec Proto;
        // Reply to Recipient
        TActorId Recipient;
        // We handle recovery log record with this lsn
        const ui64 RecoveryLogRecLsn;
        // Number of databases/actors run
        ui32 RunActors = 0;
        // Result
        TAddBulkSstEssence Essence;
        // Active Actors to kill on PoisonPill
        TActiveActors ActiveActors;
        // Load or not specific database Sst
        const bool LoadLogoBlobs;
        const bool LoadBlocks;
        const bool LoadBarriers;

        friend class TActorBootstrapped<TBulkSstLoader>;

        void Bootstrap(const TActorContext &ctx) {
            // Mark PDisk operations with this string
            const TString origin = "BulkSstLoader";
            if (Proto.LogoBlobsAdditionsSize() && LoadLogoBlobs) {
                using TLoader = TOneDbBulkSstLoader<TLogoBlobsSst>;
                auto actor = std::make_unique<TLoader>(VCtx, PDiskCtx, *Proto.MutableLogoBlobsAdditions(),
                        ctx.SelfID, RecoveryLogRecLsn, origin);
                auto aid = ctx.Register(actor.release());
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                ++RunActors;
            }
            if (Proto.BlocksAdditionsSize() && LoadBlocks) {
                using TLoader = TOneDbBulkSstLoader<TBlocksSst>;
                auto actor = std::make_unique<TLoader>(VCtx, PDiskCtx, *Proto.MutableBlocksAdditions(),
                        ctx.SelfID, RecoveryLogRecLsn, origin);
                auto aid = ctx.Register(actor.release());
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                ++RunActors;
            }
            if (Proto.BarriersAdditionsSize() && LoadBarriers) {
                using TLoader = TOneDbBulkSstLoader<TBarriersSst>;
                auto actor = std::make_unique<TLoader>(VCtx, PDiskCtx, *Proto.MutableBarriersAdditions(),
                        ctx.SelfID, RecoveryLogRecLsn, origin);
                auto aid = ctx.Register(actor.release());
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                ++RunActors;
            }
            Y_ABORT_UNLESS(RunActors);
            TThis::Become(&TThis::StateFunc);
        }

        void Handle(TEvBulkSstEssenceLoaded::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            Y_ABORT_UNLESS(RecoveryLogRecLsn == ev->Get()->RecoveryLogRecLsn);
            Essence.DestructiveMerge(std::move(ev->Get()->Essence));
            --RunActors;
            if (RunActors == 0) {
                Finish(ctx);
            }
        }

        void Finish(const TActorContext &ctx) {
            auto msg = std::make_unique<TEvBulkSstEssenceLoaded>(std::move(Essence), RecoveryLogRecLsn);
            ctx.Send(Recipient, msg.release());
            TThis::Die(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            TThis::Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvBulkSstEssenceLoaded, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_DB_LOCAL_RECOVERY;
        }

        TBulkSstLoader(
                const TVDiskContextPtr &vctx,
                const TPDiskCtxPtr &pdiskCtx,
                const NKikimrVDiskData::TAddBulkSstRecoveryLogRec &proto,
                const TActorId &recipient,
                const ui64 recoveryLogRecLsn,
                bool loadLogoBlobs,
                bool loadBlocks,
                bool loadBarriers)
            : TActorBootstrapped<TThis>()
            , VCtx(vctx)
            , PDiskCtx(pdiskCtx)
            , Proto(proto)
            , Recipient(recipient)
            , RecoveryLogRecLsn(recoveryLogRecLsn)
            , LoadLogoBlobs(loadLogoBlobs)
            , LoadBlocks(loadBlocks)
            , LoadBarriers(loadBarriers)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // CreateBulkSstLoaderActor
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateBulkSstLoaderActor(
            const TVDiskContextPtr &vctx,
            const TPDiskCtxPtr &pdiskCtx,
            const NKikimrVDiskData::TAddBulkSstRecoveryLogRec &proto,
            const TActorId &recipient,
            const ui64 recoveryLogRecLsn,
            bool loadLogoBlobs,
            bool loadBlocks,
            bool loadBarriers) {
        return new TBulkSstLoader(vctx, pdiskCtx, proto, recipient,
                recoveryLogRecLsn, loadLogoBlobs, loadBlocks, loadBarriers);
    }
} // NKikimr
