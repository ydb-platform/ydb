#include "blobstorage_syncer_committer.h"
#include "blobstorage_syncer_data.h"
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////
    // TEvSyncerCommit
    ////////////////////////////////////////////////////////////////////////
    TEvSyncerCommit::TEvSyncerCommit()
        : Modif(ENone)
    {}

    std::unique_ptr<TEvSyncerCommit> TEvSyncerCommit::Local(ELocalState state, TVDiskEternalGuid guid) {
        auto msg = std::make_unique<TEvSyncerCommit>();
        msg->Modif = ELocalGuid;
        msg->LocalGuidInfo.SetState(state);
        msg->LocalGuidInfo.SetGuid(guid);
        return msg;
    }

    std::unique_ptr<TEvSyncerCommit> TEvSyncerCommit::LocalFinal(TVDiskEternalGuid guid, ui64 dbBirthLsn) {
        auto msg = std::make_unique<TEvSyncerCommit>();
        msg->Modif = ELocalGuid;
        msg->LocalGuidInfo.SetState(TLocalVal::Final);
        msg->LocalGuidInfo.SetGuid(guid);
        msg->LocalGuidInfo.SetDbBirthLsn(dbBirthLsn);
        return msg;
    }

    std::unique_ptr<TEvSyncerCommit> TEvSyncerCommit::Remote(const TVDiskID &vdisk,
                                                     const NSyncer::TPeerSyncState &p) {
        auto msg = std::make_unique<TEvSyncerCommit>();
        msg->Modif = EVDiskEntry;
        msg->VDiskId = vdisk;
        p.Serialize(msg->VDiskEntry);
        return msg;
    }

    std::unique_ptr<TEvSyncerCommit> TEvSyncerCommit::Remote(const TVDiskID &vdisk,
                                                     ESyncState state,
                                                     TVDiskEternalGuid guid,
                                                     void *cookie) {
        auto msg = std::make_unique<TEvSyncerCommit>();
        msg->Modif = EVDiskEntry;
        msg->VDiskId = vdisk;
        msg->Cookie = cookie;
        auto *info = msg->VDiskEntry.MutableSyncGuidInfo();
        info->SetState(state);
        info->SetGuid(guid);
        return msg;
    }


    ////////////////////////////////////////////////////////////////////////////
    // TProtoState
    // We manage Syncer state in protobuf suitable for commit;
    // Updates are coming from TEvSyncerCommit message, that
    // can apply using protobuf MergeFrom facility
    ////////////////////////////////////////////////////////////////////////////
    class TProtoState {
    public:
        TProtoState(const std::shared_ptr<TBlobStorageGroupInfo::TTopology> &top,
                    TSyncerDataSerializer &&sds)
            : Top(top)
            , Sds(std::move(sds))
        {}

        void Apply(TEvSyncerCommit::TPtr &ev) {
            const auto *msg = ev->Get();
            switch (msg->Modif) {
                case TEvSyncerCommit::ENone:
                    break; // nothing to do
                case TEvSyncerCommit::ELocalGuid: {
                    Sds.Proto.MutableLocalGuidInfo()->MergeFrom(msg->LocalGuidInfo);
                    break;
                }
                case TEvSyncerCommit::EVDiskEntry: {
                    auto index = Top->GetOrderNumber(msg->VDiskId);
                    Sds.Proto.MutableEntries(index)->MergeFrom(msg->VDiskEntry);
                    break;
                }
                default: Y_ABORT("Unexpected case");
            }
        }

        TString Serialize() const {
            return Sds.Serialize();
        }

    private:
        std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
        TSyncerDataSerializer Sds;
    };


    ////////////////////////////////////////////////////////////////////////////
    // TSyncerCommitter
    // Works in the same mailbox with TSyncerScheduler
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerCommitter : public TActorBootstrapped<TSyncerCommitter> {
        struct TNotify {
            const TActorId ActorId;
            const void *Cookie = nullptr;
            TNotify(const TActorId aid, const void *cookie)
                : ActorId(aid)
                , Cookie(cookie)
            {}
        };
        using TNotifyVec = TVector<TNotify>;

        TIntrusivePtr<TSyncerContext> SyncerCtx;
        bool EnableSelfCommit;
        const TDuration AdvanceEntryPointTimeout;
        TProtoState State;
        // If InFly is not empty, we have a write to log in flight;
        // After Yard successful reply we notify this actors with
        // TEvSyncerCommitDone message
        TNotifyVec InFly;
        // A vector of TActorIDs that await to be committed
        TNotifyVec Delayed;
        TInstant LastCommitTime = {};
        ui64 CurEntryPointLsn = 0;
        bool SelfCommitInProgress = false;
        bool WakeupScheduled = false;

        friend class TActorBootstrapped<TSyncerCommitter>;

        void Bootstrap(const TActorContext &ctx) {
            if (EnableSelfCommit) {
                ScheduleWakeup(ctx);
            } else {
                LOG_WARN(ctx, NKikimrServices::BS_SYNCER,
                    VDISKP(SyncerCtx->VCtx->VDiskLogPrefix,
                        "SelfCommit in TSyncerCommitter is disabled"));
            }
            TThis::Become(&TThis::StateFunc);
        }

        void GenerateCommit(const TActorContext &ctx) {
            // commit
            NPDisk::TCommitRecord commitRec;
            commitRec.IsStartingPoint = true;
            TRcBuf data = TRcBuf(State.Serialize());
            size_t dataSize = data.size();
            TLsnSeg seg = SyncerCtx->LsnMngr->AllocLsnForLocalUse();
            auto msg = std::make_unique<NPDisk::TEvLog>(SyncerCtx->PDiskCtx->Dsk->Owner,
                SyncerCtx->PDiskCtx->Dsk->OwnerRound, TLogSignature::SignatureSyncerState,
                commitRec, data, seg, nullptr);
            SyncerCtx->MonGroup.SyncerLoggedBytes() += dataSize;
            ++SyncerCtx->MonGroup.SyncerLoggerRecords();
            ctx.Send(SyncerCtx->LoggerId, msg.release());
        }

        void Handle(TEvSyncerCommit::TPtr &ev, const TActorContext &ctx) {
            State.Apply(ev);
            const void *cookie = ev->Get()->Cookie;

            // generate commit / put to the queue
            if (InFly.empty()) {
                // no writes at this moment
                InFly.emplace_back(ev->Sender, cookie);
                GenerateCommit(ctx);
            } else {
                // commit write is going on
                Delayed.emplace_back(ev->Sender, cookie);
            }
        }

        void Handle(NPDisk::TEvLogResult::TPtr &ev, const TActorContext &ctx) {
            CHECK_PDISK_RESPONSE(SyncerCtx->VCtx, ev, ctx);
            Y_DEBUG_ABORT_UNLESS(!InFly.empty());

            // reply committed on all writes completed
            for (const auto &x: InFly){
                ctx.Send(x.ActorId, new TEvSyncerCommitDone(x.Cookie));
            }
            InFly.clear();

            // advance current entry point lsn
            Y_DEBUG_ABORT_UNLESS(ev->Get()->Results.size() == 1);
            CurEntryPointLsn = ev->Get()->Results[0].Lsn;
            LastCommitTime = TAppData::TimeProvider->Now();

            // send signal to RecoveryLogCutter
            ctx.Send(SyncerCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::Syncer, CurEntryPointLsn));

            // run another commit if required
            if (!Delayed.empty()) {
                // we have some pending messages
                InFly.swap(Delayed);
                GenerateCommit(ctx);
                Y_DEBUG_ABORT_UNLESS(Delayed.empty());
            }
        }

        void SelfCommit(const TActorContext &ctx) {
            // send commit to ourselves
            SelfCommitInProgress = true;
            ctx.Send(ctx.SelfID, new TEvSyncerCommit());
        }

        void ScheduleWakeup(const TActorContext &ctx) {
            WakeupScheduled = true;
            ctx.Schedule(AdvanceEntryPointTimeout, new TEvents::TEvWakeup());
        }

        void Handle(NPDisk::TEvCutLog::TPtr &ev, const TActorContext &ctx) {
            ui64 freeUpToLsn = ev->Get()->FreeUpToLsn;
            if (InFly.empty() && (CurEntryPointLsn < freeUpToLsn)) {
                SelfCommit(ctx);
            }
        }

        void HandleWakeup(const TActorContext &ctx) {
            WakeupScheduled = false;
            TInstant now = TAppData::TimeProvider->Now();

            // check that no writes going on at this moment and enouth time has passed
            if (InFly.empty() && (now >= LastCommitTime + AdvanceEntryPointTimeout)) {
                SelfCommit(ctx);
            }

            if (EnableSelfCommit && !SelfCommitInProgress) {
                // we don't want more than one wakeup scheduled
                ScheduleWakeup(ctx);
            }
        }

        void Handle(TEvSyncerCommitDone::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            SelfCommitInProgress = false;
            if (EnableSelfCommit && !WakeupScheduled)
                ScheduleWakeup(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvSyncerCommit, Handle)
            HFunc(TEvSyncerCommitDone, Handle)
            HFunc(NPDisk::TEvLogResult, Handle)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
        )

        PDISK_TERMINATE_STATE_FUNC_DEF;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_COMMITTER;
        }

        TSyncerCommitter(const TIntrusivePtr<TSyncerContext> &sc,
                         TSyncerDataSerializer &&sds)
            : TActorBootstrapped<TSyncerCommitter>()
            , SyncerCtx(sc)
            , EnableSelfCommit(!SyncerCtx->Config->BaseInfo.ReadOnly)
            , AdvanceEntryPointTimeout(SyncerCtx->Config->AdvanceEntryPointTimeout)
            , State(SyncerCtx->VCtx->Top, std::move(sds))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // Create Syncer Committer
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateSyncerCommitter(const TIntrusivePtr<TSyncerContext> &sc,
                                  TSyncerDataSerializer &&sds) {
        return new TSyncerCommitter(sc, std::move(sds));
    }

} // NKikimr
