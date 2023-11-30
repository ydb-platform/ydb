#include "blobstorage_anubisrunner.h"
#include "blobstorage_anubis.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_mon.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/core/mon.h>

using namespace NKikimrServices;

namespace NKikimr {


    ////////////////////////////////////////////////////////////////////////////
    // TAnubisRunnerHttpInfoActor
    ////////////////////////////////////////////////////////////////////////////
    class TAnubisRunnerHttpInfoActor : public TActorBootstrapped<TAnubisRunnerHttpInfoActor> {
        NMon::TEvHttpInfo::TPtr Ev;
        const TActorId ReplyId;
        const TActorId NotifyId;
        const TActorId AnubisId;
        const TString CurRunnerState;

        friend class TActorBootstrapped<TAnubisRunnerHttpInfoActor>;

        void Bootstrap(const TActorContext &ctx) {
            if (AnubisId == TActorId()) {
                RenderHtmlAndReply(ctx, TString());
            } else {
                Become(&TThis::StateFunc);
                ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
                ctx.Send(AnubisId, Ev->Release().Release());
            }
        }

        void RenderHtmlAndReply(const TActorContext &ctx, const TString &workerInfo) {
            TStringStream str;
            str << "\n";
            HTML(str) {
                DIV_CLASS("panel panel-warning") {
                    DIV_CLASS("panel-heading") {str << "Anubis";}
                    DIV_CLASS("panel-body") {
                        str << CurRunnerState;
                        if (!workerInfo.empty()) {
                            str << workerInfo;
                        } else {
                            str << "No Woker<br>";
                        }
                    }
                }
            }
            str << "\n";

            ctx.Send(ReplyId, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::SyncerInfoId));
            ctx.Send(NotifyId, new TEvents::TEvActorDied());
            Die(ctx);
        }

        void Handle(NMon::TEvHttpInfoRes::TPtr &ev, const TActorContext &ctx) {
            TStringStream str;
            ev->Get()->Output(str);
            RenderHtmlAndReply(ctx, str.Str());
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        void HandleWakeup(const TActorContext &ctx) {
            // didn't get reply from worker, exit by timeout
            RenderHtmlAndReply(ctx, "Worker's Timeout<br>");
        }

        STRICT_STFUNC(StateFunc,
                      HFunc(NMon::TEvHttpInfoRes, Handle)
                      HFunc(TEvents::TEvPoisonPill, HandlePoison)
                      CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
                      )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_ANUBIS;
        }

        TAnubisRunnerHttpInfoActor(
                             NMon::TEvHttpInfo::TPtr &ev,
                             const TActorId &notifyId,
                             const TActorId &anubisId,
                             const TString &curRunnerState)
            : TActorBootstrapped<TAnubisRunnerHttpInfoActor>()
            , Ev(ev)
            , ReplyId(Ev->Sender)
            , NotifyId(notifyId)
            , AnubisId(anubisId)
            , CurRunnerState(curRunnerState)
        {
            Y_ABORT_UNLESS(Ev->Get()->SubRequestId == TDbMon::SyncerInfoId);
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TQuorumForAnubisTracker
    ////////////////////////////////////////////////////////////////////////////
    class TQuorumForAnubisTracker {
    public:
        TQuorumForAnubisTracker(const TBlobStorageGroupInfo::TTopology *top)
            : Group(top)
        {}

        void FullSyncedWith(const TVDiskIdShort &vd) {
            Group |= TBlobStorageGroupInfo::TGroupVDisks(Group.GetTopology(), vd);
        }

        void Clear() {
            Group = TBlobStorageGroupInfo::TGroupVDisks(Group.GetTopology());
        }

        // we need to run Anubis because we may miss important updates
        bool HasQuorum() const {
            const TBlobStorageGroupInfo::TTopology *top = Group.GetTopology();
            return !top->GetQuorumChecker().CheckQuorumForGroup(~Group);
        }

        TString ToString() const {
            return Group.ToString();
        }

    private:
        TBlobStorageGroupInfo::TGroupVDisks Group;
    };


    ////////////////////////////////////////////////////////////////////////////
    // TAnubisRunner
    // We run Anubis in these cases:
    // 1. We got enough TEvFullSyncedWith messages, i.e. we failed to sync with
    //    peers in time and may loose some keep/don't keep flag updates
    // 2. We start Anubis once after process restart. We don't persist the fact
    //    of full sync with peers, so we don't know if successfully had run
    //    Anubis from the last full sync
    // 3. We retry Anubis if we got some errors from previous runs
    ////////////////////////////////////////////////////////////////////////////
    class TAnubisRunner : public TActorBootstrapped<TAnubisRunner> {
        std::shared_ptr<TAnubisCtx> AnubisCtx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TQuorumForAnubisTracker QuorumTracker;
        TActiveActors ActiveActors;
        TActorId AnubisId;
        bool RetryAnubis = true;
        bool AnubisScheduled = false;

        friend class TActorBootstrapped<TAnubisRunner>;

        void Bootstrap(const TActorContext &ctx) {
            Become(&TThis::StateFunc);
            ScheduleAnubisRun(ctx);
        }

        void ScheduleAnubisRun(const TActorContext &ctx) {
            // we schedule run between [AnubisTimeout, AnubisTimeout * 2]
            TDuration setting = AnubisCtx->AnubisTimeout;
            ui64 rawVal = setting.GetValue();
            ui64 randomNum = TAppData::RandomProvider->GenRand64();
            ui64 randomOffset = randomNum % rawVal;
            TDuration timeout = setting + TDuration::MicroSeconds(randomOffset);
            ctx.Schedule(timeout, new TEvents::TEvWakeup());
            AnubisScheduled = true;
        }

        void Handle(TEvFullSyncedWith::TPtr &ev, const TActorContext &ctx) {
            QuorumTracker.FullSyncedWith(ev->Get()->VDiskIdShort);
            RunAnubisIfRequired(ctx);
        }

        void Handle(TEvAnubisDone::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            AnubisId = TActorId();
            RetryAnubis = ev->Get()->Issues.HaveProblems();
            // check if need to schedule Anubis later on
            if (RetryAnubis && !AnubisScheduled) {
                ScheduleAnubisRun(ctx);
            }

            // check if need to rerun Anubis because of full syncs
            RunAnubisIfRequired(ctx);
        }

        void RunAnubisIfRequired(const TActorContext &ctx) {
            if (!AnubisId && QuorumTracker.HasQuorum()) {
                RunAnubis(ctx);
            }
        }

        void RunAnubis(const TActorContext &ctx) {
            QuorumTracker.Clear();
            AnubisId = ctx.Register(CreateAnubis(AnubisCtx->HullCtx, GInfo, ctx.SelfID, AnubisCtx->SkeletonId,
                AnubisCtx->ReplInterconnectChannel, AnubisCtx->AnubisOsirisMaxInFly));
            ActiveActors.Insert(AnubisId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        // This handler is called when TAnubisRunnerHttpInfoActor is finished
        void Handle(TEvents::TEvActorDied::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            ActiveActors.Erase(ev->Sender);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        void Handle(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            // on BlobStorage group reconfiguration we just restart Anubis
            GInfo = ev->Get()->NewInfo;
            if (AnubisId) {
                ActiveActors.Erase(AnubisId);
                ctx.Send(AnubisId, new TEvents::TEvPoisonPill());
                AnubisId = TActorId();
                RunAnubis(ctx);
                Y_ABORT_UNLESS(AnubisId != TActorId());
            }
        }

        void HandleWakeup(const TActorContext &ctx) {
            AnubisScheduled = false;
            if (AnubisId) {
                // We have Anubis running, just reschedule
                ScheduleAnubisRun(ctx);
            } else {
                // No Anubis running
                if (RetryAnubis) {
                    // Previous run was not successful, rerun
                    RunAnubis(ctx);
                } else {
                    // That's all, we don't need run or schedule anubis anymore
                }
            }
        }

        STRICT_STFUNC(StateFunc,
                      HFunc(TEvFullSyncedWith, Handle)
                      HFunc(TEvAnubisDone, Handle)
                      HFunc(NMon::TEvHttpInfo, Handle)
                      HFunc(TEvents::TEvActorDied, Handle)
                      HFunc(TEvents::TEvPoisonPill, HandlePoison)
                      HFunc(TEvVGenerationChange, Handle)
                      CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
                      )

        ////////////////////////////////////////////////////////////////////////
        // Handle TEvHttpInfo
        ////////////////////////////////////////////////////////////////////////
        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            Y_ABORT_UNLESS(ev->Get()->SubRequestId == TDbMon::SyncerInfoId);
            // save current local state
            TString s = QuorumTracker.ToString();
            // create an actor to handle request
            auto actor = std::make_unique<TAnubisRunnerHttpInfoActor>(ev, ctx.SelfID, AnubisId, s);
            auto aid = ctx.Register(actor.release());
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_ANUBIS;
        }

        TAnubisRunner(const std::shared_ptr<TAnubisCtx> &anubisCtx,
                      const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo)
            : TActorBootstrapped<TAnubisRunner>()
            , AnubisCtx(anubisCtx)
            , GInfo(ginfo)
            , QuorumTracker(AnubisCtx->HullCtx->VCtx->Top.get())
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // ANUBIS RUNNER ACTOR CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateAnubisRunner(const std::shared_ptr<TAnubisCtx> &anubisCtx,
                               const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo) {
        return new TAnubisRunner(anubisCtx, ginfo);
    }

} // NKikimr
