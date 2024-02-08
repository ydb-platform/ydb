#include "guid_firstrun.h"
#include "guid_proxywrite.h"
#include "blobstorage_syncer_committer.h"
#include "blobstorage_syncquorum.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_syncneighbors.h>
#include <ydb/core/blobstorage/vdisk/common/sublog.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <library/cpp/random_provider/random_provider.h>

using namespace NKikimrServices;
using namespace NKikimr::NSync;
using namespace NKikimr::NSyncer;

namespace NKikimr {

    namespace NSyncer {
        bool IsAction(EFirstRunStep s) {
            return s == EFirstRunStep::ACTION_GenerateGuid ||
                   s == EFirstRunStep::ACTION_WriteInProgressToQuorum ||
                   s == EFirstRunStep::ACTION_WriteSelectedLocally ||
                   s == EFirstRunStep::ACTION_WriteFinalToQuorum ||
                   s == EFirstRunStep::ACTION_WriteFinalLocally;
        }

        const char *EFirstRunStepToStr(EFirstRunStep s) {
            switch (s) {
                case EFirstRunStep::STATE__Uninitialized:
                    return "STATE:Uninitialized";
                case EFirstRunStep::ACTION_GenerateGuid:
                    return "ACTION:GenerateGuid";
                case EFirstRunStep::ACTION_WriteInProgressToQuorum:
                    return "ACTION:WriteInProgressToQuorum";
                case EFirstRunStep::STATE__WaitProgressWrittenToQuorum:
                    return "STATE:WaitProgressWrittenToQuorum";
                case EFirstRunStep::ACTION_WriteSelectedLocally:
                    return "ACTION:WriteSelectedLocally";
                case EFirstRunStep::STATE__WaitSelectedWrittenLocally:
                    return "STATE:WaitSelectedWrittenLocally";
                case EFirstRunStep::ACTION_WriteFinalToQuorum:
                    return "ACTION:WriteFinalToQuorum";
                case EFirstRunStep::STATE__WaitFinalWrittenToQuorum:
                    return "STATE:WaitFinalWrittenToQuorum";
                case EFirstRunStep::ACTION_WriteFinalLocally:
                    return "ACTION:WriteFinalLocally";
                case EFirstRunStep::STATE__WaitFinalWrittenLocally:
                    return "STATE:WaitFinalWrittenLocally";
                case EFirstRunStep::STATE__Terminated:
                    return "STATE:Terminated";
                default: return "UNKNOWN";
            }
        }
    } // NSyncer

    namespace {

        ////////////////////////////////////////////////////////////////////////
        // TVDiskState represents state of a neighbor VDisk during FirstRun
        ////////////////////////////////////////////////////////////////////////
        struct TVDiskState {
            // from protobuf
            using ESyncState = NKikimrBlobStorage::TSyncGuidInfo::EState;
            using TSyncVal = NKikimrBlobStorage::TSyncGuidInfo;


            // ActorID of a proxy that communicates with the given VDisk
            TActorId ProxyId;
            // if we got response from a vdisk
            bool GotResponse = false;

            TVDiskState() = default;

            void SetActorID(const TActorId &proxyId) {
                ProxyId = proxyId;
            }

            void Clear() {
                ProxyId = TActorId();
                GotResponse = false;
            }

            void Setup() {
                GotResponse = true;
            }
        };
    }


    ////////////////////////////////////////////////////////////////////////////
    // TVDiskGuidFirstRunState
    ////////////////////////////////////////////////////////////////////////////
    class TVDiskGuidFirstRunState {
    public:
        // from protobuf
        using ESyncState = NKikimrBlobStorage::TSyncGuidInfo::EState;
        using TSyncVal = NKikimrBlobStorage::TSyncGuidInfo;
        using ELocalState = NKikimrBlobStorage::TLocalGuidInfo::EState;
        using TLocalVal = NKikimrBlobStorage::TLocalGuidInfo;
        // do something with alive proxy: abandom it, reconfigure it, etc
        using TAliveProxyNotifier = std::function<void(TVDiskState&)>;
        // write our decision ot a given VDisk
        using TRunWriteProxyActor = std::function<void(TVDiskInfo<TVDiskState>&,
                                                       TVDiskEternalGuid,
                                                       ESyncState)>;

    public:
        TVDiskGuidFirstRunState(const TVDiskIdShort &self,
                                const std::shared_ptr<TBlobStorageGroupInfo::TTopology> &top,
                                EFirstRunStep step,
                                TVDiskEternalGuid guid)
            : Neighbors(self, top)
            , QuorumTracker(self, top, true)
            , Step(step)
            , Guid(guid)
        {
            Y_ABORT_UNLESS(IsAction(Step));
        }

        void GenerateGuid() {
            Y_ABORT_UNLESS(Step == EFirstRunStep::ACTION_GenerateGuid);
            Guid = TAppData::RandomProvider->GenRand64();
            Step = EFirstRunStep::ACTION_WriteInProgressToQuorum;
        }

        void RunWritesInProgressToQuorum(TRunWriteProxyActor func) {
            RunWritesToQuorum(func,
                              EFirstRunStep::ACTION_WriteInProgressToQuorum,
                              EFirstRunStep::STATE__WaitProgressWrittenToQuorum,
                              TSyncVal::InProgress);
        }

        // true if we got quorum and can switch to the next step, false otherwise
        bool SetResultForWriteInProgress(const TVDiskID &vdisk,
                                         TAliveProxyNotifier abandomProxy) {
            return SetResult(vdisk,
                             abandomProxy,
                             EFirstRunStep::STATE__WaitProgressWrittenToQuorum,
                             EFirstRunStep::ACTION_WriteSelectedLocally);
        }

        void RunWriteSelectedLocally() {
            Y_ABORT_UNLESS(Step == EFirstRunStep::ACTION_WriteSelectedLocally, "Step# %s",
                     EFirstRunStepToStr(Step));
            Step = EFirstRunStep::STATE__WaitSelectedWrittenLocally;
        }

        void SetResultForWriteSelectedLocally() {
            Y_ABORT_UNLESS(Step == EFirstRunStep::STATE__WaitSelectedWrittenLocally, "Step# %s",
                     EFirstRunStepToStr(Step));
            Step = EFirstRunStep::ACTION_WriteFinalToQuorum;
        }

        void RunWritesFinalToQuorum(TRunWriteProxyActor func) {
            RunWritesToQuorum(func,
                              EFirstRunStep::ACTION_WriteFinalToQuorum,
                              EFirstRunStep::STATE__WaitFinalWrittenToQuorum,
                              TSyncVal::Final);
        }

        // true if we got quorum and can switch to the next step, false otherwise
        bool SetResultForWriteFinal(const TVDiskID &vdisk, TAliveProxyNotifier abandomProxy) {
            return SetResult(vdisk,
                             abandomProxy,
                             EFirstRunStep::STATE__WaitFinalWrittenToQuorum,
                             EFirstRunStep::ACTION_WriteFinalLocally);
        }

        void RunWriteFinalLocally() {
            Y_ABORT_UNLESS(Step == EFirstRunStep::ACTION_WriteFinalLocally, "Step# %s",
                     EFirstRunStepToStr(Step));
            Step = EFirstRunStep::STATE__WaitFinalWrittenLocally;
        }

        void SetResultForWriteFinalLocally() {
            Y_ABORT_UNLESS(Step == EFirstRunStep::STATE__WaitFinalWrittenLocally, "Step# %s",
                     EFirstRunStepToStr(Step));
            Step = EFirstRunStep::STATE__Terminated;
        }

        EFirstRunStep GetStep() const {
            return Step;
        }

        TVDiskEternalGuid GetGuid() const {
            return Guid;
        }

        unsigned NotifyAliveProxies(TAliveProxyNotifier func) {
            unsigned counter = 0;
            for (auto &x : Neighbors) {
                if (!x.Get().GotResponse) {
                    // do 'func' with the proxy
                    func(x.Get());
                    ++counter;
                }
            }
            return counter;
        }

    private:
        NSync::TVDiskNeighbors<TVDiskState> Neighbors;
        NSync::TQuorumTracker QuorumTracker;
        EFirstRunStep Step;
        TVDiskEternalGuid Guid;

        void CleareNeighborsAndQuorumTracker() {
            for (auto &x : Neighbors) {
                x.Get().Clear();
            }
            QuorumTracker.Clear();
        }

        void RunWritesToQuorum(TRunWriteProxyActor func,
                               EFirstRunStep curStep,
                               EFirstRunStep nextStep,
                               ESyncState syncState) {
            Y_ABORT_UNLESS(Step == curStep, "Step# %s curStep# %s",
                     EFirstRunStepToStr(Step), EFirstRunStepToStr(curStep));
            CleareNeighborsAndQuorumTracker();
            for (auto &x : Neighbors) {
                func(x, Guid, syncState);
            }
            Step = nextStep;
        }

        bool SetResult(const TVDiskID &vdisk,
                       TAliveProxyNotifier abandomProxy,
                       EFirstRunStep curStep,
                       EFirstRunStep nextStep) {
            Y_ABORT_UNLESS(Step == curStep, "Step# %s curStep# %s",
                     EFirstRunStepToStr(Step), EFirstRunStepToStr(curStep));
            Y_ABORT_UNLESS(Neighbors[vdisk].VDiskIdShort == vdisk);

            // update
            Neighbors[vdisk].Get().Setup();
            QuorumTracker.Update(vdisk);

            // switch to next step if we got quorum
            if (QuorumTracker.HasQuorum()) {
                NotifyAliveProxies(abandomProxy);
                Step = nextStep;
                return true;
            } else {
                return false;
            }
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // TVDiskGuidFirstRunActor
    ////////////////////////////////////////////////////////////////////////////
    class TVDiskGuidFirstRunActor : public TActorBootstrapped<TVDiskGuidFirstRunActor>
    {
        friend class TActorBootstrapped<TVDiskGuidFirstRunActor>;

        // from protobuf
        using ESyncState = NKikimrBlobStorage::TSyncGuidInfo::EState;
        using TSyncVal = NKikimrBlobStorage::TSyncGuidInfo;
        using ELocalState = NKikimrBlobStorage::TLocalGuidInfo::EState;
        using TLocalVal = NKikimrBlobStorage::TLocalGuidInfo;

        enum EWaitFor {
            WaitNotSet,
            WaitForProxies,
            WaitForCommitter
        };

        TIntrusivePtr<TVDiskContext> VCtx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        const TActorId CommitterId;
        const TActorId NotifyId;
        TVDiskGuidFirstRunState FirstRunState;
        EWaitFor WaitFor = WaitNotSet;

        ////////////////////////////////////////////////////////////////////////
        // Bootstrap: start from the given step
        ////////////////////////////////////////////////////////////////////////
        void Bootstrap(const TActorContext &ctx) {
            // depending on our progress
            auto startStep = FirstRunState.GetStep();
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidFirstRunActor: START; step# %s",
                            EFirstRunStepToStr(startStep)));
            SUBLOGLINE(NotifyId, ctx, { stream << "FirstRun: START; step# " << startStep; });

            switch (startStep) {
                case EFirstRunStep::ACTION_GenerateGuid:
                    GenerateGuid(ctx);
                    break;
                case EFirstRunStep::ACTION_WriteInProgressToQuorum:
                    WriteGuidInProgressToQuorum(ctx);
                    break;
                case EFirstRunStep::ACTION_WriteSelectedLocally:
                    WriteSelectedLocally(ctx);
                    break;
                case EFirstRunStep::ACTION_WriteFinalToQuorum:
                    WriteFinalGuidToQuorum(ctx);
                    break;
                case EFirstRunStep::ACTION_WriteFinalLocally:
                    WriteFinalLocally(ctx);
                    break;
                default: Y_ABORT("Unexpected step: %s", EFirstRunStepToStr(startStep));
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // GENERATE GUID
        ////////////////////////////////////////////////////////////////////////
        void GenerateGuid(const TActorContext &ctx) {
            FirstRunState.GenerateGuid();

            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidFirstRunActor: GenerateGuid; guid# %s",
                            FirstRunState.GetGuid().ToString().data()));
            SUBLOGLINE(NotifyId, ctx, {
                stream << "FirstRun: GenerateGuid; guid# " << FirstRunState.GetGuid();
            });

            WriteGuidInProgressToQuorum(ctx);
        }

        ////////////////////////////////////////////////////////////////////////
        // WRITE GUID IN PROGRESS TO QUORUM
        ////////////////////////////////////////////////////////////////////////
        void WriteGuidInProgressToQuorum(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidFirstRunActor: WriteGuidInProgressToQuorum"));
            SUBLOGLINE(NotifyId, ctx, { stream << "FirstRun: WriteGuidInProgressToQuorum"; });

            // run proxy for every VDisk in the group
            auto runProxyForVDisk = [this, &ctx] (TVDiskInfo<TVDiskState>& x,
                                                  TVDiskEternalGuid guid,
                                                  ESyncState state) {
                auto selfVDiskId = GInfo->GetVDiskId(VCtx->ShortSelfVDisk);
                auto vd = GInfo->GetVDiskId(x.OrderNumber);
                auto aid = GInfo->GetActorId(x.OrderNumber);
                auto proxyActor = CreateProxyForWritingVDiskGuid(VCtx, selfVDiskId, vd, aid, ctx.SelfID, state, guid);
                auto actorId = ctx.Register(proxyActor);
                x.Get().SetActorID(actorId);
            };

            FirstRunState.RunWritesInProgressToQuorum(runProxyForVDisk);
            Become(&TThis::WriteGuidInProgressStateFunc);
            WaitFor = WaitForProxies;
        }

        void HandleInProgressWritten(TEvVDiskGuidWritten::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidFirstRunActor: HandleInProgressWritten: msg# %s",
                            ev->Get()->ToString().data()));
            SUBLOGLINE(NotifyId, ctx, {
                stream << "FirstRun: InProgressGuidWritten; msg# " << ev->Get()->ToString();
            });

            auto abandomProxy = [&ctx] (TVDiskState& x) {
                Y_ABORT_UNLESS(!x.GotResponse);
                // cancel proxy
                ctx.Send(x.ProxyId, new NActors::TEvents::TEvPoisonPill());
            };

            auto msg = ev->Get();
            bool enough = FirstRunState.SetResultForWriteInProgress(msg->VDiskId, abandomProxy);
            if (enough) {
                WriteSelectedLocally(ctx);
            }
        }

        STRICT_STFUNC(WriteGuidInProgressStateFunc,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvVDiskGuidWritten, HandleInProgressWritten)
            HFunc(TEvVGenerationChange, HandleWhileProxiesRunning)
        )

        ////////////////////////////////////////////////////////////////////////
        // WRITE SELECTED GUID LOCALLY
        ////////////////////////////////////////////////////////////////////////
        void WriteSelectedLocally(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidFirstRunActor: WriteSelectedLocally"));
            SUBLOGLINE(NotifyId, ctx, { stream << "FirstRun: WriteSelectedLocally"; });

            FirstRunState.RunWriteSelectedLocally();
            auto guid = FirstRunState.GetGuid();
            auto msg = TEvSyncerCommit::Local(TLocalVal::Selected, guid);
            ctx.Send(CommitterId, msg.release());
            Become(&TThis::WriteSelectedLocallyStateFunc);
            WaitFor = WaitForCommitter;
        }

        void HandleSelectedLocally(TEvSyncerCommitDone::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            FirstRunState.SetResultForWriteSelectedLocally();
            WriteFinalGuidToQuorum(ctx);
        }

        STRICT_STFUNC(WriteSelectedLocallyStateFunc,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvSyncerCommitDone, HandleSelectedLocally)
            // NOTE: In WaitForCommitter state we can still receive TEvVDiskGuidWritten
            // messages (i.e. replies from the previous phase). Ignore them.
            IgnoreFunc(TEvVDiskGuidWritten)
            HFunc(TEvVGenerationChange, HandleNoProxies)
        )

        ////////////////////////////////////////////////////////////////////////
        // WRITE FINAL GUID TO QUORUM
        ////////////////////////////////////////////////////////////////////////
        void WriteFinalGuidToQuorum(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidFirstRunActor: WriteFinalGuidToQuorum"));
            SUBLOGLINE(NotifyId, ctx, { stream << "FirstRun: WriteFinalGuidToQuorum"; });

            // run proxy for every VDisk in the group
            auto runProxyForVDisk = [this, &ctx] (TVDiskInfo<TVDiskState>& x,
                                                  TVDiskEternalGuid guid,
                                                  ESyncState state) {
                auto selfVDiskId = GInfo->GetVDiskId(VCtx->ShortSelfVDisk);
                auto vd = GInfo->GetVDiskId(x.OrderNumber);
                auto aid = GInfo->GetActorId(x.OrderNumber);
                auto proxyActor = CreateProxyForWritingVDiskGuid(VCtx, selfVDiskId, vd, aid, ctx.SelfID, state, guid);
                auto actorId = ctx.Register(proxyActor);
                x.Get().SetActorID(actorId);
            };

            FirstRunState.RunWritesFinalToQuorum(runProxyForVDisk);
            Become(&TThis::WriteFinalGuidStateFunc);
            WaitFor = WaitForProxies;
        }

        void HandleFinalWritten(TEvVDiskGuidWritten::TPtr &ev, const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidFirstRunActor: HandleFinalWritten: msg# %s",
                            ev->Get()->ToString().data()));
            SUBLOGLINE(NotifyId, ctx, {
                stream << "FirstRun: FinalGuidWritten; msg# " << ev->Get()->ToString();
            });

            auto abandomProxy = [&ctx] (TVDiskState& x) {
                Y_ABORT_UNLESS(!x.GotResponse);
                // cancel proxy
                ctx.Send(x.ProxyId, new NActors::TEvents::TEvPoisonPill());
            };

            auto msg = ev->Get();
            bool enough = FirstRunState.SetResultForWriteFinal(msg->VDiskId, abandomProxy);
            if (enough) {
                WriteFinalLocally(ctx);
            }
        }

        STRICT_STFUNC(WriteFinalGuidStateFunc,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvVDiskGuidWritten, HandleFinalWritten)
            HFunc(TEvVGenerationChange, HandleWhileProxiesRunning)
        )

        ////////////////////////////////////////////////////////////////////////
        // WRITE FINAL GUID LOCALLY
        ////////////////////////////////////////////////////////////////////////
        void WriteFinalLocally(const TActorContext &ctx) {
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidFirstRunActor: WriteFinalLocally"));
            SUBLOGLINE(NotifyId, ctx, { stream << "FirstRun: WriteFinalLocally"; });

            FirstRunState.RunWriteFinalLocally();
            auto guid = FirstRunState.GetGuid();
            Y_ABORT_UNLESS(guid);
            ui64 dbBirthLsn = 0;
            auto msg = TEvSyncerCommit::LocalFinal(guid, dbBirthLsn);
            ctx.Send(CommitterId, msg.release());
            Become(&TThis::WriteFinalLocallyStateFunc);
            WaitFor = WaitForCommitter;
        }

        void HandleFinalLocally(TEvSyncerCommitDone::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            FirstRunState.SetResultForWriteFinalLocally();
            Finish(ctx);
        }

        STRICT_STFUNC(WriteFinalLocallyStateFunc,
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(TEvSyncerCommitDone, HandleFinalLocally)
            // NOTE: In WaitForCommitter state we can still receive TEvVDiskGuidWritten
            // messages (i.e. replies from the previous phase). Ignore them.
            IgnoreFunc(TEvVDiskGuidWritten)
            HFunc(TEvVGenerationChange, HandleNoProxies)
        )

        ////////////////////////////////////////////////////////////////////////
        // FINISH
        ////////////////////////////////////////////////////////////////////////
        void Finish(const TActorContext &ctx) {
            auto guid = FirstRunState.GetGuid();
            Y_ABORT_UNLESS(guid);
            ctx.Send(NotifyId, new TEvSyncerGuidFirstRunDone(guid));
            LOG_DEBUG(ctx, BS_SYNCER,
                      VDISKP(VCtx->VDiskLogPrefix, "TVDiskGuidFirstRunActor: FINISH"));
            SUBLOGLINE(NotifyId, ctx, { stream << "FirstRun: FINISH"; });
            Die(ctx);
        }

        ////////////////////////////////////////////////////////////////////////
        // POISON
        ////////////////////////////////////////////////////////////////////////
        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            switch (WaitFor) {
                case WaitNotSet:
                    break;
                case WaitForProxies: {
                    auto abandomProxy = [&ctx] (TVDiskState& x) {
                        Y_ABORT_UNLESS(!x.GotResponse);
                        // cancel proxy
                        ctx.Send(x.ProxyId, new NActors::TEvents::TEvPoisonPill());
                    };
                    FirstRunState.NotifyAliveProxies(abandomProxy);
                    break;
                }
                case WaitForCommitter:
                    break;
                default: Y_ABORT("Unexpected case");
            }
        }

        ////////////////////////////////////////////////////////////////////////
        // BlobStorage Group reconfiguration
        ////////////////////////////////////////////////////////////////////////
        void HandleNoProxies(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            // save new Group Info
            GInfo = ev->Get()->NewInfo;
        }

        void HandleWhileProxiesRunning(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            // save new Group Info
            GInfo = ev->Get()->NewInfo;
            // reconfigure alive proxies
            auto reconfigureProxy = [&ctx, &ev] (TVDiskState& x) {
                Y_ABORT_UNLESS(!x.GotResponse);
                // cancel proxy
                ctx.Send(x.ProxyId, ev->Get()->Clone());
            };
            FirstRunState.NotifyAliveProxies(reconfigureProxy);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNC_VDISK_GUID_FIRST_RUN;
        }

        TVDiskGuidFirstRunActor(TIntrusivePtr<TVDiskContext> vctx,
                                TIntrusivePtr<TBlobStorageGroupInfo> info,
                                const TActorId &committerId,
                                const TActorId &notifyId,
                                EFirstRunStep startStep,
                                TVDiskEternalGuid guid)
            : TActorBootstrapped<TVDiskGuidFirstRunActor>()
            , VCtx(std::move(vctx))
            , GInfo(std::move(info))
            , CommitterId(committerId)
            , NotifyId(notifyId)
            , FirstRunState(VCtx->ShortSelfVDisk, GInfo->PickTopology(), startStep, guid)
        {}
    };

    IActor *CreateVDiskGuidFirstRunActor(TIntrusivePtr<TVDiskContext> vctx,
                                         TIntrusivePtr<TBlobStorageGroupInfo> info,
                                         const TActorId &committerId,
                                         const TActorId &notifyId,
                                         EFirstRunStep startStep,
                                         TVDiskEternalGuid guid) {
        return new TVDiskGuidFirstRunActor(std::move(vctx), std::move(info), committerId, notifyId, startStep, guid);
    }

} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NSyncer::EFirstRunStep, stream, value) {
    stream << NKikimr::NSyncer::EFirstRunStepToStr(value);
}

