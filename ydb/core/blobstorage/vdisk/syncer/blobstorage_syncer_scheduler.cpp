#include "blobstorage_syncer_scheduler.h"
#include "blobstorage_syncer_localwriter.h"
#include "blobstorage_syncer_committer.h"
#include "index_sst_writer.h"
#include "syncer_job_task.h"
#include "syncer_job_actor.h"
#include "syncer_merger.h"
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_status.h>

#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <util/generic/queue.h>
#include <util/generic/deque.h>

using namespace NKikimrServices;
using namespace NKikimr::NSyncer;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TSyncerSchedulerHttpActor
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerSchedulerHttpActor : public TActorBootstrapped<TSyncerSchedulerHttpActor> {
        TIntrusivePtr<TSyncerContext> SyncerContext;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TIntrusivePtr<TSyncerData> SyncerData;
        NMon::TEvHttpInfo::TPtr Ev;
        const TActorId NotifyId;

        friend class TActorBootstrapped<TSyncerSchedulerHttpActor>;

        // outputs state in HTML
        struct TPrinter {
            const TBlobStorageGroupInfo &GInfo;
            TEvInterconnect::TEvNodesInfo::TPtr NodesInfo;

            TPrinter(const TBlobStorageGroupInfo &ginfo,
                     TEvInterconnect::TEvNodesInfo::TPtr nodesInfo)
                : GInfo(ginfo)
                , NodesInfo(nodesInfo)
            {}

            void operator() (IOutputStream &str, TSyncNeighbors::TConstIterator it) {
                HTML(str) {
                    SMALL() {
                        STRONG() {
                            // output VDiskID
                            auto vd = GInfo.GetVDiskId(it->VDiskIdShort);
                            str << "VDiskId: " << vd.ToStringWOGeneration() << "<br>";
                            // output node info
                            TActorId aid = GInfo.GetActorId(it->VDiskIdShort);
                            ui32 nodeId = aid.NodeId();
                            using TNodeInfo = TEvInterconnect::TNodeInfo;
                            const TNodeInfo *info = NodesInfo->Get()->GetNodeInfo(nodeId);
                            if (!info) {
                                str << "Node: NameService Error<br>";
                            } else {
                                str << "Node: " << info->Host << ":" << info->Port << "<br>";
                            }
                        }
                    }

                    if (it->Myself) {
                        PARA_CLASS("text-info") {str << "Self";}
                    } else {
                        const NSyncer::TPeer &peer = it->Get();
                        peer.OutputHtml(str);
                    }
                }
            }
        };

        void Bootstrap(const TActorContext &ctx) {
            ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
            Become(&TThis::StateFunc);
        }

        void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
            TStringStream str;
            TPrinter printer(*GInfo, ev);
            SyncerData->Neighbors->OutputHtmlTable(str, printer);
            ctx.Send(Ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), TDbMon::SyncerInfoId));
            ctx.Send(NotifyId, new TEvents::TEvGone());
            Die(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
                      HFunc(TEvInterconnect::TEvNodesInfo, Handle)
                      HFunc(TEvents::TEvPoisonPill, HandlePoison)
                      )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_HTTPREQ;
        }

        TSyncerSchedulerHttpActor(const TIntrusivePtr<TSyncerContext> &sc,
                                  const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
                                  const TIntrusivePtr<TSyncerData> &syncerData,
                                  NMon::TEvHttpInfo::TPtr &ev,
                                  const TActorId &notifyId)
            : TActorBootstrapped<TSyncerSchedulerHttpActor>()
            , SyncerContext(sc)
            , GInfo(ginfo)
            , SyncerData(syncerData)
            , Ev(ev)
            , NotifyId(notifyId)
        {
            Y_VERIFY_S(Ev->Get()->SubRequestId == TDbMon::SyncerInfoId, SyncerContext->VCtx->VDiskLogPrefix);
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // TEvSyncerCommitProxyDone
    ////////////////////////////////////////////////////////////////////////////
    struct TEvSyncerCommitProxyDone :
        public TEventLocal<TEvSyncerCommitProxyDone, TEvBlobStorage::EvSyncerCommitProxyDone>
    {
        TVDiskID VDiskId;
        NSyncer::TPeerSyncState PeerSyncState;
        bool FullRecovery = false;

        TEvSyncerCommitProxyDone(
                const TVDiskID& vDiskId,
                const NSyncer::TPeerSyncState& peerSyncState,
                bool fullRecovery)
            : VDiskId(vDiskId)
            , PeerSyncState(peerSyncState)
            , FullRecovery(fullRecovery)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSyncerCommitterProxy
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerCommitterProxy : public TActorBootstrapped<TSyncerCommitterProxy> {
        friend class TActorBootstrapped<TSyncerCommitterProxy>;

        const TActorId NotifyId;
        const TActorId CommitterId;
        TVDiskID VDiskId;
        NSyncer::TPeerSyncState PeerSyncState;
        bool FullRecovery = false;

        void Bootstrap(const TActorContext &ctx) {
            auto msg = TEvSyncerCommit::Remote(VDiskId, PeerSyncState);
            ctx.Send(CommitterId, msg.release());
            TThis::Become(&TThis::StateFunc);
        }

        void Handle(TEvSyncerCommitDone::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ctx.Send(NotifyId, new TEvSyncerCommitProxyDone(VDiskId, PeerSyncState, FullRecovery));
            Die(ctx);
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvSyncerCommitDone, Handle);
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr auto ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_COMMITTER_PROXY;
        }

        TSyncerCommitterProxy(const TActorId &notifyId,
                              const TActorId &committerId,
                              const TVDiskID &vDiskId,
                              const NSyncer::TPeerSyncState &peerSyncState,
                              bool fullRecovery)
            : TActorBootstrapped<TSyncerCommitterProxy>()
            , NotifyId(notifyId)
            , CommitterId(committerId)
            , VDiskId(vDiskId)
            , PeerSyncState(peerSyncState)
            , FullRecovery(fullRecovery)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // TSyncerScheduler
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerScheduler : public TActorBootstrapped<TSyncerScheduler> {
        using IActor::Schedule; // name is used by IActor API

        // from protobuf
        using ESyncStatus = NKikimrVDiskData::TSyncerVDiskEntry::ESyncStatus;
        using TSyncStatusVal = NKikimrVDiskData::TSyncerVDiskEntry;
        // other
        using TVDiskInfoPtr = const TSyncNeighbors::TValue *;

        struct TGreater {
            bool operator() (const TVDiskInfoPtr &x, const TVDiskInfoPtr &y) {
                return x->Get().PeerSyncState.SchTime > y->Get().PeerSyncState.SchTime;
            }
        };

        using TSchedulerQueue = TPriorityQueue<TVDiskInfoPtr, TVector<TVDiskInfoPtr>, TGreater>;

        using TPeerSyncStateMap = std::unordered_map<TVDiskID, NSyncer::TPeerSyncState>;

        TIntrusivePtr<TSyncerContext> SyncerContext;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TIntrusivePtr<TSyncerData> SyncerData;
        TSchedulerQueue SchedulerQueue;
        TActiveActors ActiveActors;
        const TDuration SyncTimeInterval;
        TActorId CommitterId;
        bool Scheduled;
        std::shared_ptr<TSjCtx> JobCtx;

        bool FullSyncGatherMode = false;
        TPeerSyncStateMap GatheredDisksForFullSync;
        std::unordered_map<TActorId, TPeerSyncStateMap> FullSyncsInProgress;

        friend class TActorBootstrapped<TSyncerScheduler>;

        struct TEvPrivate {
            enum EEv {
                EvFullSyncGatherTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
                EvEnd
            };
            static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE),
                "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

            struct TEvFullSyncGatherTimeout :
                public TEventLocal<TEvFullSyncGatherTimeout, EvFullSyncGatherTimeout> {};
        };


        void ActualizeUnsyncedDisksNum() {
            unsigned unsyncedDisks = 0;
            for (const auto &x : *SyncerData->Neighbors) {
                if (!x.Myself) {
                    auto state = x.Get().PeerSyncState.LastSyncStatus;
                    if (!NSyncer::TPeerSyncState::Good(state))
                        unsyncedDisks++;
                }
            }
            SyncerContext->MonGroup.SyncerUnsyncedDisks() = unsyncedDisks;
        }


        void Bootstrap(const TActorContext &ctx) {
            // fill in SchedulerQueue
            for (const auto &x : *SyncerData->Neighbors) {
                if (!x.Myself) {
                    Y_DEBUG_ABORT_UNLESS(x.Get().PeerSyncState.LastSyncStatus != TSyncStatusVal::Running);
                    SchedulerQueue.push(&x);
                }
            }

            // if we haven't found any neighbors to sync with, notify skeleton
            if (SchedulerQueue.empty()) {
                Become(&TThis::NothingToDo);
            } else {
                // start sync immediately
                Schedule(ctx);
            }
        }

        void HandleWakeup(const TActorContext &ctx) {
            Scheduled = false;
            Schedule(ctx);
        }

        void HandleFullSyncGatherTimeout(TEvPrivate::TEvFullSyncGatherTimeout::TPtr& /*ev*/, const TActorContext& ctx) {
            Y_VERIFY_S(FullSyncGatherMode, SyncerContext->VCtx->VDiskLogPrefix);
            FullSyncGatherMode = false;
            StartFullSyncJob(ctx);
            Schedule(ctx);
        }

        void ApplyChanges(
                const TActorContext &ctx,
                const TVDiskID& vDiskId,
                const NSyncer::TPeerSyncState& peerSyncState,
                bool fullRecovery) {
            LOG_INFO(ctx, BS_SYNCER, VDISKP(SyncerContext->VCtx->VDiskLogPrefix, "SYNCER JOB DONE: %s", vDiskId.ToString().data()));

            auto interval = fullRecovery ? TDuration::Seconds(0) : SyncTimeInterval;
            SyncerData->Neighbors->ApplyChanges(vDiskId, peerSyncState, interval);
            ActualizeUnsyncedDisksNum();
            SchedulerQueue.push(&(*SyncerData->Neighbors)[vDiskId]);
            Schedule(ctx);
        }

        void ApplyFullSyncChanges(
                const TActorContext& ctx,
                const std::unordered_map<TVDiskID, NSyncer::TPeerSyncState>& syncStates) {
            for (const auto& [id, state] : syncStates) {
                LOG_INFO(ctx, BS_SYNCER, VDISKP(SyncerContext->VCtx->VDiskLogPrefix, "FULL SYNC APPLIED: %s", id.ToString().data()));

                SyncerData->Neighbors->ApplyChanges(id, state, TDuration::Seconds(0));
                SchedulerQueue.push(&(*SyncerData->Neighbors)[id]);
            }
            ActualizeUnsyncedDisksNum();
            Schedule(ctx);
        }

        void Commit(
                const TActorContext& ctx,
                const TVDiskID& vDiskId,
                const NSyncer::TPeerSyncState& peerSyncState,
                bool fullRecovery) {
            auto proxy = std::make_unique<TSyncerCommitterProxy>(
                    ctx.SelfID,
                    CommitterId,
                    vDiskId,
                    peerSyncState,
                    fullRecovery);
            const TActorId aid = ctx.Register(proxy.release());
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        void Handle(TEvSyncerJobDone::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            TEvSyncerJobDone *msg = ev->Get();
#ifdef USE_NEW_FULL_SYNC_SCHEME
            if (msg->Task->SstWriterId) {
                ActiveActors.Erase(msg->Task->SstWriterId);
            }
#endif
#ifdef USE_MERGE_FULL_SYNC_SCHEME
            if (msg->Task->Type == TSyncerJobTask::EFullRecover) {
                GatheredDisksForFullSync[msg->Task->VDiskId] = msg->Task->GetCurrent();
                if (!FullSyncGatherMode) {
                    EnterFullSyncGatherMode(ctx);
                }
                return;
            }
#endif
            if (msg->Task->NeedCommit()) {
                Commit(ctx, msg->Task->VDiskId, msg->Task->GetCurrent(), msg->Task->IsFullRecoveryTask());
            } else {
                ApplyChanges(ctx, msg->Task->VDiskId, msg->Task->GetCurrent(), msg->Task->IsFullRecoveryTask());
            }
        }

        void Handle(TEvSyncerFullSyncFinished::TPtr& ev, const TActorContext& ctx) {
            ActiveActors.Erase(ev->Sender);

            Y_VERIFY_S(FullSyncsInProgress.contains(ev->Sender), SyncerContext->VCtx->VDiskLogPrefix);
            auto& oldSyncStates = FullSyncsInProgress[ev->Sender];

            for (const auto& [vDiskId, peerSyncState] : ev->Get()->PeerSyncStates) {
                auto it = oldSyncStates.find(vDiskId);
                if (it == oldSyncStates.end()) {
                    continue;
                }
                if (it->second.SyncState != peerSyncState.SyncState) {
                    Commit(ctx, vDiskId, peerSyncState, false);
                } else {
                    ApplyChanges(ctx, vDiskId, peerSyncState, false);
                }
            }
            FullSyncsInProgress.erase(ev->Sender);
        }

        void Handle(TEvSyncerCommitProxyDone::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            TEvSyncerCommitProxyDone *msg = ev->Get();
            ApplyChanges(ctx, msg->VDiskId, msg->PeerSyncState, msg->FullRecovery);
        }

        void StartSyncJob(const TActorContext& ctx, const TVDiskInfoPtr& info) {
            TActorId sstWriterId;
#ifdef USE_NEW_FULL_SYNC_SCHEME
            auto* sstWriterActor = new TIndexSstWriterActor(
                SyncerContext->VCtx,
                SyncerContext->PDiskCtx,
                SyncerContext->LevelIndexLogoBlob,
                SyncerContext->LevelIndexBlock,
                SyncerContext->LevelIndexBarrier);
            sstWriterId = ctx.Register(sstWriterActor);
            ActiveActors.Insert(sstWriterId, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
#endif
            auto task = std::make_unique<TSyncerJobTask>(
                    TSyncerJobTask::EJustSync,
                    GInfo->GetVDiskId(info->OrderNumber),
                    GInfo->GetActorId(info->OrderNumber),
                    sstWriterId,
                    info->Get().PeerSyncState,
                    JobCtx);
            const TActorId aid = ctx.Register(CreateSyncerJob(SyncerContext, std::move(task), ctx.SelfID));
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);

#ifdef USE_NEW_FULL_SYNC_SCHEME
            sstWriterActor->SetSyncerJobActorId(aid);
#endif
        }

        void StartFullSyncJob(const TActorContext& ctx) {
            auto mergerActor = ctx.Register(CreateIndexMergerActor(SyncerContext, ctx.SelfID, GatheredDisksForFullSync, GInfo));
            ActiveActors.Insert(mergerActor, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);

            FullSyncsInProgress[mergerActor] = GatheredDisksForFullSync;
            GatheredDisksForFullSync.clear();
        }

        void EnterFullSyncGatherMode(const TActorContext &ctx) {
            while (!SchedulerQueue.empty()) {
                TVDiskInfoPtr info = SchedulerQueue.top();
                SchedulerQueue.pop();
                Y_DEBUG_ABORT_UNLESS(info->Get().PeerSyncState.LastSyncStatus != TSyncStatusVal::Running);
                StartSyncJob(ctx, info);
            }
            FullSyncGatherMode = true;
            ctx.Schedule(SyncTimeInterval, new TEvPrivate::TEvFullSyncGatherTimeout);
        }

        void Schedule(const TActorContext &ctx) {
            Become(&TThis::StateFunc);

            TInstant now = TAppData::TimeProvider->Now();
            // NOTE: After full recovery we run sync op immediately. We achieve this by setting
            //       SchTime to 'now' in ApplyChanges and running sync op below
            //       (SchTime <= now is always true):
            while (!SchedulerQueue.empty() &&
                   SchedulerQueue.top()->Get().PeerSyncState.SchTime <= now) {
                TVDiskInfoPtr info = SchedulerQueue.top();
                SchedulerQueue.pop();
                Y_DEBUG_ABORT_UNLESS(info->Get().PeerSyncState.LastSyncStatus != TSyncStatusVal::Running);
                StartSyncJob(ctx, info);
            }

            if (!SchedulerQueue.empty() && !Scheduled) {
                TDuration waitTime = SchedulerQueue.top()->Get().PeerSyncState.SchTime - now;
                Scheduled = true;
                ctx.Schedule(waitTime, new TEvents::TEvWakeup());
            }
        }

        void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
            Y_DEBUG_ABORT_UNLESS(ev->Get()->SubRequestId == TDbMon::SyncerInfoId);
            // create an actor to handle request
            auto actor = std::make_unique<TSyncerSchedulerHttpActor>(SyncerContext, GInfo, SyncerData, ev, ctx.SelfID);
            auto aid = ctx.RegisterWithSameMailbox(actor.release());
            ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        }

        void Handle(TEvLocalStatus::TPtr &ev, const TActorContext &ctx) {
            std::unique_ptr<TEvLocalStatusResult> result(new TEvLocalStatusResult());
            NKikimrBlobStorage::TSyncerStatus *rec = result->Record.MutableSyncerStatus();
            rec->SetPhase(NKikimrBlobStorage::TSyncerStatus::PhaseStandardMode);
            for (const auto &x: *SyncerData->Neighbors) {
                NKikimrBlobStorage::TSyncState *st = rec->AddSyncState();
                SyncStateFromSyncState(x.Get().PeerSyncState.SyncState, st);
            }
            ctx.Send(ev->Sender, result.release());
        }

        void Handle(NPDisk::TEvCutLog::TPtr &ev, const TActorContext &ctx) {
            ctx.Send(ev->Forward(CommitterId));
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        // BlobStorage Group reconfiguration
        void Handle(TEvVGenerationChange::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            auto *msg = ev->Get();
            GInfo = msg->NewInfo;
            JobCtx = TSjCtx::Create(SyncerContext, GInfo);
        }

        void Handle(TEvents::TEvGone::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ctx);
            ActiveActors.Erase(ev->Sender);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvSyncerJobDone, Handle)
            HFunc(TEvSyncerCommitProxyDone, Handle)
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvLocalStatus, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(TEvVGenerationChange, Handle)
            HFunc(TEvents::TEvGone, Handle)
            HFunc(TEvPrivate::TEvFullSyncGatherTimeout, HandleFullSyncGatherTimeout)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
        )

        STRICT_STFUNC(NothingToDo,
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvLocalStatus, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(TEvVGenerationChange, Handle)
            HFunc(TEvents::TEvGone, Handle)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_SYNCER_SCHEDULER;
        }

        TSyncerScheduler(const TIntrusivePtr<TSyncerContext> &sc,
                         const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                         const TIntrusivePtr<TSyncerData> &syncerData,
                         const TActorId &committerId)
            : TActorBootstrapped<TSyncerScheduler>()
            , SyncerContext(sc)
            , GInfo(info)
            , SyncerData(syncerData)
            , SchedulerQueue()
            , ActiveActors()
            , SyncTimeInterval(SyncerContext->Config->SyncTimeInterval)
            , CommitterId(committerId)
            , Scheduled(false)
            , JobCtx(TSjCtx::Create(SyncerContext, GInfo))
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // SYNCER ACTOR CREATOR
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateSyncerSchedulerActor(const TIntrusivePtr<TSyncerContext> &sc,
                                       const TIntrusivePtr<TBlobStorageGroupInfo> &info,
                                       const TIntrusivePtr<TSyncerData> &syncerData,
                                       const TActorId &committerId) {
        return new TSyncerScheduler(sc, info, syncerData, committerId);
    }

} // NKikimr
