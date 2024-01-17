#include "blobstorage_syncer_scheduler.h"
#include "blobstorage_syncer_localwriter.h"
#include "blobstorage_syncer_committer.h"
#include "syncer_job_task.h"
#include "syncer_job_actor.h"
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
            ctx.Send(NotifyId, new TEvents::TEvActorDied());
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
            Y_ABORT_UNLESS(Ev->Get()->SubRequestId == TDbMon::SyncerInfoId);
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // TEvSyncerCommitProxyDone
    ////////////////////////////////////////////////////////////////////////////
    struct TEvSyncerCommitProxyDone :
        public TEventLocal<TEvSyncerCommitProxyDone, TEvBlobStorage::EvSyncerCommitProxyDone>
    {
        std::unique_ptr<TSyncerJobTask> Task;

        TEvSyncerCommitProxyDone(std::unique_ptr<TSyncerJobTask> task)
            : Task(std::move(task))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSyncerCommitterProxy
    ////////////////////////////////////////////////////////////////////////////
    class TSyncerCommitterProxy : public TActorBootstrapped<TSyncerCommitterProxy> {
        friend class TActorBootstrapped<TSyncerCommitterProxy>;

        const TActorId NotifyId;
        const TActorId CommitterId;
        std::unique_ptr<TSyncerJobTask> Task;

        void Bootstrap(const TActorContext &ctx) {
            auto msg = TEvSyncerCommit::Remote(Task->VDiskId, Task->GetCurrent());
            ctx.Send(CommitterId, msg.release());
            TThis::Become(&TThis::StateFunc);
        }

        void Handle(TEvSyncerCommitDone::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ctx.Send(NotifyId, new TEvSyncerCommitProxyDone(std::move(Task)));
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
                              std::unique_ptr<TSyncerJobTask> task)
            : TActorBootstrapped<TSyncerCommitterProxy>()
            , NotifyId(notifyId)
            , CommitterId(committerId)
            , Task(std::move(task))
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

        TIntrusivePtr<TSyncerContext> SyncerContext;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TIntrusivePtr<TSyncerData> SyncerData;
        TSchedulerQueue SchedulerQueue;
        TActiveActors ActiveActors;
        const TDuration SyncTimeInterval;
        TActorId CommitterId;
        bool Scheduled;
        std::shared_ptr<TSjCtx> JobCtx;

        friend class TActorBootstrapped<TSyncerScheduler>;


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

        void ApplyChanges(const TActorContext &ctx, TSyncerJobTask& task) {
            SyncerData->Neighbors->ApplyChanges(ctx, &task, SyncerContext->Config->SyncTimeInterval);
            ActualizeUnsyncedDisksNum();
            SchedulerQueue.push(&(*SyncerData->Neighbors)[task.VDiskId]);
            Schedule(ctx);
        }

        void Handle(TEvSyncerJobDone::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            TEvSyncerJobDone *msg = ev->Get();

            if (msg->Task->NeedCommit()) {
                auto proxy = std::make_unique<TSyncerCommitterProxy>(ctx.SelfID, CommitterId, std::move(msg->Task));
                const TActorId aid = ctx.Register(proxy.release());
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
            } else {
                ApplyChanges(ctx, *msg->Task);
            }
        }

        void Handle(TEvSyncerCommitProxyDone::TPtr &ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            TEvSyncerCommitProxyDone *msg = ev->Get();
            ApplyChanges(ctx, *msg->Task);
        }

        void Schedule(const TActorContext &ctx) {
            Become(&TThis::StateFunc);

            TInstant now = TAppData::TimeProvider->Now();
            // NOTE: After full recovery we run sync op immediately. We achieve this by setting
            //       SchTime to 'now' in ApplyChanges and running sync op below
            //       (SchTime <= now is always true):
            while (!SchedulerQueue.empty() &&
                   SchedulerQueue.top()->Get().PeerSyncState.SchTime <= now) {
                TVDiskInfoPtr tmp = SchedulerQueue.top();
                SchedulerQueue.pop();
                Y_DEBUG_ABORT_UNLESS(tmp->Get().PeerSyncState.LastSyncStatus != TSyncStatusVal::Running);
                auto task = std::make_unique<TSyncerJobTask>(TSyncerJobTask::EJustSync, GInfo->GetVDiskId(tmp->OrderNumber),
                    GInfo->GetActorId(tmp->OrderNumber), tmp->Get().PeerSyncState, JobCtx);
                const TActorId aid = ctx.Register(CreateSyncerJob(SyncerContext, std::move(task), ctx.SelfID));
                ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
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

        void Handle(TEvents::TEvActorDied::TPtr &ev, const TActorContext &ctx) {
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
            HFunc(TEvents::TEvActorDied, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
        )

        STRICT_STFUNC(NothingToDo,
            HFunc(NMon::TEvHttpInfo, Handle)
            HFunc(TEvLocalStatus, Handle)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            HFunc(NPDisk::TEvCutLog, Handle)
            HFunc(TEvVGenerationChange, Handle)
            HFunc(TEvents::TEvActorDied, Handle)
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
