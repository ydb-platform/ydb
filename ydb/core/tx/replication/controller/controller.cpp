#include "controller.h"
#include "controller_impl.h"

#include <ydb/core/discovery/discovery.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

namespace NKikimr::NReplication {

namespace NController {

TController::TController(const TActorId& tablet, TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    , LogPrefix(this)
{
}

void TController::OnDetach(const TActorContext& ctx) {
    CLOG_T(ctx, "OnDetach");
    Cleanup(ctx);
    Die(ctx);
}

void TController::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) {
    CLOG_T(ctx, "OnTabletDead");
    Cleanup(ctx);
    Die(ctx);
}

void TController::OnActivateExecutor(const TActorContext& ctx) {
    CLOG_T(ctx, "OnActivateExecutor");
    RunTxInitSchema(ctx);
}

void TController::DefaultSignalTabletActive(const TActorContext&) {
    // nop
}

STFUNC(TController::StateInit) {
    StateInitImpl(ev, SelfId());
}

STFUNC(TController::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvController::TEvCreateReplication, Handle);
        HFunc(TEvController::TEvAlterReplication, Handle);
        HFunc(TEvController::TEvDropReplication, Handle);
        HFunc(TEvController::TEvDescribeReplication, Handle);
        HFunc(TEvPrivate::TEvDropReplication, Handle);
        HFunc(TEvPrivate::TEvDiscoveryTargetsResult, Handle);
        HFunc(TEvPrivate::TEvAssignStreamName, Handle);
        HFunc(TEvPrivate::TEvCreateStreamResult, Handle);
        HFunc(TEvPrivate::TEvDropStreamResult, Handle);
        HFunc(TEvPrivate::TEvCreateDstResult, Handle);
        HFunc(TEvPrivate::TEvAlterDstResult, Handle);
        HFunc(TEvPrivate::TEvDropDstResult, Handle);
        HFunc(TEvPrivate::TEvResolveSecretResult, Handle);
        HFunc(TEvPrivate::TEvResolveTenantResult, Handle);
        HFunc(TEvPrivate::TEvUpdateTenantNodes, Handle);
        HFunc(TEvPrivate::TEvProcessQueues, Handle);
        HFunc(TEvPrivate::TEvRemoveWorker, Handle);
        HFunc(TEvPrivate::TEvDescribeTargetsResult, Handle);
        HFunc(TEvDiscovery::TEvDiscoveryData, Handle);
        HFunc(TEvDiscovery::TEvError, Handle);
        HFunc(TEvService::TEvStatus, Handle);
        HFunc(TEvService::TEvWorkerStatus, Handle);
        HFunc(TEvService::TEvRunWorker, Handle);
        HFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
    default:
        HandleDefaultEvents(ev, SelfId());
    }
}

void TController::Cleanup(const TActorContext& ctx) {
    for (auto& [_, replication] : Replications) {
        replication->Shutdown(ctx);
    }

    if (auto actorId = std::exchange(DiscoveryCache, {})) {
        Send(actorId, new TEvents::TEvPoison());
    }

    for (const auto& [nodeId, _] : Sessions) {
        CloseSession(nodeId, ctx);
    }

    NodesManager.Shutdown(ctx);
}

void TController::SwitchToWork(const TActorContext& ctx) {
    CLOG_T(ctx, "SwitchToWork");

    SignalTabletActive(ctx);
    Become(&TThis::StateWork);

    if (!DiscoveryCache) {
        DiscoveryCache = ctx.Register(CreateDiscoveryCache());
    }

    for (auto& [_, replication] : Replications) {
        replication->Progress(ctx);
    }
}

void TController::Reset() {
    SysParams.Reset();
    Replications.clear();
    ReplicationsByPathId.clear();
}

void TController::Handle(TEvController::TEvCreateReplication::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxCreateReplication(ev, ctx);
}

void TController::Handle(TEvController::TEvAlterReplication::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxAlterReplication(ev, ctx);
}

void TController::Handle(TEvController::TEvDropReplication::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxDropReplication(ev, ctx);
}

void TController::Handle(TEvPrivate::TEvDropReplication::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxDropReplication(ev, ctx);
}

void TController::Handle(TEvController::TEvDescribeReplication::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxDescribeReplication(ev, ctx);
}

void TController::Handle(TEvPrivate::TEvDescribeTargetsResult::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxDescribeReplication(ev, ctx);
}

void TController::Handle(TEvPrivate::TEvDiscoveryTargetsResult::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxDiscoveryTargetsResult(ev, ctx);
}

void TController::Handle(TEvPrivate::TEvAssignStreamName::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxAssignStreamName(ev, ctx);
}

void TController::Handle(TEvPrivate::TEvCreateStreamResult::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxCreateStreamResult(ev, ctx);
}

void TController::Handle(TEvPrivate::TEvDropStreamResult::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxDropStreamResult(ev, ctx);
}

void TController::Handle(TEvPrivate::TEvCreateDstResult::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxCreateDstResult(ev, ctx);
}

void TController::Handle(TEvPrivate::TEvAlterDstResult::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxAlterDstResult(ev, ctx);
}

void TController::Handle(TEvPrivate::TEvDropDstResult::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxDropDstResult(ev, ctx);
}

void TController::Handle(TEvPrivate::TEvResolveSecretResult::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxResolveSecretResult(ev, ctx);
}

void TController::Handle(TEvPrivate::TEvResolveTenantResult::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());

    const auto rid = ev->Get()->ReplicationId;
    const auto& tenant = ev->Get()->Tenant;

    auto replication = Find(rid);
    if (!replication) {
        CLOG_W(ctx, "Unknown replication"
            << ": rid# " << rid);
        return;
    }

    if (ev->Get()->IsSuccess()) {
        CLOG_N(ctx, "Tenant resolved"
            << ": rid# " << rid
            << ", tenant# " << tenant);

        if (!NodesManager.HasTenant(tenant)) {
            CLOG_I(ctx, "Discover tenant nodes"
                << ": tenant# " << tenant);
            NodesManager.DiscoverNodes(tenant, DiscoveryCache, ctx);
        }
    } else {
        CLOG_E(ctx, "Resolve tenant error"
            << ": rid# " << rid);
        Y_ABORT_UNLESS(!tenant);
    }

    replication->SetTenant(tenant);
    replication->Progress(ctx);
}

void TController::Handle(TEvPrivate::TEvUpdateTenantNodes::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());

    const auto& tenant = ev->Get()->Tenant;
    if (NodesManager.HasTenant(tenant)) {
        CLOG_I(ctx, "Discover tenant nodes"
            << ": tenant# " << tenant);
        NodesManager.DiscoverNodes(tenant, DiscoveryCache, ctx);
    }
}

void TController::Handle(TEvDiscovery::TEvDiscoveryData::TPtr& ev, const TActorContext& ctx) {
    Y_ABORT_UNLESS(ev->Get()->CachedMessageData);
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());

    auto result = NodesManager.ProcessResponse(ev, ctx);

    for (auto nodeId : result.NewNodes) {
        if (!Sessions.contains(nodeId)) {
            CreateSession(nodeId, ctx);
        }
    }

    for (auto nodeId : result.RemovedNodes) {
        if (Sessions.contains(nodeId)) {
            DeleteSession(nodeId, ctx);
        }
    }
}

void TController::Handle(TEvDiscovery::TEvError::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    NodesManager.ProcessResponse(ev, ctx);
}

void TController::CreateSession(ui32 nodeId, const TActorContext& ctx) {
    CLOG_D(ctx, "Create session"
        << ": nodeId# " << nodeId);

    Y_ABORT_UNLESS(!Sessions.contains(nodeId));
    Sessions.emplace(nodeId, TSessionInfo());

    auto ev = MakeHolder<TEvService::TEvHandshake>(TabletID(), Executor()->Generation());
    ui32 flags = 0;
    if (SelfId().NodeId() != nodeId) {
        flags = IEventHandle::FlagSubscribeOnSession;
    }

    Send(MakeReplicationServiceId(nodeId), std::move(ev), flags);
}

void TController::DeleteSession(ui32 nodeId, const TActorContext& ctx) {
    CLOG_D(ctx, "Delete session"
        << ": nodeId# " << nodeId);

    Y_ABORT_UNLESS(Sessions.contains(nodeId));
    auto& session = Sessions[nodeId];

    for (const auto& id : session.GetWorkers()) {
        auto it = Workers.find(id);
        if (it == Workers.end()) {
            continue;
        }

        auto& worker = it->second;
        worker.ClearSession();

        if (worker.HasCommand()) {
            BootQueue.insert(id);
        }
    }

    Sessions.erase(nodeId);
    CloseSession(nodeId, ctx);
    ScheduleProcessQueues();
}

void TController::CloseSession(ui32 nodeId, const TActorContext& ctx) {
    CLOG_T(ctx, "Close session"
        << ": nodeId# " << nodeId);

    if (SelfId().NodeId() != nodeId) {
        Send(ctx.InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
    }
}

void TController::Handle(TEvService::TEvStatus::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());

    const auto nodeId = ev->Sender.NodeId();
    if (!Sessions.contains(nodeId)) {
        return;
    }

    auto& session = Sessions[nodeId];
    session.SetReady();

    for (const auto& protoId : ev->Get()->Record.GetWorkers()) {
        const auto id = TWorkerId::Parse(protoId);
        if (!IsValidWorker(id)) {
            StopQueue.emplace(id, nodeId);
            continue;
        }

        auto* worker = GetOrCreateWorker(id);
        if (worker->HasSession()) {
            if (const auto sessionId = worker->GetSession(); sessionId != nodeId) {
                Y_ABORT_UNLESS(Sessions.contains(sessionId));
                Sessions[sessionId].DetachWorker(id);
                StopQueue.emplace(id, sessionId);
            }
        }

        session.AttachWorker(id);
        worker->AttachSession(nodeId);
    }

    ScheduleProcessQueues();
}

void TController::Handle(TEvService::TEvWorkerStatus::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());

    const auto nodeId = ev->Sender.NodeId();
    if (!Sessions.contains(nodeId)) {
        return;
    }

    auto& session = Sessions[nodeId];
    const auto& record = ev->Get()->Record;
    const auto id = TWorkerId::Parse(record.GetWorker());

    switch (record.GetStatus()) {
    case NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING:
        if (!session.HasWorker(id)) {
            StopQueue.emplace(id, nodeId);
        } else if (record.GetReason() == NKikimrReplication::TEvWorkerStatus::REASON_INFO) {
            UpdateLag(id, TDuration::MilliSeconds(record.GetLagMilliSeconds()));
        }
        break;
    case NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED:
        if (!MaybeRemoveWorker(id, ctx)) {
            if (record.GetReason() == NKikimrReplication::TEvWorkerStatus::REASON_ERROR) {
                RunTxWorkerError(id, record.GetErrorDescription(), ctx);
            } else {
                session.DetachWorker(id);
                if (IsValidWorker(id)) {
                    auto* worker = GetOrCreateWorker(id);
                    worker->ClearSession();
                    if (worker->HasCommand()) {
                        BootQueue.insert(id);
                    }
                }
            }
        }
        break;
    default:
        CLOG_W(ctx, "Unknown worker status"
            << ": value# " << static_cast<int>(record.GetStatus()));
        break;
    }

    ScheduleProcessQueues();
}

void TController::UpdateLag(const TWorkerId& id, TDuration lag) {
    auto replication = Find(id.ReplicationId());
    if (!replication) {
        return;
    }

    auto* target = replication->FindTarget(id.TargetId());
    if (!target) {
        return;
    }

    target->UpdateLag(id.WorkerId(), lag);
}

void TController::Handle(TEvService::TEvRunWorker::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());

    auto& record = ev->Get()->Record;
    const auto id = TWorkerId::Parse(record.GetWorker());
    auto* cmd = record.MutableCommand();

    if (!IsValidWorker(id)) {
        return;
    }

    auto* worker = GetOrCreateWorker(id, cmd);
    if (!worker->HasCommand()) {
        worker->SetCommand(cmd);
    }

    if (!worker->HasSession()) {
        BootQueue.insert(id);
    }

    ScheduleProcessQueues();
}

bool TController::IsValidWorker(const TWorkerId& id) const {
    auto replication = Find(id.ReplicationId());
    if (!replication) {
        return false;
    }

    if (replication->GetState() != TReplication::EState::Ready) {
        return false;
    }

    auto* target = replication->FindTarget(id.TargetId());
    if (!target) {
        return false;
    }

    if (target->GetDstState() != TReplication::EDstState::Ready) {
        return false;
    }

    if (target->GetStreamState() != TReplication::EStreamState::Ready) {
        return false;
    }

    return true;
}

TWorkerInfo* TController::GetOrCreateWorker(const TWorkerId& id, NKikimrReplication::TRunWorkerCommand* cmd) {
    auto it = Workers.find(id);
    if (it == Workers.end()) {
        it = Workers.emplace(id, cmd).first;
    }

    auto replication = Find(id.ReplicationId());
    Y_ABORT_UNLESS(replication);

    auto* target = replication->FindTarget(id.TargetId());
    Y_ABORT_UNLESS(target);

    target->AddWorker(id.WorkerId());
    return &it->second;
}

void TController::ScheduleProcessQueues() {
    if (ProcessQueuesScheduled || (!BootQueue && !StopQueue)) {
        return;
    }

    Schedule(TDuration::MilliSeconds(100), new TEvPrivate::TEvProcessQueues());
    ProcessQueuesScheduled = true;
}

void TController::Handle(TEvPrivate::TEvProcessQueues::TPtr&, const TActorContext& ctx) {
    CLOG_D(ctx, "Process queues"
        << ": boot# " << BootQueue.size()
        << ": stop# " << StopQueue.size());

    ProcessBootQueue(ctx);
    ProcessStopQueue(ctx);

    ProcessQueuesScheduled = false;
    ScheduleProcessQueues();
}

void TController::ProcessBootQueue(const TActorContext&) {
    ui32 i = 0;
    for (auto iter = BootQueue.begin(); iter != BootQueue.end() && i < ProcessBatchLimit;) {
        const auto id = *iter;
        if (!IsValidWorker(id)) {
            BootQueue.erase(iter++);
            continue;
        }

        auto it = Workers.find(id);
        if (it == Workers.end()) {
            BootQueue.erase(iter++);
            continue;
        }

        auto& worker = it->second;
        if (worker.HasSession()) {
            BootQueue.erase(iter++);
            continue;
        }

        auto replication = Find(id.ReplicationId());
        Y_ABORT_UNLESS(replication);

        const auto& tenant = replication->GetTenant();
        if (!tenant || !NodesManager.HasTenant(tenant) || !NodesManager.HasNodes(tenant)) {
            ++iter;
            continue;
        }

        const auto nodeId = NodesManager.GetRandomNode(tenant);
        if (!Sessions.contains(nodeId) || !Sessions[nodeId].IsReady()) {
            ++iter;
            continue;
        }

        Y_ABORT_UNLESS(worker.HasCommand());
        BootWorker(nodeId, id, *worker.GetCommand());
        worker.AttachSession(nodeId);

        BootQueue.erase(iter++);
        ++i;
    }
}

void TController::BootWorker(ui32 nodeId, const TWorkerId& id, const NKikimrReplication::TRunWorkerCommand& cmd) {
    LOG_D("Boot worker"
        << ": nodeId# " << nodeId
        << ", workerId# " << id);

    Y_ABORT_UNLESS(Sessions.contains(nodeId));
    auto& session = Sessions[nodeId];

    auto ev = MakeHolder<TEvService::TEvRunWorker>();
    auto& record = ev->Record;

    auto& controller = *record.MutableController();
    controller.SetTabletId(TabletID());
    controller.SetGeneration(Executor()->Generation());
    id.Serialize(*record.MutableWorker());
    record.MutableCommand()->CopyFrom(cmd);

    Send(MakeReplicationServiceId(nodeId), std::move(ev));
    session.AttachWorker(id);
}

void TController::ProcessStopQueue(const TActorContext& ctx) {
    ui32 i = 0;
    for (auto iter = StopQueue.begin(); iter != StopQueue.end() && i < ProcessBatchLimit;) {
        const auto& id = iter->first;
        auto sessionId = iter->second;

        if (!Sessions.contains(sessionId) || !Sessions[sessionId].IsReady()) {
            MaybeRemoveWorker(id, ctx);
            StopQueue.erase(iter++);
            continue;
        }

        StopWorker(sessionId, id);

        StopQueue.erase(iter++);
        ++i;
    }
}

void TController::StopWorker(ui32 nodeId, const TWorkerId& id) {
    LOG_D("Stop worker"
        << ": nodeId# " << nodeId
        << ", workerId# " << id);

    Y_ABORT_UNLESS(Sessions.contains(nodeId));
    auto& session = Sessions[nodeId];

    auto ev = MakeHolder<TEvService::TEvStopWorker>();
    auto& record = ev->Record;

    auto& controller = *record.MutableController();
    controller.SetTabletId(TabletID());
    controller.SetGeneration(Executor()->Generation());
    id.Serialize(*record.MutableWorker());

    Send(MakeReplicationServiceId(nodeId), std::move(ev));
    session.DetachWorker(id);
}

void TController::Handle(TEvPrivate::TEvRemoveWorker::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());

    const auto& id = ev->Get()->Id;
    RemoveQueue.insert(id);

    auto it = Workers.find(id);
    if (it == Workers.end()) {
        return RemoveWorker(id, ctx);
    }

    auto& worker = it->second;
    if (!worker.HasSession()) {
        return RemoveWorker(id, ctx);
    }

    StopQueue.emplace(id, worker.GetSession());
    ScheduleProcessQueues();
}

void TController::RemoveWorker(const TWorkerId& id, const TActorContext& ctx) {
    LOG_D("Remove worker"
        << ": workerId# " << id);

    Y_ABORT_UNLESS(RemoveQueue.contains(id));

    RemoveQueue.erase(id);
    Workers.erase(id);

    auto replication = Find(id.ReplicationId());
    if (!replication) {
        return;
    }

    auto* target = replication->FindTarget(id.TargetId());
    if (!target) {
        return;
    }

    target->RemoveWorker(id.WorkerId());
    target->Progress(ctx);
}

bool TController::MaybeRemoveWorker(const TWorkerId& id, const TActorContext& ctx) {
    if (!RemoveQueue.contains(id)) {
        return false;
    }

    RemoveWorker(id, ctx);
    return true;
}

void TController::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const TActorContext& ctx) {
    const ui32 nodeId = ev->Get()->NodeId;

    CLOG_I(ctx, "Node disconnected"
        << ": nodeId# " << nodeId);

    if (Sessions.contains(nodeId)) {
        DeleteSession(nodeId, ctx);
    }
}

TReplication::TPtr TController::Find(ui64 id) const {
    auto it = Replications.find(id);
    if (it == Replications.end()) {
        return nullptr;
    }

    return it->second;
}

TReplication::TPtr TController::Find(const TPathId& pathId) const {
    auto it = ReplicationsByPathId.find(pathId);
    if (it == ReplicationsByPathId.end()) {
        return nullptr;
    }

    return it->second;
}

void TController::Remove(ui64 id) {
    auto it = Replications.find(id);
    if (it == Replications.end()) {
        return;
    }

    ReplicationsByPathId.erase(it->second->GetPathId());
    Replications.erase(it);
}

} // NController

IActor* CreateController(const TActorId& tablet, TTabletStorageInfo* info) {
    return new NController::TController(tablet, info);
}

}
