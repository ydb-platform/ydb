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
        HFunc(TEvPrivate::TEvRunWorkers, Handle);
        HFunc(TEvDiscovery::TEvDiscoveryData, Handle);
        HFunc(TEvDiscovery::TEvError, Handle);
        HFunc(TEvService::TEvStatus, Handle);
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
            WorkersToRun.insert(id);
        }
    }

    Sessions.erase(nodeId);
    CloseSession(nodeId, ctx);
    ScheduleRunWorkers();
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

    for (const auto& workerIdentity : ev->Get()->Record.GetWorkers()) {
        const auto id = TWorkerId::Parse(workerIdentity);

        auto it = Workers.find(id);
        if (it == Workers.end()) {
            it = Workers.emplace(id, TWorkerInfo()).first;
        }

        auto& worker = it->second;
        if (worker.HasSession() && Sessions.contains(worker.GetSession())) {
            StopWorker(worker.GetSession(), id);
        }

        session.AttachWorker(id);
        worker.AttachSession(nodeId);
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

void TController::Handle(TEvService::TEvRunWorker::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());

    auto& record = ev->Get()->Record;
    const auto id = TWorkerId::Parse(record.GetWorker());
    auto* cmd = record.MutableCommand();

    auto it = Workers.find(id);
    if (it == Workers.end()) {
        it = Workers.emplace(id, TWorkerInfo(cmd)).first;
    }

    auto& worker = it->second;
    if (!worker.HasCommand()) {
        worker.SetCommand(cmd);
    }

    if (!worker.HasSession()) {
        WorkersToRun.insert(id);
    }

    ScheduleRunWorkers();
}

void TController::ScheduleRunWorkers() {
    if (RunWorkersScheduled || !WorkersToRun) {
        return;
    }

    Schedule(TDuration::MilliSeconds(100), new TEvPrivate::TEvRunWorkers());
    RunWorkersScheduled = true;
}

void TController::Handle(TEvPrivate::TEvRunWorkers::TPtr&, const TActorContext& ctx) {
    CLOG_D(ctx, "Run workers"
        << ": queue# " << WorkersToRun.size());

    static constexpr ui32 limit = 100;
    ui32 i = 0;

    for (auto iter = WorkersToRun.begin(); iter != WorkersToRun.end() && i < limit;) {
        const auto id = *iter;

        auto it = Workers.find(id);
        Y_ABORT_UNLESS(it != Workers.end());

        auto& worker = it->second;
        if (worker.HasSession()) {
            WorkersToRun.erase(iter++);
            continue;
        }

        auto replication = Find(id.ReplicationId());
        if (!replication) {
            WorkersToRun.erase(iter++);
            continue;
        }

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
        RunWorker(nodeId, id, *worker.GetCommand());
        worker.AttachSession(nodeId);

        WorkersToRun.erase(iter++);
        ++i;
    }

    RunWorkersScheduled = false;
    ScheduleRunWorkers();
}

void TController::RunWorker(ui32 nodeId, const TWorkerId& id, const NKikimrReplication::TRunWorkerCommand& cmd) {
    LOG_D("Run worker"
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

void TController::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const TActorContext& ctx) {
    const ui32 nodeId = ev->Get()->NodeId;

    CLOG_I(ctx, "Node disconnected"
        << ": nodeId# " << nodeId);

    if (Sessions.contains(nodeId)) {
        DeleteSession(nodeId, ctx);
    }
}

TReplication::TPtr TController::Find(ui64 id) {
    auto it = Replications.find(id);
    if (it == Replications.end()) {
        return nullptr;
    }

    return it->second;
}

TReplication::TPtr TController::Find(const TPathId& pathId) {
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
