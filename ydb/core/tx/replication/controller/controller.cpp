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
        HFunc(TEvController::TEvDropReplication, Handle);
        HFunc(TEvPrivate::TEvDropReplication, Handle);
        HFunc(TEvPrivate::TEvDiscoveryTargetsResult, Handle);
        HFunc(TEvPrivate::TEvAssignStreamName, Handle);
        HFunc(TEvPrivate::TEvCreateStreamResult, Handle);
        HFunc(TEvPrivate::TEvDropStreamResult, Handle);
        HFunc(TEvPrivate::TEvCreateDstResult, Handle);
        HFunc(TEvPrivate::TEvDropDstResult, Handle);
        HFunc(TEvPrivate::TEvResolveTenantResult, Handle);
        HFunc(TEvPrivate::TEvUpdateTenantNodes, Handle);
        HFunc(TEvDiscovery::TEvDiscoveryData, Handle);
        HFunc(TEvDiscovery::TEvError, Handle);
        HFunc(TEvService::TEvStatus, Handle);
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

void TController::Handle(TEvController::TEvDropReplication::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxDropReplication(ev, ctx);
}

void TController::Handle(TEvPrivate::TEvDropReplication::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxDropReplication(ev, ctx);
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

void TController::Handle(TEvPrivate::TEvDropDstResult::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    RunTxDropDstResult(ev, ctx);
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
    Sessions.emplace(nodeId, TSessionInfo{});

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

    auto it = Sessions.find(nodeId);
    Y_ABORT_UNLESS(it != Sessions.end());
    Sessions.erase(it);

    if (SelfId().NodeId() != nodeId) {
        Send(ctx.InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
    }
}

void TController::Handle(TEvService::TEvStatus::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    // TODO
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
