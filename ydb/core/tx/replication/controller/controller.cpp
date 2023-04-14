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
    Die(ctx);
}

void TController::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) {
    CLOG_T(ctx, "OnTabletDead");
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
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoison, Handle);
    default:
        return StateInitImpl(ev, SelfId());
    }
}

STFUNC(TController::StateZombie) {
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
        HFunc(TEvStateStorage::TEvBoardInfo, Handle);
        HFunc(TEvDiscovery::TEvError, Handle);
        HFunc(TEvents::TEvPoison, Handle);
    }
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
        Y_VERIFY(!tenant);
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

void TController::Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    NodesManager.ProcessResponse(ev, ctx);
}

void TController::Handle(TEvDiscovery::TEvError::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());
    NodesManager.ProcessResponse(ev, ctx);
}

void TController::Handle(TEvents::TEvPoison::TPtr& ev, const TActorContext& ctx) {
    CLOG_T(ctx, "Handle " << ev->Get()->ToString());

    for (auto& [_, replication] : Replications) {
        replication->Shutdown(ctx);
    }

    if (auto actorId = std::exchange(DiscoveryCache, {})) {
        Send(actorId, new TEvents::TEvPoison());
    }

    NodesManager.Shutdown(ctx);

    Send(Tablet(), new TEvents::TEvPoison());
    Become(&TThis::StateZombie);
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
