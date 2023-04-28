#include "nodes_manager.h"
#include "private_events.h"

#include <ydb/core/tx/replication/service/service.h>

namespace NKikimr::NReplication::NController {

bool TNodesManager::HasTenant(const TString& tenant) const {
    return TenantNodes.contains(tenant);
}

const THashSet<ui32>& TNodesManager::GetNodes(const TString& tenant) const {
    Y_VERIFY(HasTenant(tenant));
    return TenantNodes.at(tenant);
}

void TNodesManager::DiscoverNodes(const TString& tenant, const TActorId& cache, const TActorContext& ctx) {
    TenantNodes.emplace(tenant, THashSet<ui32>());
    NodeDiscoverers.emplace(
        ctx.Register(CreateDiscoverer(&NService::MakeDiscoveryPath, tenant, ctx.SelfID, cache)), tenant
    );
}

void TNodesManager::ProcessResponse(TEvDiscovery::TEvDiscoveryData::TPtr& ev, const TActorContext& ctx) {
    Y_VERIFY(ev->Get()->CachedMessageData && ev->Get()->CachedMessageData->Info);
    Y_VERIFY(ev->Get()->CachedMessageData->Info->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok);

    auto it = NodeDiscoverers.find(ev->Sender);
    if (it == NodeDiscoverers.end()) {
        return;
    }

    auto& nodes = TenantNodes[it->second];
    nodes.clear();

    for (const auto& [actorId, _] : ev->Get()->CachedMessageData->Info->InfoEntries) {
        nodes.insert(actorId.NodeId());
    }

    ctx.Schedule(UpdateInternal, new TEvPrivate::TEvUpdateTenantNodes(it->second));
    NodeDiscoverers.erase(it);
}

void TNodesManager::ProcessResponse(TEvDiscovery::TEvError::TPtr& ev, const TActorContext& ctx) {
    auto it = NodeDiscoverers.find(ev->Sender);
    if (it == NodeDiscoverers.end()) {
        return;
    }

    ctx.Schedule(RetryInternal, new TEvPrivate::TEvUpdateTenantNodes(it->second));
    NodeDiscoverers.erase(it);
}

void TNodesManager::Shutdown(const TActorContext& ctx) {
    for (const auto& [actorId, _] : std::exchange(NodeDiscoverers, {})) {
        ctx.Send(actorId, new TEvents::TEvPoison());
    }
}

}

