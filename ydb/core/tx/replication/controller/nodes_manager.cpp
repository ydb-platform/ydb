#include "nodes_manager.h"
#include "private_events.h"

#include <ydb/core/tx/replication/service/service.h>

#include <util/generic/vector.h>
#include <util/random/random.h>

namespace NKikimr::NReplication::NController {

bool TNodesManager::HasTenant(const TString& tenant) const {
    return TenantNodes.contains(tenant);
}

bool TNodesManager::HasNodes(const TString& tenant) const {
    return !GetNodes(tenant).empty();
}

const THashSet<ui32>& TNodesManager::GetNodes(const TString& tenant) const {
    Y_ABORT_UNLESS(HasTenant(tenant));
    return TenantNodes.at(tenant);
}

ui32 TNodesManager::GetRandomNode(const TString& tenant) const {
    const auto& nodes = GetNodes(tenant);
    TVector<ui32> nodesVec(nodes.begin(), nodes.end());
    return nodesVec[RandomNumber(nodesVec.size())];
}

void TNodesManager::DiscoverNodes(const TString& tenant, const TActorId& cache, const TActorContext& ctx) {
    TenantNodes.emplace(tenant, THashSet<ui32>());
    NodeDiscoverers.emplace(
        ctx.Register(CreateDiscoverer(&NService::MakeDiscoveryPath, tenant, ctx.SelfID, cache)), tenant
    );
}

TNodesManager::TProcessResult TNodesManager::ProcessResponse(TEvDiscovery::TEvDiscoveryData::TPtr& ev, const TActorContext& ctx) {
    Y_ABORT_UNLESS(ev->Get()->CachedMessageData);
    Y_ABORT_UNLESS(!ev->Get()->CachedMessageData->InfoEntries.empty());
    Y_ABORT_UNLESS(ev->Get()->CachedMessageData->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok);

    TProcessResult result;

    auto it = NodeDiscoverers.find(ev->Sender);
    if (it == NodeDiscoverers.end()) {
        return result;
    }

    THashSet<ui32> newNodes;
    auto& curNodes = TenantNodes[it->second];

    for (const auto& [actorId, _] : ev->Get()->CachedMessageData->InfoEntries) {
        const ui32 nodeId = actorId.NodeId();
        newNodes.insert(nodeId);
        auto it = curNodes.find(nodeId);
        if (it != curNodes.end()) {
            curNodes.erase(it);
        } else {
            result.NewNodes.insert(nodeId);
        }
    }

    result.RemovedNodes = std::move(curNodes);
    curNodes = std::move(newNodes);

    ctx.Schedule(UpdateInternal, new TEvPrivate::TEvUpdateTenantNodes(it->second));
    NodeDiscoverers.erase(it);

    return result;
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

