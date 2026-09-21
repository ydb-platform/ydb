#include "internal.h"

#include <ydb/services/workload_manager/gateway.h>

#include <ydb/services/workload_manager/common/helpers.h>
#include <ydb/services/workload_manager/events.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>

#include <util/system/mutex.h>

#include <unordered_map>


namespace NKikimr::NWorkloadManager {

namespace NPrivate {

std::shared_ptr<IQueryClassifier> TWorkloadManagerGateway::TryCreateQueryClassifier(
    const TString& databaseId, TClassifyContext context)
{
    TSnapshotPtr snapshot;
    with_lock (Lock_) {
        snapshot = Snapshot_;
    }

    if (!snapshot || !snapshot->IsResourcePoolsEnabled(databaseId)) {
        return nullptr;
    }

    const TString effectivePoolId = context.PoolId
        ? context.PoolId
        : NResourcePool::DEFAULT_POOL_ID;

    if (!snapshot->Pools || !snapshot->Pools->contains(GetPoolKey(databaseId, effectivePoolId))) {
        NActors::TActivationContext::Send(new NActors::IEventHandle(
            NKqp::MakeKqpSchedulerServiceId(NodeId_),
            CacheActorId_,
            new NKqp::NScheduler::TEvAddPool(databaseId, effectivePoolId)));
        NActors::TActivationContext::Send(new NActors::IEventHandle(
            WorkloadManagerServiceId_,
            CacheActorId_,
            new TEvSubscribeOnPoolChanges(databaseId, effectivePoolId)));
    }

    return CreateQueryClassifier(
        snapshot->Pools,
        TClassifierConfigsView(snapshot->Classifiers, databaseId),
        databaseId,
        std::move(context),
        *AppData());
}

}

namespace {

struct TGateways {
    std::weak_ptr<NPrivate::TWorkloadManagerGateway> Default;
    TMutex Lock;
    std::unordered_map<ui32, std::weak_ptr<NPrivate::TWorkloadManagerGateway>> ByNodeId;
};

TGateways Gateways;

}

namespace NPrivate {

void RegisterGateway(std::shared_ptr<TWorkloadManagerGateway> gateway, ui32 nodeId) {
    with_lock (Gateways.Lock) {
        if (Gateways.Default.expired()) {
            Gateways.Default = gateway;
        }
        Gateways.ByNodeId[nodeId] = gateway;
    }
}

}

TGatewayPtr TryGetGateway(TMaybe<ui32> nodeId) {
    const ui32 resolvedNodeId = nodeId
        ? *nodeId
        : NActors::TActivationContext::ActorSystem()->NodeId;

    if (auto gw = Gateways.Default.lock(); Y_LIKELY(gw && gw->GetNodeId() == resolvedNodeId)) {
        return gw;
    }
    with_lock (Gateways.Lock) {
        const auto it = Gateways.ByNodeId.find(resolvedNodeId);
        if (it != Gateways.ByNodeId.end()) {
            return it->second.lock();
        }
    }
    return nullptr;
}

TGatewayPtr GetGateway(TMaybe<ui32> nodeId) {
    if (auto gw = TryGetGateway(nodeId)) {
        return gw;
    }
    const ui32 resolvedNodeId = nodeId
        ? *nodeId
        : NActors::TActivationContext::ActorSystem()->NodeId;
    Y_ABORT("WorkloadManager gateway not ready, node #%" PRIu32, resolvedNodeId);
}

}
