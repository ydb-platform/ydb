#pragma once

#include <atomic>
#include <memory>

#include "kqp_node_state.h"

#include <ydb/library/actors/core/actor.h>

#include <ydb/core/kqp/compute_actor/kqp_compute_actor_factory.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>

namespace NKikimr::NKqp {

NActors::IActor* CreateKqpQueryManager(TIntrusivePtr<TKqpCounters>& counters, std::shared_ptr<TNodeState>& state,
    std::shared_ptr<NRm::IKqpResourceManager>& resourceManager, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory,
    bool enableSmallComputeMemoryAllocations, bool enableChannelMemoryTracking);

// Per query (per tx on a node), IS THREAD SAFE: the only path between the query's tx and the resource manager.
// The start reservation and every allocation and free of the task and channel quota managers go through it, so it
// knows how much the query holds: Memory + ExternalMemory, see GetAllocatedMemory(). A pure pass-through for now,
// it never refuses on its own. Called from compute actors and from the channel service (under its locks), must stay
// lock-free and must not call actors.
class TQueryQuotaManager final : public NYql::NDq::IMemoryQuotaManager {
public:
    explicit TQueryQuotaManager(TIntrusivePtr<NRm::TTxState> tx);

    // taskId and the request are forwarded as is: resource manager logs, fail reasons and broker task names are kept
    NRm::TKqpRMAllocateResult AllocateResources(ui64 taskId, const NRm::TKqpResourcesRequest& resources);
    void FreeResources(ui64 taskId, const NRm::TKqpResourcesRequest& resources);

    // Query level Memory with taskId 0. isOptional is ignored: refusing optional requests in advance is up to
    // the caller, see GetMemoryAvailability()
    bool AllocateQuota(ui64 memorySize, bool isOptional) override;
    void FreeQuota(ui64 memorySize) override;
    ui64 GetCurrentQuota() const override;
    ui64 GetMaxMemorySize() const override;
    i64 GetMemoryAvailability() const override;
    TString MemoryConsumptionDetails() const override;

    // Memory + ExternalMemory currently held from the resource manager through this object
    ui64 GetAllocatedMemory() const;
    ui64 GetTxId() const;
    // Read only, e.g. for the pool of the tx: an allocation made directly on the tx is not seen by the counter
    const TIntrusivePtr<NRm::TTxState>& GetTx() const;

private:
    const TIntrusivePtr<NRm::TTxState> Tx;
    std::atomic<ui64> AllocatedMemory = 0;
    std::atomic<ui64> MaxAllocatedMemory = 0;
};

using TQueryQuotaManagerPtr = std::shared_ptr<TQueryQuotaManager>;

TQueryQuotaManagerPtr CreateQueryQuotaManager(TIntrusivePtr<NRm::TTxState> tx);

NYql::NDq::IMemoryQuotaManager::TPtr CreateTaskQuotaManager(TQueryQuotaManagerPtr queryQuotaManager,
    ui64 taskId, ui64 initialMemoryLimit);

NYql::NDq::IMemoryQuotaManager::TPtr CreateChannelQuotaManager(TQueryQuotaManagerPtr queryQuotaManager,
    ui64 initialMemoryLimit, ui64 allocationStep = 1_MB);


} // namespace NKikimr::NKqp
