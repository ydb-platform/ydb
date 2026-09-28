#pragma once

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

// Per query (per tx on a node), IS THREAD SAFE, implemented by TQueryQuotaManager in the cpp: the only path between the
// query's tx and the resource manager. It holds the execution units and the external memory of the started tasks, see
// AllocateTasks(); the task and channel quota managers keep it alive and take Memory from it through AllocateQuota()
// and FreeQuota(). GetCurrentQuota() is what the query holds from the resource manager: Memory + ExternalMemory.
// Called from compute actors and from the channel service (under its locks), must stay lock-free and must not call
// actors.
class IQueryQuotaManager : public NYql::NDq::IMemoryQuotaManager {
public:
    // The execution units and the external memory of the tasks of a start request. The task and channel quota managers
    // use their part of the external memory as their initial limit. What FreeTasks() does not return is returned to
    // the resource manager when the query quota manager dies
    virtual NRm::TKqpRMAllocateResult AllocateTasks(ui64 executionUnits, ui64 externalMemory) = 0;
    // A part of what AllocateTasks() took, e.g. the execution unit and the initial memory limit of a task whose compute
    // actor terminated
    virtual void FreeTasks(ui64 executionUnits, ui64 externalMemory) = 0;
    virtual const TIntrusivePtr<NRm::TTxState>& GetTx() const = 0;
};

using TQueryQuotaManagerPtr = std::shared_ptr<IQueryQuotaManager>;

TQueryQuotaManagerPtr CreateQueryQuotaManager(TIntrusivePtr<NRm::TTxState> tx);

// initialMemoryLimit is the part of the external memory of the query quota manager the task starts with
NYql::NDq::IMemoryQuotaManager::TPtr CreateTaskQuotaManager(NYql::NDq::IMemoryQuotaManager::TPtr queryQuotaManager,
    ui64 initialMemoryLimit, ui64 allocationStep = 1_MB);

// initialMemoryLimit is the part of the external memory of the query quota manager the channels start with
NYql::NDq::IMemoryQuotaManager::TPtr CreateChannelQuotaManager(NYql::NDq::IMemoryQuotaManager::TPtr queryQuotaManager,
    ui64 initialMemoryLimit, ui64 allocationStep = 1_MB);


} // namespace NKikimr::NKqp
