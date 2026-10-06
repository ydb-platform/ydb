#pragma once

#include <memory>

#include "kqp_node_state.h"

#include <ydb/library/actors/core/actor.h>

#include <ydb/core/kqp/compute_actor/kqp_compute_actor_factory.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_memory.h>

namespace NKikimr::NKqp {

NActors::IActor* CreateKqpQueryManager(TIntrusivePtr<TKqpCounters>& counters, std::shared_ptr<TNodeState>& state,
    std::shared_ptr<NRm::IKqpResourceManager>& resourceManager, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory,
    bool enableSmallComputeMemoryAllocations, bool enableChannelMemoryTracking);

// Per query (per tx on a node), IS THREAD SAFE, implemented by TQueryQuotaManager in the cpp: the mediator between the
// query's tx and the node resources - the execution units are taken from the resource manager, the memory from the
// compute scheduler (see NScheduler::TSchedulableMemory). It holds the execution units and the external memory of the
// started tasks, see AllocateTasks(); the task and channel quota managers keep it alive and take Memory from it through
// AllocateQuota() and FreeQuota(). GetCurrentQuota() is what the query holds: Memory + ExternalMemory. Keeps the
// statistics of the tx for the out-of-memory reports, see MemoryConsumptionDetails().
// Called from compute actors and from the channel service (under its locks), so it must never wait for an actor: its
// own state is atomics, and the resource manager calls it makes take short mutexes (resource manager, resource broker)
// and at most post events.
class IQueryQuotaManager : public NYql::NDq::IMemoryQuotaManager {
public:
    // The execution units and the external memory (M) of the tasks of a start request. The task and channel quota
    // managers use their part of the external memory as their initial limit. The elastic memory (E) is what the tasks
    // are expected to grow by - it's only the demand. What FreeTasks() does not return is returned when the query quota
    // manager dies
    virtual NRm::TKqpRMAllocateResult AllocateTasks(ui64 executionUnits, ui64 externalMemory, ui64 elasticMemory = 0) = 0;
    // A part of what AllocateTasks() took, e.g. the execution unit, the initial memory limit and the elastic memory of
    // a task whose compute actor terminated
    virtual void FreeTasks(ui64 executionUnits, ui64 externalMemory, ui64 elasticMemory = 0) = 0;
    virtual const TIntrusivePtr<NRm::TTxState>& GetTx() const = 0;
};

using TQueryQuotaManagerPtr = std::shared_ptr<IQueryQuotaManager>;

// Without the memory (no compute scheduler) the memory is only accounted in the statistics, but not limited.
// TODO: taskMemory is a stub - the footprint of a compute actor outside of its MKQL quota isn't measured, so a fixed
//       amount of memory is charged for every execution unit instead.
TQueryQuotaManagerPtr CreateQueryQuotaManager(TIntrusivePtr<NRm::TTxState> tx, NScheduler::TSchedulableMemoryPtr memory,
    ui64 taskMemory);

// initialMemoryLimit is the part of the external memory of the query quota manager the task starts with
NYql::NDq::IMemoryQuotaManager::TPtr CreateTaskQuotaManager(NYql::NDq::IMemoryQuotaManager::TPtr queryQuotaManager,
    ui64 initialMemoryLimit, ui64 allocationStep = 1_MB);

// initialMemoryLimit is the part of the external memory of the query quota manager the channels start with
NYql::NDq::IMemoryQuotaManager::TPtr CreateChannelQuotaManager(NYql::NDq::IMemoryQuotaManager::TPtr queryQuotaManager,
    ui64 initialMemoryLimit, ui64 allocationStep = 1_MB);


} // namespace NKikimr::NKqp
