#pragma once

#include "dq_compute_memory_quota.h"
#include "dq_schedulable.h"
#include <ydb/library/yql/dq/task_scheduler/worker_pool.h>

namespace NYql::NDq {

// Shared by the task endpoint and workers. Compute context and channel adapters
// remain attached to the endpoint during the first PoC; the lease protects their
// lifetime and every access while the runner executes on a different mailbox.
class TDqTaskExecution final : public IDqTaskExecution, public std::enable_shared_from_this<TDqTaskExecution> {
public:
    TDqTaskExecution(TIntrusivePtr<IDqTaskRunner> runner, TDqMemoryQuota* memoryQuota,
        std::shared_ptr<IDqTaskScheduler> scheduler, std::shared_ptr<TTaskSchedulingHandle> task,
        IDqSchedulableWorkFactoryPtr workFactory, NActors::TActorSystem* actorSystem, NActors::TActorId endpoint);
    std::shared_ptr<const std::atomic<bool>> GetAvailability() const override { return Available; }
    bool IsAvailable() const override { return Available->load(std::memory_order_acquire); }
    bool TryAcquire() override;
    void Release() override;
    bool TryAcquireEndpoint();
    void Retire(bool completed); // endpoint lease must be held
    TTaskSliceResult RunSlice(const TTaskSliceBudget& budget) override;
    void OnSliceFinished(TTaskSliceResult result) override;

private:
    TIntrusivePtr<IDqTaskRunner> Runner;
    TDqMemoryQuota* const MemoryQuota;
    const std::shared_ptr<IDqTaskScheduler> Scheduler;
    const std::shared_ptr<TTaskSchedulingHandle> Task;
    std::unique_ptr<IDqSchedulableWork> Work;
    bool WaitingForQuota = false;
    NActors::TActorSystem* const ActorSystem;
    const NActors::TActorId Endpoint;
    const std::shared_ptr<std::atomic<bool>> Available = std::make_shared<std::atomic<bool>>(true);
    std::atomic<bool> EndpointWaiting = false;
};

} // namespace NYql::NDq
