#pragma once

#include "actorsystem.h"
#include "balancer.h"
#include "scheduler_queue.h"
#include "executor_pool_base.h"

#include <library/cpp/actors/util/unordered_cache.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/actors/util/unordered_cache.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/noncopyable.h>

namespace NActors {
    class TMailboxTable;

    class TUnitedWorkers: public TNonCopyable {
        struct TWorker;
        struct TPool;
        struct TCpu;

        size_t WorkerCount;
        TArrayHolder<TWorker> Workers; // indexed by WorkerId
        size_t PoolCount;
        TArrayHolder<TPool> Pools;  // indexed by PoolId, so may include not used (not united) pools
        size_t CpuCount;
        TArrayHolder<TCpu> Cpus; // indexed by CpuId, so may include not allocated CPUs

        IBalancer* Balancer; // external pool cpu balancer

        TUnitedWorkersConfig Config;
        TCpuAllocationConfig Allocation;

        volatile bool StopFlag = false;

    public:
        TUnitedWorkers(
            const TUnitedWorkersConfig& config,
            const TVector<TUnitedExecutorPoolConfig>& unitedPools,
            const TCpuAllocationConfig& allocation,
            IBalancer* balancer);
        ~TUnitedWorkers();
        void Prepare(TActorSystem* actorSystem, TVector<NSchedulerQueue::TReader*>& scheduleReaders);
        void Start();
        void PrepareStop();
        void Shutdown();

        bool IsStopped() const {
            return RelaxedLoad(&StopFlag);
        }

        TWorkerId GetWorkerCount() const {
            return WorkerCount;
        }

        // Returns thread id of a worker
        TThreadId GetWorkerThreadId(TWorkerId workerId) const;

        // Returns per worker schedule writers
        NSchedulerQueue::TWriter* GetScheduleWriter(TWorkerId workerId) const;

        // Sets executor for specified pool
        void SetupPool(TPoolId pool, IExecutorPool* executorPool, TMailboxTable* mailboxTable);

        // Add activation of newly scheduled mailbox and wake cpu to execute it if required
        void PushActivation(TPoolId pool, ui32 activation, ui64 revolvingCounter);

        // Try acquire pending token. Must be done before execution
        bool TryAcquireToken(TPoolId pool);

        // Try to wake idle cpu waiting for tokens on specified pool
        void TryWake(TPoolId pool);

        // Get activation from pool; requires pool's token
        void BeginExecution(TPoolId pool, ui32& activation, ui64 revolvingCounter);

        // Stop currently active execution and start new one if token is available
        // NOTE: Reuses token if it's not destroyed
        bool NextExecution(TPoolId pool, ui32& activation, ui64 revolvingCounter);

        // Stop active execution
        void StopExecution(TPoolId pool);

        // Runs balancer to assign pools to cpus
        void Balance();

        // Returns pool to be executed by worker or `CpuShared`
        TPoolId AssignedPool(TWorkerContext& wctx);

        // Checks if balancer has assigned another pool for worker's cpu
        bool IsPoolReassigned(TWorkerContext& wctx);

        // Switch worker context into specified pool
        void SwitchPool(TWorkerContext& wctx, ui64 softDeadlineTs);

        // Wait for tokens from any pool allowed on specified cpu
        TPoolId Idle(TPoolId assigned, TWorkerContext& wctx);

        // Fill stats for specified pool
        void GetCurrentStats(TPoolId pool, TVector<TExecutorThreadStats>& statsCopy) const;

    private:
        TPoolId WaitSequence(TCpu& cpu, TWorkerContext& wctx, TTimeTracker& timeTracker);
    };

    class TUnitedExecutorPool: public TExecutorPoolBaseMailboxed {
        TUnitedWorkers* United;
        const TString PoolName;
        TAtomic ActivationsRevolvingCounter = 0;
    public:
        TUnitedExecutorPool(const TUnitedExecutorPoolConfig& cfg, TUnitedWorkers* united);

        void Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) override;
        void Start() override;
        void PrepareStop() override;
        void Shutdown() override;

        TAffinity* Affinity() const override;
        ui32 GetThreads() const override;
        ui32 GetReadyActivation(TWorkerContext& wctx, ui64 revolvingReadCounter) override;
        void ScheduleActivation(ui32 activation) override;
        void ScheduleActivationEx(ui32 activation, ui64 revolvingWriteCounter) override;
        void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;

        void GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const override; 

        TString GetName() const override {
            return PoolName;
        }
    };
}
