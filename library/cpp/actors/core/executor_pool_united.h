#pragma once

#include "actorsystem.h"
#include "balancer.h"
#include "scheduler_queue.h"
#include "executor_pool_base.h"

#include <library/cpp/actors/util/unordered_cache.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/actors/util/cpu_load_log.h>
#include <library/cpp/actors/util/unordered_cache.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/noncopyable.h>

namespace NActors {
    class TMailboxTable;

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
        void SpecificScheduleActivation(ui32 activation) override;
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
