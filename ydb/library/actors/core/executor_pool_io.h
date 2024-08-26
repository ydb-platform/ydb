#pragma once

#include "actorsystem.h"
#include "executor_thread.h"
#include "executor_thread_ctx.h"
#include "scheduler_queue.h"
#include "executor_pool_base.h"
#include <ydb/library/actors/actor_type/indexes.h>
#include <ydb/library/actors/util/ticket_lock.h>
#include <ydb/library/actors/util/unordered_cache.h>
#include <ydb/library/actors/util/threadparkpad.h>
#include <util/system/condvar.h>

namespace NActors {
    struct TIOExecutorPoolConfig;

    class TIOExecutorPool: public TExecutorPoolBase {
        TArrayHolder<TExecutorThreadCtx> Threads;
        TUnorderedCache<ui32, 512, 4> ThreadQueue;

        THolder<NSchedulerQueue::TQueueType> ScheduleQueue;
        TTicketLock ScheduleLock;

        const TString PoolName;
        const ui32 ActorSystemIndex = NActors::TActorTypeOperator::GetActorSystemIndex();
    public:
        TIOExecutorPool(ui32 poolId, ui32 threads, const TString& poolName = "", TAffinity* affinity = nullptr);
        explicit TIOExecutorPool(const TIOExecutorPoolConfig& cfg);
        ~TIOExecutorPool();

        ui32 GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) override;

        void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;
        void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) override;

        void ScheduleActivationEx(ui32 activation, ui64 revolvingWriteCounter) override;

        void Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) override;
        void Start() override;
        void PrepareStop() override;
        void Shutdown() override;

        void GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const override;
        TString GetName() const override;
    };
}
