#include "executor_pool_io.h"
#include "actor.h"
#include "mailbox.h"
#include <library/cpp/actors/util/affinity.h>
#include <library/cpp/actors/util/datetime.h>

namespace NActors {
    TIOExecutorPool::TIOExecutorPool(ui32 poolId, ui32 threads, const TString& poolName, TAffinity* affinity, ui32 maxActivityType)
        : TExecutorPoolBase(poolId, threads, affinity, maxActivityType)
        , Threads(new TThreadCtx[threads])
        , PoolName(poolName)
    {}

    TIOExecutorPool::TIOExecutorPool(const TIOExecutorPoolConfig& cfg)
        : TIOExecutorPool(
            cfg.PoolId,
            cfg.Threads,
            cfg.PoolName,
            new TAffinity(cfg.Affinity),
            cfg.MaxActivityType
        )
    {}

    TIOExecutorPool::~TIOExecutorPool() {
        Threads.Destroy();
        while (ThreadQueue.Pop(0))
            ;
    }

    ui32 TIOExecutorPool::GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) {
        ui32 workerId = wctx.WorkerId;
        Y_VERIFY_DEBUG(workerId < PoolThreads);

        NHPTimer::STime elapsed = 0;
        NHPTimer::STime parked = 0;
        NHPTimer::STime hpstart = GetCycleCountFast();
        NHPTimer::STime hpnow;

        const TAtomic x = AtomicDecrement(Semaphore);
        if (x < 0) {
            TThreadCtx& threadCtx = Threads[workerId];
            ThreadQueue.Push(workerId + 1, revolvingCounter);
            hpnow = GetCycleCountFast();
            elapsed += hpnow - hpstart;
            if (threadCtx.Pad.Park())
                return 0;
            hpstart = GetCycleCountFast();
            parked += hpstart - hpnow;
        }

        while (!RelaxedLoad(&StopFlag)) {
            if (const ui32 activation = Activations.Pop(++revolvingCounter)) {
                hpnow = GetCycleCountFast();
                elapsed += hpnow - hpstart;
                wctx.AddElapsedCycles(ActorSystemIndex, elapsed);
                if (parked > 0) {
                    wctx.AddParkedCycles(parked);
                }
                return activation;
            }
            SpinLockPause();
        }

        return 0;
    }

    void TIOExecutorPool::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Schedule(deadline - ActorSystem->Timestamp(), ev, cookie, workerId);
    }

    void TIOExecutorPool::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_UNUSED(workerId);

        const auto current = ActorSystem->Monotonic();
        if (deadline < current)
            deadline = current;

        TTicketLock::TGuard guard(&ScheduleLock);
        ScheduleQueue->Writer.Push(deadline.MicroSeconds(), ev.Release(), cookie);
    }

    void TIOExecutorPool::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_UNUSED(workerId);
        const auto deadline = ActorSystem->Monotonic() + delta;

        TTicketLock::TGuard guard(&ScheduleLock);
        ScheduleQueue->Writer.Push(deadline.MicroSeconds(), ev.Release(), cookie);
    }

    void TIOExecutorPool::ScheduleActivationEx(ui32 activation, ui64 revolvingWriteCounter) {
        Activations.Push(activation, revolvingWriteCounter);
        const TAtomic x = AtomicIncrement(Semaphore);
        if (x <= 0) {
            for (;; ++revolvingWriteCounter) {
                if (const ui32 x = ThreadQueue.Pop(revolvingWriteCounter)) {
                    const ui32 threadIdx = x - 1;
                    Threads[threadIdx].Pad.Unpark();
                    return;
                }
                SpinLockPause();
            }
        }
    }

    void TIOExecutorPool::Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) {
        TAffinityGuard affinityGuard(Affinity());

        ActorSystem = actorSystem;

        ScheduleQueue.Reset(new NSchedulerQueue::TQueueType());

        for (ui32 i = 0; i != PoolThreads; ++i) {
            Threads[i].Thread.Reset(new TExecutorThread(i, 0, actorSystem, this, MailboxTable.Get(), PoolName));
        }

        *scheduleReaders = &ScheduleQueue->Reader;
        *scheduleSz = 1;
    }

    void TIOExecutorPool::Start() {
        TAffinityGuard affinityGuard(Affinity());

        for (ui32 i = 0; i != PoolThreads; ++i)
            Threads[i].Thread->Start();
    }

    void TIOExecutorPool::PrepareStop() {
        AtomicStore(&StopFlag, true);
        for (ui32 i = 0; i != PoolThreads; ++i)
            Threads[i].Pad.Interrupt();
    }

    void TIOExecutorPool::Shutdown() {
        for (ui32 i = 0; i != PoolThreads; ++i)
            Threads[i].Thread->Join();
    }

    void TIOExecutorPool::GetCurrentStats(TExecutorPoolStats& /*poolStats*/, TVector<TExecutorThreadStats>& statsCopy) const {
        statsCopy.resize(PoolThreads + 1);
        // Save counters from the pool object
        statsCopy[0] = TExecutorThreadStats();
        statsCopy[0].Aggregate(Stats);
        // Per-thread stats
        for (size_t i = 0; i < PoolThreads; ++i) {
            Threads[i].Thread->GetCurrentStats(statsCopy[i + 1]);
        }
    }

    TString TIOExecutorPool::GetName() const {
        return PoolName;
    }
}
