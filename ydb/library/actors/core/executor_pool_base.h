#pragma once

#include "executor_pool.h"
#include "executor_thread.h"
#include "mon_stats.h"
#include "scheduler_queue.h"
#include <ydb/library/actors/queues/activation_queue.h>
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/unordered_cache.h>
#include <ydb/library/actors/util/threadparkpad.h>

//#define RING_ACTIVATION_QUEUE 

namespace NActors {
    class TActorSystem;

    class TExecutorPoolBaseMailboxed: public IExecutorPool {
    protected:
        TActorSystem* ActorSystem;
        THolder<TMailboxTable> MailboxTable;
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        // Need to have per pool object to collect stats like actor registrations (because
        // registrations might be done in threads from other pools)
        TExecutorThreadStats Stats;

        // Stuck actor monitoring
        TMutex StuckObserverMutex;
        std::vector<IActor*> Actors;
        mutable std::vector<std::tuple<ui32, double>> DeadActorsUsage;
        friend class TGenericExecutorThread;
        friend class TSharedExecutorThread;
        void RecalculateStuckActors(TExecutorThreadStats& stats) const;
#endif
        TAtomic RegisterRevolvingCounter = 0;
        ui64 AllocateID();
    public:
        explicit TExecutorPoolBaseMailboxed(ui32 poolId);
        ~TExecutorPoolBaseMailboxed();
        void ReclaimMailbox(TMailboxType::EType mailboxType, ui32 hint, TWorkerId workerId, ui64 revolvingWriteCounter) override;
        TMailboxHeader *ResolveMailbox(ui32 hint) override;
        bool Send(TAutoPtr<IEventHandle>& ev) override;
        bool SpecificSend(TAutoPtr<IEventHandle>& ev) override;
        TActorId Register(IActor* actor, TMailboxType::EType mailboxType, ui64 revolvingWriteCounter, const TActorId& parentId) override;
        TActorId Register(IActor* actor, TMailboxHeader* mailbox, ui32 hint, const TActorId& parentId) override;
        bool Cleanup() override;
    };

    class TExecutorPoolBase: public TExecutorPoolBaseMailboxed {
    protected:

#ifdef RING_ACTIVATION_QUEUE
        using TActivationQueue = TRingActivationQueue;
#else
        using TActivationQueue = TUnorderedCache<ui32, 512, 4>;
#endif

        const i16 PoolThreads;
        TIntrusivePtr<TAffinity> ThreadsAffinity;
        TAtomic Semaphore = 0;
        TActivationQueue Activations;
        TAtomic ActivationsRevolvingCounter = 0;
        std::atomic_bool StopFlag = false;
    public:
        TExecutorPoolBase(ui32 poolId, ui32 threads, TAffinity* affinity);
        ~TExecutorPoolBase();
        void ScheduleActivation(ui32 activation) override;
        void SpecificScheduleActivation(ui32 activation) override;
        TAffinity* Affinity() const override;
        ui32 GetThreads() const override;
    };

    void DoActorInit(TActorSystem*, IActor*, const TActorId&, const TActorId&);
}
