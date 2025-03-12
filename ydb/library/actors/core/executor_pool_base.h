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
        THolder<TMailboxTable> MailboxTableHolder;
        TMailboxTable* MailboxTable;
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        // Need to have per pool object to collect stats like actor registrations (because
        // registrations might be done in threads from other pools)
        TExecutorThreadStats Stats;

        // Stuck actor monitoring
        TMutex StuckObserverMutex;
        std::vector<IActor*> Actors;
        mutable std::vector<std::tuple<ui32, double>> DeadActorsUsage;
        friend class TExecutorThread;
        friend class TSharedExecutorThread;
        void RecalculateStuckActors(TExecutorThreadStats& stats) const;
#endif
        TAtomic RegisterRevolvingCounter = 0;
        ui64 AllocateID();
    public:
        explicit TExecutorPoolBaseMailboxed(ui32 poolId);
        ~TExecutorPoolBaseMailboxed();
        TMailbox* ResolveMailbox(ui32 hint) override;
        bool Send(TAutoPtr<IEventHandle>& ev) override;
        bool SpecificSend(TAutoPtr<IEventHandle>& ev) override;
        TActorId Register(IActor* actor, TMailboxType::EType mailboxType, ui64 revolvingWriteCounter, const TActorId& parentId) override;
        TActorId Register(IActor* actor, TMailboxCache& cache, ui64 revolvingWriteCounter, const TActorId& parentId) override;
        TActorId Register(IActor* actor, TMailbox* mailbox, const TActorId& parentId) override;
        TActorId RegisterAlias(TMailbox* mailbox, IActor* actor) override;
        void UnregisterAlias(TMailbox* mailbox, const TActorId& actorId) override;
        bool Cleanup() override;
        TMailboxTable* GetMailboxTable() const override;
    };

    class TExecutorPoolBase: public TExecutorPoolBaseMailboxed {
    protected:
        using TUnorderedCacheActivationQueue = TUnorderedCache<ui32, 512, 4>;

        const i16 PoolThreads;
        const bool UseRingQueueValue;
        alignas(64) TIntrusivePtr<TAffinity> ThreadsAffinity;
        alignas(64) TAtomic Semaphore = 0;
        alignas(64) std::variant<TUnorderedCacheActivationQueue, TRingActivationQueue> Activations;
        TAtomic ActivationsRevolvingCounter = 0;
        std::atomic_bool StopFlag = false;
    public:
        TExecutorPoolBase(ui32 poolId, ui32 threads, TAffinity* affinity, bool useRingQueue);
        ~TExecutorPoolBase();
        void ScheduleActivation(TMailbox* mailbox) override;
        void SpecificScheduleActivation(TMailbox* mailbox) override;
        TAffinity* Affinity() const override;
        ui32 GetThreads() const override;
        bool UseRingQueue() const;
    };

    void DoActorInit(TActorSystem*, IActor*, const TActorId&, const TActorId&);
}
