#pragma once

#include "executor_pool.h"
#include "executor_thread.h"
#include "mon_stats.h"
#include "scheduler_queue.h"
#include <ydb/library/actors/queues/activation_queue.h>
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/unordered_cache.h>
#include <ydb/library/actors/util/threadparkpad.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

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
        bool Send(std::unique_ptr<IEventHandle>& ev) override;
        bool SpecificSend(std::unique_ptr<IEventHandle>& ev) override;
        TActorId Register(IActor* actor, TMailboxType::EType mailboxType, ui64 revolvingWriteCounter, const TActorId& parentId) override;
        TActorId Register(IActor* actor, TMailboxCache& cache, ui64 revolvingWriteCounter, const TActorId& parentId) override;
        TActorId Register(IActor* actor, TMailbox* mailbox, const TActorId& parentId) override;
        TActorId RegisterAlias(TMailbox* mailbox, IActor* actor) override;
        void UnregisterAlias(TMailbox* mailbox, const TActorId& actorId) override;
        bool Cleanup() override;
        TMailboxTable* GetMailboxTable() const override;
    };

    struct TTaskPool {
        using TUnorderedCacheActivationQueue = TUnorderedCache<ui32, 512, 4>;

        alignas(64) std::variant<TUnorderedCacheActivationQueue, TRingActivationQueueV4> Activations;
        alignas(64) std::atomic<i64> Semaphore = 0;
        alignas(64) std::atomic<ui64> ActivationsRevolvingCounter = 0;

        TTaskPool();

        void Init(ui32 threads, bool useRingQueue);
    };

    class TExecutorPoolBase: public TExecutorPoolBaseMailboxed {
    protected:
        static constexpr ui64 ThreadsForTaskPool = 4;

        const i16 PoolThreads;
        i16 TaskPoolsCount;
        const bool UseRingQueueValue;
        std::unique_ptr<TTaskPool[]> TaskPoolsHolder;
        TTaskPool* TaskPools;
        alignas(64) TIntrusivePtr<TAffinity> ThreadsAffinity;
        alignas(64) std::atomic_bool StopFlag = false;

    public:
        TExecutorPoolBase(ui32 poolId, ui32 threads, TAffinity* affinity, bool useRingQueue, bool useTaskPools=false);
        ~TExecutorPoolBase();
        void ScheduleActivation(TMailbox* mailbox) override;
        void SpecificScheduleActivation(TMailbox* mailbox) override;
        TAffinity* Affinity() const override;
        ui32 GetThreads() const override;
        bool UseRingQueue() const;
    };

    void DoActorInit(TActorSystem*, IActor*, const TActorId&, const TActorId&);
}
