#pragma once

#include "defs.h"
#include "event.h"
#include "thread_context.h"
#include "execution_stats.h"
#include "log_settings.h"

#include <ydb/library/actors/util/datetime.h>

#include <util/system/thread.h>

namespace NActors {
    class IActor;
    class TActorSystem;
    struct TExecutorThreadCtx;
    struct TSharedExecutorThreadCtx;
    class TExecutorPoolBaseMailboxed;
    class TMailboxTable;
    class TMailboxCache;
    struct TThreadContext;
    class TExecutorThread: public ISimpleThread {
    protected:
        struct TProcessingResult {
            bool IsPreempted = false;
            bool WasWorking = false;
        };

        constexpr static ui32 DefaultPoolCountForExecutorThread = 1;

    public:
        static constexpr TDuration DEFAULT_TIME_PER_MAILBOX =
            TDuration::MilliSeconds(10);
        static constexpr ui32 DEFAULT_EVENTS_PER_MAILBOX = 100;

        // common thread ctor
        TExecutorThread(TWorkerId workerId,
                        TActorSystem* actorSystem,
                        IExecutorPool* executorPool,
                        const TString& threadName);

        // shared thread ctor
        TExecutorThread(TWorkerId workerId,
                    TActorSystem* actorSystem,
                    IExecutorPool* sharedPool,
                    IExecutorPool* executorPool,
                    i16 poolCount,
                    const TString& threadName,
                    ui64 softProcessingDurationTs);

        virtual ~TExecutorThread();

        template <ESendingType SendingType = ESendingType::Common>
        TActorId RegisterActor(IActor* actor, TMailboxType::EType mailboxType = TMailboxType::HTSwap, ui32 poolId = Max<ui32>(),
                               TActorId parentId = TActorId());
        template <ESendingType SendingType = ESendingType::Common>
        TActorId RegisterActor(IActor* actor, TMailbox* mailbox, TActorId parentId = TActorId());
        void UnregisterActor(TMailbox* mailbox, TActorId actorId);
        void DropUnregistered();
        const std::vector<THolder<IActor>>& GetUnregistered() const { return DyingActors; }

        TActorId RegisterAlias(TMailbox* mailbox, IActor* actor);
        void UnregisterAlias(TMailbox* mailbox, const TActorId& actorId);

        void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr);
        void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr);
        void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr);

        template <ESendingType SendingType = ESendingType::Common>
        bool Send(TAutoPtr<IEventHandle> ev);

        void GetCurrentStats(TExecutorThreadStats& statsCopy);
        void GetSharedStats(i16 poolId, TExecutorThreadStats &stats);

        void GetCurrentStatsForHarmonizer(TExecutorThreadStats& statsCopy);
        void GetSharedStatsForHarmonizer(i16 poolId, TExecutorThreadStats &stats);

        TThreadId GetThreadId() const; // blocks, must be called after Start()
        TWorkerId GetWorkerId() const;

        void SubscribeToPreemption(TActorId actorId);
        ui32 GetOverwrittenEventsPerMailbox() const;
        void SetOverwrittenEventsPerMailbox(ui32 value);
        ui64 GetOverwrittenTimePerMailboxTs() const;
        void SetOverwrittenTimePerMailboxTs(ui64 value);

    protected:
        void ProcessExecutorPool();

        TProcessingResult Execute(TMailbox* mailbox, bool isTailExecution);

        void UpdateThreadStats();

    public:
        TActorSystem* const ActorSystem;
        std::atomic<bool> StopFlag = false;

        void SwitchPool(TExecutorPoolBaseMailboxed* pool);

    private:
        void* ThreadProc();

    protected:
        // Pool-specific
        TStackVec<TExecutorThreadStats, DefaultPoolCountForExecutorThread> Stats;

        // Event-specific (currently executing)
        TVector<THolder<IActor>> DyingActors;
        TActorId CurrentRecipient;
        ui64 CurrentActorScheduledEventsCounter = 0;

        // Thread-specific
        alignas(64) mutable TThreadContext ThreadCtx;
        alignas(64) mutable TExecutionStats ExecutionStats;
        alignas(64) ui64 RevolvingReadCounter = 0;
        ui64 RevolvingWriteCounter = 0;
        const TString ThreadName;
        volatile TThreadId ThreadId = UnknownThreadId;
        bool IsSharedThread = false;

        ui64 SoftProcessingDurationTs;

        const ui32 ActorSystemIndex;
    };

}
