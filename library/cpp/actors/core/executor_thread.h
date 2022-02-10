#pragma once

#include "defs.h"
#include "event.h"
#include "actor.h"
#include "actorsystem.h"
#include "callstack.h" 
#include "probes.h"
#include "worker_context.h"

#include <library/cpp/actors/util/datetime.h>

#include <util/system/thread.h>

namespace NActors {

    class TExecutorThread: public ISimpleThread {
    public:
        static constexpr TDuration DEFAULT_TIME_PER_MAILBOX =
            TDuration::MilliSeconds(10);
        static constexpr ui32 DEFAULT_EVENTS_PER_MAILBOX = 100;

        TExecutorThread(TWorkerId workerId,
                        TWorkerId cpuId,
                        TActorSystem* actorSystem,
                        IExecutorPool* executorPool,
                        TMailboxTable* mailboxTable,
                        const TString& threadName,
                        TDuration timePerMailbox = DEFAULT_TIME_PER_MAILBOX,
                        ui32 eventsPerMailbox = DEFAULT_EVENTS_PER_MAILBOX);

        TExecutorThread(TWorkerId workerId,
                        TActorSystem* actorSystem,
                        IExecutorPool* executorPool,
                        TMailboxTable* mailboxTable,
                        const TString& threadName,
                        TDuration timePerMailbox = DEFAULT_TIME_PER_MAILBOX,
                        ui32 eventsPerMailbox = DEFAULT_EVENTS_PER_MAILBOX)
            : TExecutorThread(workerId, 0, actorSystem, executorPool, mailboxTable, threadName, timePerMailbox, eventsPerMailbox)
        {}

        TActorId RegisterActor(IActor* actor, TMailboxType::EType mailboxType = TMailboxType::HTSwap, ui32 poolId = Max<ui32>(),
                               const TActorId& parentId = TActorId());
        TActorId RegisterActor(IActor* actor, TMailboxHeader* mailbox, ui32 hint, const TActorId& parentId = TActorId());
        void UnregisterActor(TMailboxHeader* mailbox, ui64 localActorId);
        void DropUnregistered();
        const std::vector<THolder<IActor>>& GetUnregistered() const { return DyingActors; }

        void Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr);
        void Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr);
        void Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie = nullptr);

        bool Send(TAutoPtr<IEventHandle> ev) {
#ifdef USE_ACTOR_CALLSTACK
            ev->Callstack = TCallstack::GetTlsCallstack();
            ev->Callstack.Trace();
#endif
            Ctx.IncrementSentEvents();
            return ActorSystem->Send(ev);
        }

        void GetCurrentStats(TExecutorThreadStats& statsCopy) const {
            Ctx.GetCurrentStats(statsCopy);
        }

        TThreadId GetThreadId() const; // blocks, must be called after Start()
        TWorkerId GetWorkerId() const { return Ctx.WorkerId; }

    private:
        void* ThreadProc();

        template <typename TMailbox>
        void Execute(TMailbox* mailbox, ui32 hint);

    public:
        TActorSystem* const ActorSystem;

    private:
        // Pool-specific
        IExecutorPool* const ExecutorPool;

        // Event-specific (currently executing)
        TVector<THolder<IActor>> DyingActors;
        TActorId CurrentRecipient;
        ui64 CurrentActorScheduledEventsCounter = 0;

        // Thread-specific
        TWorkerContext Ctx;
        ui64 RevolvingReadCounter = 0;
        ui64 RevolvingWriteCounter = 0;
        const TString ThreadName;
        volatile TThreadId ThreadId = UnknownThreadId;
    };

    template <typename TMailbox>
    void UnlockFromExecution(TMailbox* mailbox, IExecutorPool* executorPool, bool asFree, ui32 hint, TWorkerId workerId, ui64& revolvingWriteCounter) {
        mailbox->UnlockFromExecution1();
        const bool needReschedule1 = (nullptr != mailbox->Head());
        if (!asFree) {
            if (mailbox->UnlockFromExecution2(needReschedule1)) {
                RelaxedStore<NHPTimer::STime>(&mailbox->ScheduleMoment, GetCycleCountFast());
                executorPool->ScheduleActivationEx(hint, ++revolvingWriteCounter);
            }
        } else {
            if (mailbox->UnlockAsFree(needReschedule1)) {
                RelaxedStore<NHPTimer::STime>(&mailbox->ScheduleMoment, GetCycleCountFast());
                executorPool->ScheduleActivationEx(hint, ++revolvingWriteCounter);
            }
            executorPool->ReclaimMailbox(TMailbox::MailboxType, hint, workerId, ++revolvingWriteCounter);
        }
    }
}
