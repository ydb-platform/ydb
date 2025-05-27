#pragma once

#include "defs.h"
#include "mailbox.h"

#include <atomic>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/queues/mpmc_ring_queue.h>
#include <util/system/tls.h>

#include <library/cpp/lwtrace/shuttle.h>

namespace NActors {

    class TMailbox;
    class TMailboxTable;
    struct TExecutionStats;

    class IExecutorPool;

    template <typename T>
    struct TWaitingStats;

    constexpr ui32 SleepActivity = Max<ui32>();

    struct TCapturedActivation {
        TMailbox* Mailbox = nullptr;
        ESendingType SendingType = ESendingType::Common;
    };

    struct TLocalQueueContext {
        ui32 WriteTurn = 0;
        ui16 LocalQueueSize = 0;
    };

    struct TThreadActivityContext {
        std::atomic<i64> StartOfProcessingEventTS = GetCycleCountFast();
        std::atomic<i64> ActivationStartTS = 0;
        std::atomic<ui32> ElapsingActorActivity = SleepActivity;
        ui32 ActorSystemIndex = 0;
    };

    struct TWorkerContext {
        const TWorkerId WorkerId;
        IExecutorPool* Pool = nullptr;
        IExecutorPool* OwnerPool = nullptr;
        IExecutorPool* SharedPool = nullptr;
        TMailboxTable* MailboxTable = nullptr;
        TMailboxCache MailboxCache;
        ui64 TimePerMailboxTs = 0;
        ui32 EventsPerMailbox = 0;
        ui64 SoftDeadlineTs = ui64(-1);
        bool UseRingQueueValue = false;

        TWorkerContext(TWorkerId workerId, IExecutorPool* pool, IExecutorPool* sharedPool);

        ui32 PoolId() const;
        TString PoolName() const;
        ui32 OwnerPoolId() const;
        bool IsShared() const;
        bool UseRingQueue() const;
        void AssignPool(IExecutorPool* pool, ui64 softDeadlineTs = -1);
        void FreeMailbox(TMailbox* mailbox);
    };

    struct TExecutionContext {
        TCapturedActivation CapturedActivation;
        ui32 ExecutedEvents = 0;
        ui32 OverwrittenEventsPerMailbox = 0;
        ui64 OverwrittenTimePerMailboxTs = 0;
        TStackVec<TActorId, 1> PreemptionSubscribed;
        bool IsNeededToWaitNextActivation = true;
        ESendingType SendingType = ESendingType::Common;
        NHPTimer::STime HPStart = 0;
        mutable NLWTrace::TOrbit Orbit;

        bool CheckSendingType(ESendingType type) const;
        bool CheckCapturedSendingType(ESendingType type) const;
    };

    struct TThreadContext {
        TWorkerContext WorkerContext;
        TLocalQueueContext LocalQueueContext;
        TThreadActivityContext ActivityContext;
        TExecutionContext ExecutionContext;
        TExecutionStats *ExecutionStats = nullptr;


        bool IsEnoughCpu = true;
        TWaitingStats<ui64> *WaitingStats = nullptr;
        bool IsCurrentRecipientAService = false;
        TMPMCRingQueue<20>::EPopMode ActivationPopMode = TMPMCRingQueue<20>::EPopMode::ReallySlow;
        ui64 ProcessedActivationsByCurrentPool = 0;


        TThreadContext(TWorkerId workerId, IExecutorPool* pool, IExecutorPool* sharedPool);

        ui64 UpdateStartOfProcessingEventTS(i64 newValue) {
            i64 oldValue = ActivityContext.StartOfProcessingEventTS.load(std::memory_order_acquire);
            for (;;) {
                if (newValue - oldValue <= 0) {
                    break;
                }
                if (ActivityContext.StartOfProcessingEventTS.compare_exchange_strong(oldValue, newValue, std::memory_order_acq_rel)) {
                    break;
                }
            }
            return oldValue;
        }

        ui32 PoolId() const;
        TString PoolName() const;
        ui32 OwnerPoolId() const;
        TWorkerId WorkerId() const;
        IExecutorPool* Pool() const;
        IExecutorPool* SharedPool() const;
        bool IsShared() const;
        ui64 TimePerMailboxTs() const;
        ui32 EventsPerMailbox() const;
        ui64 SoftDeadlineTs() const;
        void FreeMailbox(TMailbox* mailbox);
        bool UseRingQueue() const;
        void AssignPool(IExecutorPool* pool, ui64 softDeadlineTs = Max<ui64>());

        bool CheckSendingType(ESendingType type) const;
        ESendingType SendingType() const;
        void SetSendingType(ESendingType type);
        ESendingType ExchangeSendingType(ESendingType type);
        bool CheckCapturedSendingType(ESendingType type) const;
        TMailbox* CaptureMailbox(TMailbox* mailbox);
        void ChangeCapturedSendingType(ESendingType type);

        ui32 OverwrittenEventsPerMailbox() const;
        void SetOverwrittenEventsPerMailbox(ui32 value);
        void ResetOverwrittenEventsPerMailbox();
        ui64 OverwrittenTimePerMailboxTs() const;
        void SetOverwrittenTimePerMailboxTs(ui64 value);
        void ResetOverwrittenTimePerMailboxTs();
    };

    extern Y_POD_THREAD(TThreadContext*) TlsThreadContext; // in actor.cpp

}
