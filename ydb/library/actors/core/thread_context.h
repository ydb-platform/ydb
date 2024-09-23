#pragma once

#include "defs.h"

#include <atomic>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/queues/mpmc_ring_queue.h>

#include <util/system/tls.h>


namespace NActors {

    class IExecutorPool;
    struct TWorkerContext;

    template <typename T>
    struct TWaitingStats;

    struct TThreadContext {
        IExecutorPool *Pool = nullptr;
        ui32 CapturedActivation = 0;
        ESendingType CapturedType = ESendingType::Lazy;
        ESendingType SendingType = ESendingType::Common;
        bool IsEnoughCpu = true;
        ui32 WriteTurn = 0;
        TWorkerId WorkerId;
        ui16 LocalQueueSize = 0;
        TWaitingStats<ui64> *WaitingStats = nullptr;
        bool IsCurrentRecipientAService = false;
        TMPMCRingQueue<20>::EPopMode ActivationPopMode = TMPMCRingQueue<20>::EPopMode::ReallySlow;

        std::atomic<i64> StartOfProcessingEventTS = GetCycleCountFast();
        std::atomic<i64> ActivationStartTS = 0;
        std::atomic<ui64> ElapsingActorActivity = Max<ui64>();
        TWorkerContext *WorkerCtx = nullptr;
        ui32 ActorSystemIndex = 0;

        TThreadContext() {
            i64 now = GetCycleCountFast();
            StartOfProcessingEventTS = now;
            ActivationStartTS = now;
        }

        ui64 UpdateStartOfProcessingEventTS(i64 newValue) {
            i64 oldValue = StartOfProcessingEventTS.load(std::memory_order_acquire);
            for (;;) {
                if (newValue - oldValue <= 0) {
                    break;
                }
                if (StartOfProcessingEventTS.compare_exchange_strong(oldValue, newValue, std::memory_order_acq_rel)) {
                    break;
                }
            }
            return oldValue;
        }
    };

    extern Y_POD_THREAD(TThreadContext*) TlsThreadContext; // in actor.cpp

}
