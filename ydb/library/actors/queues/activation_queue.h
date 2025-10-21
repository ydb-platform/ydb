#pragma once

#include "defs.h"
#include "mpmc_ring_queue.h"
#include "mpmc_ring_queue_v1.h"
#include "mpmc_ring_queue_v2.h"
#include "mpmc_ring_queue_v3.h"
#include "mpmc_ring_queue_v4.h"
#include "mpmc_ring_queue_v4_correct.h"
#include "mpmc_ring_queue_v5.h"
#include "mpmc_ring_queue_v6.h"
#include "mpmc_ring_queue_v7.h"
#include "mpmc_ring_queue_v8.h"
#include "mpmc_ring_queue_v9.h"
#include "mpmc_ring_queue_vnext_v1.h"
#include "mpmc_ring_queue_blocking.h"
#include <atomic>


namespace NActors {


class TRingActivationQueue {
    NThreading::TPadded<std::atomic_bool> IsNeedToWriteToOldQueue = false;
    NThreading::TPadded<TMPMCRingQueue<20>> ActivationQueue;
    NThreading::TPadded<TUnorderedCache<ui32, 512, 4>> OldActivationQueue;
    NThreading::TPadded<std::atomic_uint64_t> RevolvingCounter = 0;
    const bool IsMPSC = false;

public:
    TRingActivationQueue(ui32 readersCount)
        : IsMPSC(readersCount == 1)
    {}

    void Push(ui32 activation, ui64 revolvingCounter) {
        if (!IsNeedToWriteToOldQueue.load(std::memory_order_acquire)) {
            if (ActivationQueue.TryPush(activation)) {
                return;
            }
            IsNeedToWriteToOldQueue.store(true, std::memory_order_release);
        }
        if (!revolvingCounter) {
            revolvingCounter = RevolvingCounter.fetch_add(1, std::memory_order_relaxed);
        }
        OldActivationQueue.Push(activation, AtomicIncrement(revolvingCounter));
    }

    ui32 Pop(ui64 revolvingCounter) {
        std::optional<ui32> activation;
        if (IsMPSC) {
            activation = ActivationQueue.TryPopSingleConsumer();
        } else if (TlsThreadContext) {
            activation = ActivationQueue.TryPop(TlsThreadContext->ActivationPopMode);
        } else {
            // must be for destruction of actorsystem outside of actorsystem threads
            using EPopMode = decltype(ActivationQueue)::EPopMode;
            EPopMode popMode = EPopMode::ReallySlow;
            activation = ActivationQueue.TryPop(popMode);
        }
        if (activation) {
            return *activation;
        }
        if (IsNeedToWriteToOldQueue.load(std::memory_order_acquire)) {
            return OldActivationQueue.Pop(revolvingCounter);
        }
        return 0;
    }

};

class TRingActivationQueueV4 {
    NThreading::TPadded<std::atomic_bool> IsNeedToWriteToOldQueue = false;
    NThreading::TPadded<TMPMCRingQueueV4Correct<20>> ActivationQueue;
    NThreading::TPadded<TUnorderedCache<ui32, 512, 4>> OldActivationQueue;
    NThreading::TPadded<std::atomic_uint64_t> RevolvingCounter = 0;

public:
    TRingActivationQueueV4(ui32 readersCount)
        : ActivationQueue(readersCount)
    {}

    void Push(ui32 activation, ui64 revolvingCounter) {
        if (!IsNeedToWriteToOldQueue.load(std::memory_order_acquire)) {
            if (ActivationQueue.TryPush(activation)) {
                return;
            }
            IsNeedToWriteToOldQueue.store(true, std::memory_order_release);
        }
        if (!revolvingCounter) {
            revolvingCounter = RevolvingCounter.fetch_add(1, std::memory_order_relaxed);
        }
        OldActivationQueue.Push(activation, AtomicIncrement(revolvingCounter));
    }

    ui32 Pop(ui64 revolvingCounter) {
        std::optional<ui32> activation = ActivationQueue.TryPop();
        if (activation) {
            return *activation;
        }
        if (IsNeedToWriteToOldQueue.load(std::memory_order_acquire)) {
            return OldActivationQueue.Pop(revolvingCounter);
        }
        return 0;
    }

};

class TBlockingActivationQueue {
    NThreading::TPadded<TMPMCBlockingRingQueue<20>> ActivationQueue;

public:
    TBlockingActivationQueue(ui32)
    {}


    void Push(ui32 activation, ui64) {
        for (;;) {
            if (ActivationQueue.TryPush(activation)) {
                return;
            }
        }
    }

    ui32 Pop(ui64) {
        std::optional<ui32> activation;
        activation = ActivationQueue.TryPop();
        if (activation) {
            return *activation;
        }
        return 0;
    }

    void Stop() {
        ActivationQueue.Stop();
    }
};

} // NActors
