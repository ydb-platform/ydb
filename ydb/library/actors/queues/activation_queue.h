#pragma once

#include "defs.h"
#include "mpmc_ring_queue.h"
#include "mpmc_ring_queue_v2.h"
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
    TRingActivationQueue(bool isMPSC)
        : IsMPSC(isMPSC)
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

class TBlockingActivationQueue {
    NThreading::TPadded<TMPMCBlockingRingQueue<20>> ActivationQueue;

public:
    TBlockingActivationQueue(bool)
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
