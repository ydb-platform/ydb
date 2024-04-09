#pragma once

#include "defs.h"
#include "mpmc_ring_queue.h"
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
        } else {
            activation = ActivationQueue.TryPop(TlsThreadContext->ActivationPopMode);
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

} // NActors
