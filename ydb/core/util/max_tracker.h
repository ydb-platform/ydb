#pragma once

#include "defs.h"
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {

class TMaxTracker {
    TVector<std::atomic<i64>> Slots;
    std::atomic<size_t> CurrentIdx = 0;
    TInstant Last = TInstant::Now();
    ::NMonitoring::TDynamicCounters::TCounterPtr Counter;

public:

    TMaxTracker(size_t slots = 15) 
        : Slots(std::max<size_t>(1, slots))
    {
        for (auto& a : Slots) {
            a.store(0, std::memory_order_relaxed);
        }
    }

    void Init(::NMonitoring::TDynamicCounters::TCounterPtr counter) {
        Counter = counter;
    }

    void Collect(i64 val) {
        const size_t idx = CurrentIdx.load(std::memory_order_acquire);
        i64 oldVal = Slots[idx].load(std::memory_order_relaxed);
        while (val > oldVal) {
            if (Slots[idx].compare_exchange_weak(oldVal, val,
                std::memory_order_release, std::memory_order_relaxed)) {
                break;
            }
        }
    }

    // The function is supposed to be called every 1 second.
    // Calling thread can differ from Collect's caller thread
    void Update() {
        if (Counter) {
            i64 maxVal = Slots[0].load(std::memory_order_relaxed);
            for (size_t i = 1; i < Slots.size(); ++i) {
                maxVal = std::max(maxVal, Slots[i].load(std::memory_order_relaxed));
            }
            Counter->Set(maxVal);
        }
        const size_t idx = CurrentIdx.load(std::memory_order_relaxed);
        const size_t newIdx = (idx + 1) % Slots.size();
        Slots[newIdx].store(0, std::memory_order_relaxed);
        CurrentIdx.store(newIdx, std::memory_order_release);
    }
};

}
