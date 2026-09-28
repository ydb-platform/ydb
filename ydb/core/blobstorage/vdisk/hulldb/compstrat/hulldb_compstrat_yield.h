#pragma once

#include <library/cpp/time_provider/monotonic.h>

#include <functional>

namespace NKikimr::NHullComp {

    // The budget for the selection, synchronous strategy omits it.
    class TCompactionYield {
        using TMonotonic = NMonotonic::TMonotonic;

        const std::function<void()> Yield;
        const TDuration Quantum;
        TMonotonic Deadline = TMonotonic::Now() + Quantum;
        TDuration SuspendedTime = TDuration::Zero();
        ui32 Steps = 0;

    public:
        TCompactionYield(TDuration quantum, std::function<void()> yield)
            : Yield(std::move(yield))
            , Quantum(quantum)
        {}

        void Check() {
            // Avoid reading the clock too often.
            if (++Steps % 64) {
                return;
            }
            const TMonotonic now = TMonotonic::Now();
            if (now >= Deadline) {
                Yield();
                const TMonotonic resumed = TMonotonic::Now();
                SuspendedTime += resumed - now;
                Deadline = resumed + Quantum;
            }
        }

        TDuration GetSuspendedTime() const {
            return SuspendedTime;
        }
    };

    inline void CheckCompactionYield(TCompactionYield* yield) {
        if (yield) {
            yield->Check();
        }
    }

} // namespace NKikimr::NHullComp
