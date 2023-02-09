#include "duration_counter.h"

namespace NUnifiedAgent {
    using namespace NMonitoring;

    TDurationUsCounter::TDurationUsCounter(const TString& name, TDynamicCounters& owner)
        : Counter(*owner.GetCounter(name, true))
        , ActiveTimers()
        , Lock()
    {
    }

    NHPTimer::STime* TDurationUsCounter::Begin() {
        with_lock (Lock) {
            ActiveTimers.push_back(0);
            auto& result = ActiveTimers.back();
            NHPTimer::GetTime(&result);
            return &result;
        }
    }

    void TDurationUsCounter::End(NHPTimer::STime* startTime) {
        with_lock (Lock) {
            Counter += static_cast<ui64>(NHPTimer::GetTimePassed(startTime) * 1000000);
            *startTime = 0;
            while (!ActiveTimers.empty() && ActiveTimers.front() == 0) {
                ActiveTimers.pop_front();
            }
        }
    }

    void TDurationUsCounter::Update() {
        with_lock (Lock) {
            for (auto& startTime : ActiveTimers) {
                if (startTime != 0) {
                    Counter += static_cast<ui64>(NHPTimer::GetTimePassed(&startTime) * 1000000);
                }
            }
        }
    }
}
