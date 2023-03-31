#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/deque.h>
#include <util/system/hp_timer.h>
#include <util/system/mutex.h>

namespace NUnifiedAgent {
    class TDurationUsCounter {
    public:
        class TScope {
        public:
            TScope(TDurationUsCounter& counter)
                : Counter(counter)
                , StartTime(Counter.Begin())
            {
            }

            ~TScope() {
                Counter.End(StartTime);
            }

        private:
            TDurationUsCounter& Counter;
            NHPTimer::STime* StartTime;
        };

    public:
        TDurationUsCounter(const TString& name, NMonitoring::TDynamicCounters& owner);

        NHPTimer::STime* Begin();

        void End(NHPTimer::STime* startTime);

        void Update();

    private:
        NMonitoring::TDeprecatedCounter& Counter;
        TDeque<NHPTimer::STime> ActiveTimers;
        TAdaptiveLock Lock;
    };
}
