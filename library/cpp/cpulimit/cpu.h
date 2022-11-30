#pragma once

#include "cpu.h"

#include <util/datetime/base.h>
#include <util/system/types.h>
#include <util/thread/factory.h>

#include <deque>

namespace NCpuLimit {
    class TCpuMeasurer {
        struct TProbe {
            TDuration Period;
            std::atomic<double> Usage;
            THolder<IThreadFactory::IThread> MeasurerThread;
            std::deque<std::pair<TInstant, TDuration>> Window;
            TDuration WindowDuration{};
            TDuration WindowUsage{};
        };

    public:
        explicit TCpuMeasurer(TDuration probePeriod);
        ~TCpuMeasurer();

        double CpuUsageFast() const {
            return FastProbe_.Usage.load();
        }

        double CpuUsageSlow() const {
            return SlowProbe_.Usage.load();
        }

    private:
        void UpdateProbeThread(TProbe& probe);

        std::atomic<bool> Finished_ = false;

        TProbe FastProbe_;
        TProbe SlowProbe_;
    };

    class TCpuLimiter {
    public:
        TCpuLimiter(double slowThreshold, double fastThresholdBegin, double fastThresholdEnd);

        // Only throttle (all requests) when LA is greater than FastThresholdEnd_
        bool ThrottleSoft(double slowUsage, double fastUsage) const;

        // Throttle requests with some probability when LA is greater than
        // FastThresholdBegin_
        bool ThrottleHard(double slowUsage, double fastUsage) const;

    private:
        double SlowThreshold_;
        double FastThresholdBegin_;
        double FastThresholdEnd_;
    };
}
