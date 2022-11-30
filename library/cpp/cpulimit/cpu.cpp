#include "cpu.h"

#include <util/datetime/cputimer.h>
#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>
#include <util/random/random.h>
#include <util/system/rusage.h>
#include <util/thread/pool.h>

namespace {
    TDuration GetUsage() {
        const auto rusage = TRusage::Get();
        return rusage.Utime + rusage.Stime;
    }

    TInstant MonotonicNow() {
        return TInstant::Zero() + CyclesToDuration(GetCycleCount());
    }
}

namespace NCpuLimit {
    TCpuMeasurer::TCpuMeasurer(TDuration probePeriod)
        : FastProbe_{.Period = probePeriod, .Usage = 0}
        , SlowProbe_{.Period = TDuration::MilliSeconds(100), .Usage = 0}
    {
        const auto now = MonotonicNow();
        const auto usageNow = GetUsage();
        for (auto [probe, size] : {std::pair(&FastProbe_, 5), std::pair(&SlowProbe_, 3)}) {
            probe->Window.assign(size, {now, usageNow});
            probe->MeasurerThread = SystemThreadFactory()->Run([this, probe = probe] {
                UpdateProbeThread(*probe);
            });
        }
    }

    TCpuMeasurer::~TCpuMeasurer() {
        Finished_ = true;
        for (auto* probe : {&FastProbe_, &SlowProbe_}) {
            probe->MeasurerThread->Join();
        }
    }

    void TCpuMeasurer::UpdateProbeThread(TProbe& probe) {
        const ui64 numberOfMeasurments = 10;
        const TDuration checkPeriod = probe.Period / numberOfMeasurments;
        while (!Finished_.load()) {
            Sleep(checkPeriod);

            const auto now = MonotonicNow();
            const auto usageNow = GetUsage();

            const auto [windowStartTime, windowStartUsage] = probe.Window.front();

            const auto windowDuration = now - windowStartTime;
            const auto windowUsage = usageNow - windowStartUsage;

            probe.Usage.store(windowUsage / windowDuration);
            probe.Window.emplace_back(now, usageNow);

            if (probe.Window.size() > numberOfMeasurments) {
                probe.Window.pop_front();
            }
        }
    }

    TCpuLimiter::TCpuLimiter(double slowThreshold, double fastThresholdBegin, double fastThresholdEnd)
        : SlowThreshold_(slowThreshold)
        , FastThresholdBegin_(fastThresholdBegin)
        , FastThresholdEnd_(fastThresholdEnd)
    {
        Y_ENSURE(FastThresholdBegin_ <= FastThresholdEnd_);
    }

    bool TCpuLimiter::ThrottleHard(double slowUsage, double fastUsage) const {
        if (slowUsage <= SlowThreshold_) {
            return false;
        }
        if (fastUsage > FastThresholdEnd_) {
            return true;
        } else if (fastUsage > FastThresholdBegin_) {
            return RandomNumber<ui32>() * (FastThresholdEnd_ - FastThresholdBegin_) <
                   Max<ui32>() * (fastUsage - FastThresholdBegin_);
        } else {
            return false;
        }
    }

    bool TCpuLimiter::ThrottleSoft(double slowUsage, double fastUsage) const {
        return slowUsage > SlowThreshold_ && fastUsage > FastThresholdEnd_;
    }
}
