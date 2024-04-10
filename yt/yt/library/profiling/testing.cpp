#include "testing.h"
#include "impl.h"

#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

double TTesting::ReadGauge(const TGauge& gauge)
{
    Y_ENSURE(gauge.Gauge_, "Gauge is not registered");
    return gauge.Gauge_->GetValue();
}

TDuration TTesting::ReadTimeGauge(const TTimeGauge& gauge)
{
    Y_ENSURE(gauge.Gauge_, "Gauge is not registered");
    return gauge.Gauge_->GetValue();
}

i64 TTesting::ReadCounter(const TCounter& counter)
{
    Y_ENSURE(counter.Counter_, "Counter is not registered");
    return counter.Counter_->GetValue();
}

TDuration TTesting::ReadTimeCounter(const TTimeCounter& counter)
{
    Y_ENSURE(counter.Counter_, "Counter is not registered");
    return counter.Counter_->GetValue();
}

THistogramSnapshot TTesting::ReadRateHistogram(const TRateHistogram& histogram)
{
    Y_ENSURE(histogram.Histogram_, "Histogram is not registered");
    return histogram.Histogram_->GetSnapshot(false);
}

const TSensorOptions& TTesting::ReadOptions(const TProfiler& profiler) {
    return profiler.Options_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
