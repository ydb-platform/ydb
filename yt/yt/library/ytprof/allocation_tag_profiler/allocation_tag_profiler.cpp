#include "allocation_tag_profiler.h"

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/library/ytprof/heap_profiler.h>

namespace NYT::NYTProf {

using namespace NProfiling;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

THeapUsageProfiler::THeapUsageProfiler(
    std::vector<TString> tags,
    IInvokerPtr invoker,
    std::optional<TDuration> updatePeriod,
    std::optional<i64> samplingRate,
    NProfiling::TProfiler profiler)
    : Profiler_(std::move(profiler))
    , TagTypes_(std::move(tags))
    , UpdateExecutor_(New<TPeriodicExecutor>(
        std::move(invoker),
        BIND(&THeapUsageProfiler::UpdateGauges, MakeWeak(this)),
        std::move(updatePeriod)))
{
    if (samplingRate) {
        tcmalloc::MallocExtension::SetProfileSamplingRate(*samplingRate);
    }

    UpdateExecutor_->Start();
}

void THeapUsageProfiler::UpdateGauges()
{
    const auto memorySnapshot = GetMemoryUsageSnapshot();
    YT_VERIFY(memorySnapshot);

    for (const auto& tagType : TagTypes_) {
        auto& heapUsageMap = HeapUsageByType_.emplace(tagType, THashMap<TString, TGauge>{}).first->second;
        const auto& snapshotSlice = memorySnapshot->GetUsage(tagType);

        for (auto &[tag, gauge] : heapUsageMap) {
            if (const auto& iter = snapshotSlice.find(tag)) {
                gauge.Update(iter->second);
            } else {
                gauge.Update(0.0);
            }
        }

        for (const auto& [tag, usage] : snapshotSlice) {
            auto gauge = heapUsageMap.find(tag);

            if (gauge.IsEnd()) {
                gauge = heapUsageMap.emplace(tag, Profiler_
                    .WithTag(tagType, tag)
                    .Gauge(tagType))
                    .first;
                gauge->second.Update(usage);
            }
        }
    }
}

///////////////////////////////////////////////////////////////////

THeapUsageProfilerPtr CreateHeapProfilerWithTags(
    std::vector<TString>&& tags,
    IInvokerPtr invoker,
    std::optional<TDuration> updatePeriod,
    std::optional<i64> samplingRate,
    NYT::NProfiling::TProfiler profiler)
{
    return New<THeapUsageProfiler>(
        std::move(tags),
        std::move(invoker),
        std::move(updatePeriod),
        std::move(samplingRate),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf

