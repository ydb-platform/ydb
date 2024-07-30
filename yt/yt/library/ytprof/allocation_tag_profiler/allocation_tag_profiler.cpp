#include "allocation_tag_profiler.h"

#ifdef __x86_64__
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/library/ytprof/heap_profiler.h>
#endif

namespace NYT::NYTProf {

#ifdef __x86_64__

using namespace NProfiling;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TAllocationTagProfiler
    : public IAllocationTagProfiler
{
public:
    TAllocationTagProfiler(
        std::vector<TString> tagNames,
        IInvokerPtr invoker,
        std::optional<TDuration> updatePeriod,
        std::optional<i64> samplingRate,
        NProfiling::TProfiler profiler)
        : Profiler_(std::move(profiler))
        , TagNames_(std::move(tagNames))
        , UpdateExecutor_(New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TAllocationTagProfiler::UpdateGauges, MakeWeak(this)),
            std::move(updatePeriod)))
    {
        if (samplingRate) {
            tcmalloc::MallocExtension::SetProfileSamplingRate(*samplingRate);
        }

        UpdateExecutor_->Start();
    }

private:
    const NProfiling::TProfiler Profiler_;
    const std::vector<TString> TagNames_;

    const NConcurrency::TPeriodicExecutorPtr UpdateExecutor_;

    THashMap<TString, THashMap<TString, NProfiling::TGauge>> HeapUsageByType_;

    void UpdateGauges()
    {
        auto memorySnapshot = GetMemoryUsageSnapshot();
        YT_VERIFY(memorySnapshot);

        for (const auto& tagName : TagNames_) {
            auto& heapUsageMap = HeapUsageByType_.emplace(tagName, THashMap<TString, TGauge>{}).first->second;
            const auto& snapshotSlice = memorySnapshot->GetUsage(tagName);

            for (auto& [tagValue, gauge] : heapUsageMap) {
                if (auto it = snapshotSlice.find(tagValue)) {
                    gauge.Update(it->second);
                } else {
                    gauge.Update(0.0);
                }
            }

            for (const auto& [tagValue, usage] : snapshotSlice) {
                auto it = heapUsageMap.find(tagValue);
                if (it == heapUsageMap.end()) {
                    it = heapUsageMap.emplace(tagValue, Profiler_
                        .WithTag(tagName, tagValue)
                        .Gauge(Format("/%v", NYPath::ToYPathLiteral(tagName))))
                        .first;
                    it->second.Update(usage);
                }
            }
        }
    }
};

#else

class TAllocationTagProfiler
    : public IAllocationTagProfiler
{
public:
    TAllocationTagProfiler(auto&&... /*args*/)
    { }
};

#endif

////////////////////////////////////////////////////////////////////////////////

IAllocationTagProfilerPtr CreateAllocationTagProfiler(
    std::vector<TString> tagNames,
    IInvokerPtr invoker,
    std::optional<TDuration> updatePeriod,
    std::optional<i64> samplingRate,
    NYT::NProfiling::TProfiler profiler)
{
    return New<TAllocationTagProfiler>(
        std::move(tagNames),
        std::move(invoker),
        std::move(updatePeriod),
        std::move(samplingRate),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
