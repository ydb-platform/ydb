#include "allocation_tag_profiler.h"

#ifdef __x86_64__
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/library/ytprof/heap_profiler.h>

#include <tcmalloc/malloc_extension.h>
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
        std::vector<TAllocationTagKey> tagKeys,
        IInvokerPtr invoker,
        std::optional<TDuration> updatePeriod,
        std::optional<i64> samplingRate,
        NProfiling::TProfiler profiler)
        : Profiler_(std::move(profiler))
        , TagKeys_(std::move(tagKeys))
        , UpdateExecutor_(New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TAllocationTagProfiler::UpdateGauges, MakeWeak(this)),
            updatePeriod))
    {
        if (samplingRate) {
            tcmalloc::MallocExtension::SetProfileSamplingRate(*samplingRate);
        }

        UpdateExecutor_->Start();
    }

private:
    const NProfiling::TProfiler Profiler_;
    const std::vector<TAllocationTagKey> TagKeys_;

    const NConcurrency::TPeriodicExecutorPtr UpdateExecutor_;

    THashMap<TAllocationTagKey, THashMap<TAllocationTagValue, NProfiling::TGauge>> Guages_;

    void UpdateGauges()
    {
        auto memorySnapshot = GetGlobalMemoryUsageSnapshot();

        for (const auto& tagKey : TagKeys_) {
            auto& guages = Guages_.emplace(tagKey, THashMap<TAllocationTagValue, TGauge>{}).first->second;
            const auto& slice = memorySnapshot->GetUsageSlice(tagKey);

            for (auto& [tagValue, gauge] : guages) {
                if (auto it = slice.find(tagValue)) {
                    gauge.Update(it->second);
                } else {
                    gauge.Update(0.0);
                }
            }

            for (const auto& [tagValue, usage] : slice) {
                auto it = guages.find(tagValue);
                if (it == guages.end()) {
                    it = guages.emplace(tagValue, Profiler_
                        .WithTag(tagKey, tagValue)
                        .Gauge(Format("/%v", NYPath::ToYPathLiteral(tagKey))))
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
    std::vector<TAllocationTagKey> tagKeys,
    IInvokerPtr invoker,
    std::optional<TDuration> updatePeriod,
    std::optional<i64> samplingRate,
    NYT::NProfiling::TProfiler profiler)
{
    return New<TAllocationTagProfiler>(
        std::move(tagKeys),
        std::move(invoker),
        std::move(updatePeriod),
        std::move(samplingRate),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
