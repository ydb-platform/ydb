#include "profiler.h"

#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/library/profiling/producer.h>

#include <tcmalloc/malloc_extension.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TProfilingStatisticsProducer
    : public ISensorProducer
{
public:
    TProfilingStatisticsProducer()
    {
        TProfiler profiler{""};
        profiler.AddProducer("/memory", MakeStrong(this));
    }

    void CollectSensors(ISensorWriter* writer) override
    {
        for (const auto& property : TCMallocStats_) {
            if (auto value = tcmalloc::MallocExtension::GetNumericProperty(property)) {
                writer->AddGauge("/" + property, *value);
            }
        }
    }

private:
    std::vector<TString> TCMallocStats_ = {
        "tcmalloc.per_cpu_caches_active",
        "generic.virtual_memory_used",
        "generic.physical_memory_used",
        "generic.bytes_in_use_by_app",
        "generic.heap_size",
        "tcmalloc.central_cache_free",
        "tcmalloc.cpu_free",
        "tcmalloc.page_heap_free",
        "tcmalloc.page_heap_unmapped",
        "tcmalloc.page_algorithm",
        "tcmalloc.max_total_thread_cache_bytes",
        "tcmalloc.thread_cache_free",
        "tcmalloc.thread_cache_count",
        "tcmalloc.local_bytes",
        "tcmalloc.external_fragmentation_bytes",
        "tcmalloc.metadata_bytes",
        "tcmalloc.transfer_cache_free",
        "tcmalloc.hard_usage_limit_bytes",
        "tcmalloc.desired_usage_limit_bytes",
        "tcmalloc.required_bytes"
    };
};

void EnableTCMallocProfiler()
{
    LeakyRefCountedSingleton<TProfilingStatisticsProducer>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
