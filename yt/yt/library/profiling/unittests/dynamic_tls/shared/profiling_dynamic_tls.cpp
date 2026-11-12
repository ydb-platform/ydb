#include <yt/yt/library/profiling/solomon/config.h>
#include <yt/yt/library/profiling/solomon/registry.h>

#include <library/cpp/yt/memory/new.h>

#include <cstddef>

extern "C" {

// Force the legacy rseq path and keep its dynamically allocated TLS private to this DSO.
extern const std::ptrdiff_t __rseq_offset = 0;
extern const unsigned int __rseq_size = 0;

bool RunPerCpuSensorFallbackUpdatesFromDynamicTlsLibrary()
{
    using namespace NYT;
    using namespace NYT::NProfiling;

    auto registry = New<TSolomonRegistry>();
    auto config = New<TSolomonRegistryConfig>();
    config->EnableRseq = false;
    registry->Configure(config);
    if (registry->IsRseqEnabled()) {
        return false;
    }

    TSensorOptions options{
        .Hot = true,
    };
    auto counter = registry->RegisterCounter("/counter", {}, options);
    auto timeCounter = registry->RegisterTimeCounter("/time_counter", {}, options);
    auto gauge = registry->RegisterGauge("/gauge", {}, options);
    auto summary = registry->RegisterSummary("/summary", {}, options);

    counter->Increment(7);
    timeCounter->Add(TDuration::MicroSeconds(11));
    gauge->Update(42.0);
    summary->Record(3.0);

    auto summarySnapshot = summary->GetSummary();
    return counter->GetValue() == 7 &&
        timeCounter->GetValue() == TDuration::MicroSeconds(11) &&
        gauge->GetValue() == 42.0 &&
        summarySnapshot.Count() == 1 &&
        summarySnapshot.Sum() == 3.0;
}

} // extern "C"
