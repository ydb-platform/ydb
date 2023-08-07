#include "util/system/yield.h"
#include <gtest/gtest.h>

#include <yt/yt/library/profiling/perf/counters.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/proc.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TFn>
void IgnorePermissionError(const TFn& fn)
{
    try {
        fn();
    } catch (const TErrorException& ex) {
        constexpr auto PermissionErrorCode = TErrorCode(LinuxErrorCodeBase + EACCES);
        if (ex.Error().FindMatching(PermissionErrorCode)) {
            return;
        }

        throw;
    }
}

TEST(TPerfCounters, Cycles)
{
    IgnorePermissionError([&] {
        TPerfEventCounter counter(EPerfEventType::CpuCycles);
        ASSERT_GE(counter.Read(), 0u);
    });
}

TEST(TPerfCounters, ContextSwitches)
{
    IgnorePermissionError([&] {
        TPerfEventCounter counter(EPerfEventType::ContextSwitches);

        for (int i = 0; i < 10; i++) {
            SchedYield();
        }

        ASSERT_GE(counter.Read(), 0u);
    });
}

TEST(TPerfCounters, CounterError)
{
    auto createCounter = [] {
        TPerfEventCounter counter{EPerfEventType::StalledCyclesBackend};
        return 0;
    };

    ASSERT_THROW(createCounter(), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
