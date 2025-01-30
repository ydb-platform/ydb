#include <gtest/gtest.h>

#include <yt/yt/library/profiling/perf/event_counter.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/proc.h>

#include <util/system/yield.h>

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

////////////////////////////////////////////////////////////////////////////////

i64 ReadPerfEventCounter(EPerfEventType type)
{
    auto counter = CreatePerfEventCounter(type);
    return counter->Read();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPerfEventCounterTest, CpuCycles)
{
    IgnorePermissionError([&] {
        ASSERT_GE(ReadPerfEventCounter(EPerfEventType::CpuCycles), 0);
    });
}

TEST(TPerfEventCounterTest, ContextSwitches)
{
    IgnorePermissionError([&] {
        for (int i = 0; i < 10; i++) {
            SchedYield();
        }
        ASSERT_GE(ReadPerfEventCounter(EPerfEventType::ContextSwitches), 0);
    });
}

TEST(TPerfEventCounterTest, CounterError)
{
    ASSERT_THROW(ReadPerfEventCounter(EPerfEventType::StalledCyclesBackend), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
