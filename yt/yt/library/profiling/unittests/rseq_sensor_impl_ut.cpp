#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/library/profiling/rseq_sensor_impl.h>

#include <library/cpp/yt/memory/new.h>

#include <util/system/types.h>

#include <algorithm>
#include <thread>
#include <vector>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

// The rseq-backed sharded sensors (rseq_sensor_impl.h, Linux-only). Mirrors
// per_cpu_sensor_impl_ut.cpp so the two interchangeable hot implementations stay in sync.

TEST(TRseqSensorTest, CounterAccumulates)
{
    auto counter = TRseqCounter::Create();
    counter->Increment(1'000'000'000'000LL);
    counter->Increment(-7);
    counter->Increment(-1'000'000'000'000LL);
    EXPECT_EQ(counter->GetValue(), -7);
}

// The core guarantee: across many threads (which the scheduler migrates between CPUs,
// exercising rseq aborts/restarts), not a single increment is lost.
TEST(TRseqSensorTest, CounterConcurrentNoLostUpdates)
{
    auto counter = TRseqCounter::Create();

    int threadCount = std::max<int>(4, std::thread::hardware_concurrency());
    constexpr i64 IterationCount = 1'000'000;

    std::vector<std::thread> threads;
    for (int index = 0; index < threadCount; ++index) {
        threads.emplace_back([&] {
            for (i64 i = 0; i < IterationCount; ++i) {
                counter->Increment(1);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(counter->GetValue(), static_cast<i64>(threadCount) * IterationCount);
}

TEST(TRseqSensorTest, TimeCounterAccumulates)
{
    auto counter = TRseqTimeCounter::Create();
    counter->Add(TDuration::MicroSeconds(10));
    counter->Add(TDuration::MicroSeconds(5));
    EXPECT_EQ(counter->GetValue(), TDuration::MicroSeconds(15));
}

TEST(TRseqSensorTest, GaugePublishesLastValue)
{
    auto gauge = TRseqGauge::Create();
    gauge->Update(1.0);
    gauge->Update(42.0);
    EXPECT_EQ(gauge->GetValue(), 42.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
