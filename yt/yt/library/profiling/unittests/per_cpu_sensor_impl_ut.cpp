#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/library/profiling/per_cpu_sensor_impl.h>

#include <library/cpp/yt/memory/new.h>

#include <util/system/types.h>

#include <algorithm>
#include <thread>
#include <vector>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

// The atomic sharded sensors (per_cpu_sensor_impl.h). The rseq-backed counterparts are
// covered, with the same checks, by rseq_sensor_impl_ut.cpp.

TEST(TPerCpuSensorTest, CounterAccumulates)
{
    auto counter = New<TPerCpuCounter>();
    counter->Increment(1'000'000'000'000LL);
    counter->Increment(-7);
    counter->Increment(-1'000'000'000'000LL);
    EXPECT_EQ(counter->GetValue(), -7);
}

TEST(TPerCpuSensorTest, CounterConcurrentNoLostUpdates)
{
    auto counter = New<TPerCpuCounter>();

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

TEST(TPerCpuSensorTest, TimeCounterAccumulates)
{
    auto counter = New<TPerCpuTimeCounter>();
    counter->Add(TDuration::MicroSeconds(10));
    counter->Add(TDuration::MicroSeconds(5));
    EXPECT_EQ(counter->GetValue(), TDuration::MicroSeconds(15));
}

TEST(TPerCpuSensorTest, GaugePublishesLastValue)
{
    auto gauge = New<TPerCpuGauge>();
    gauge->Update(1.0);
    gauge->Update(42.0);
    EXPECT_EQ(gauge->GetValue(), 42.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
