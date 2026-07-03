#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/library/profiling/simple_sensor_impl.h>

#include <library/cpp/yt/memory/new.h>

#include <util/system/types.h>

#include <algorithm>
#include <thread>
#include <vector>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

// The simple, single-atomic sensors (simple_sensor_impl.h) used for non-hot sensors and as the
// off-Linux hot fallback.

// TSimpleCounter is monotonic (it verifies delta >= 0), unlike the sharded counters.
TEST(TSimpleSensorTest, CounterAccumulates)
{
    auto counter = New<TSimpleCounter>();
    counter->Increment(3);
    counter->Increment(1'000'000'000'000LL);
    counter->Increment(7);
    EXPECT_EQ(counter->GetValue(), 1'000'000'000'010LL);
}

TEST(TSimpleSensorTest, CounterConcurrentNoLostUpdates)
{
    auto counter = New<TSimpleCounter>();

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

TEST(TSimpleSensorTest, TimeCounterAccumulates)
{
    auto counter = New<TSimpleTimeCounter>();
    counter->Add(TDuration::MicroSeconds(10));
    counter->Add(TDuration::MicroSeconds(5));
    EXPECT_EQ(counter->GetValue(), TDuration::MicroSeconds(15));
}

TEST(TSimpleSensorTest, GaugePublishesLastValue)
{
    auto gauge = New<TSimpleGauge>();
    gauge->Update(1.0);
    gauge->Update(42.0);
    EXPECT_EQ(gauge->GetValue(), 42.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
