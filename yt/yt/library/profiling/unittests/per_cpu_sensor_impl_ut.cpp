#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/library/profiling/per_cpu_sensor_impl.h>

#include <library/cpp/yt/memory/new.h>

#include <util/system/types.h>

#include <algorithm>
#include <barrier>
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

TEST(TPerCpuSensorTest, SummarySupportsReadResetAndRecordAfterReset)
{
    auto summary = New<TPerCpuSummary<double>>();

    auto initial = summary->GetSummary();
    EXPECT_EQ(initial.Count(), 0);
    EXPECT_DOUBLE_EQ(initial.Sum(), 0.0);

    summary->Record(2.0);
    summary->Record(3.0);

    auto current = summary->GetSummary();
    EXPECT_EQ(current.Count(), 2);
    EXPECT_DOUBLE_EQ(current.Sum(), 5.0);

    auto drained = summary->GetSummaryAndReset();
    EXPECT_EQ(drained.Count(), 2);
    EXPECT_DOUBLE_EQ(drained.Sum(), 5.0);

    auto empty = summary->GetSummaryAndReset();
    EXPECT_EQ(empty.Count(), 0);
    EXPECT_DOUBLE_EQ(empty.Sum(), 0.0);

    summary->Record(7.0);

    auto afterReset = summary->GetSummaryAndReset();
    EXPECT_EQ(afterReset.Count(), 1);
    EXPECT_DOUBLE_EQ(afterReset.Sum(), 7.0);
}

TEST(TPerCpuSensorTest, SummaryConcurrentDrainConservesSamples)
{
    auto summary = New<TPerCpuSummary<double>>();

    constexpr int IterationCount = 10'000;
    std::barrier barrier(2);

    std::thread writer([&] {
        for (int index = 0; index < IterationCount; ++index) {
            barrier.arrive_and_wait();
            summary->Record(static_cast<double>(index + 1));
        }
    });

    i64 drainedCount = 0;
    double drainedSum = 0.0;
    for (int index = 0; index < IterationCount; ++index) {
        barrier.arrive_and_wait();
        auto drained = summary->GetSummaryAndReset();
        drainedCount += drained.Count();
        drainedSum += drained.Sum();
    }

    writer.join();

    auto tail = summary->GetSummaryAndReset();
    drainedCount += tail.Count();
    drainedSum += tail.Sum();

    EXPECT_EQ(drainedCount, IterationCount);
    EXPECT_DOUBLE_EQ(drainedSum, static_cast<double>(IterationCount) * (IterationCount + 1) / 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
