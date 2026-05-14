#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metric_buffer.h>
#include <ydb/public/sdk/cpp/tests/common/fake_metric_registry.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

using namespace NYdb;
using namespace NYdb::NObservability;
using namespace NYdb::NMetrics;
using namespace NYdb::NTests;

namespace {

constexpr const char kCounter[] = "test.counter";
constexpr const char kHistogram[] = "test.histogram";

struct TFixture {
    std::shared_ptr<TFakeMetricRegistry> Fake;
    std::shared_ptr<IMetricRegistry> Buffered;
};

TFixture MakeFixture(std::chrono::milliseconds flushInterval,
                      std::size_t threshold = 0) {
    auto fake = std::make_shared<TFakeMetricRegistry>();
    TMetricBufferSettings settings;
    settings.FlushInterval = flushInterval;
    settings.ThreadPendingThreshold = threshold;
    auto buffered = CreateBufferedMetricRegistry(fake, settings);
    return {std::move(fake), std::move(buffered)};
}

void SpinUntil(std::function<bool()> pred,
                std::chrono::milliseconds deadline = std::chrono::milliseconds(2000)) {
    const auto t0 = std::chrono::steady_clock::now();
    while (!pred()) {
        if (std::chrono::steady_clock::now() - t0 > deadline) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}

} // namespace

// ---------------------------------------------------------------------------
// Coalescing on a single thread.
// ---------------------------------------------------------------------------

TEST(MetricBufferTest, CounterIncsAreCoalescedIntoOneAddPerFlush) {
    auto fix = MakeFixture(std::chrono::seconds(10));

    auto counter = fix.Buffered->Counter(kCounter, {}, "", "");
    for (int i = 0; i < 1000; ++i) {
        counter->Inc();
    }

    auto fakeCounter = fix.Fake->GetCounter(kCounter, {});
    ASSERT_NE(fakeCounter, nullptr);
    EXPECT_EQ(fakeCounter->Get(), 0);
    EXPECT_EQ(fakeCounter->IncCalls(), 0u);
    EXPECT_EQ(fakeCounter->AddCalls(), 0u);

    fix.Buffered.reset();

    EXPECT_EQ(fakeCounter->Get(), 1000);
    EXPECT_EQ(fakeCounter->IncCalls(), 0u);
    EXPECT_EQ(fakeCounter->AddCalls(), 1u);
}

TEST(MetricBufferTest, HistogramRecordsAreCoalescedIntoOneRecordMany) {
    auto fix = MakeFixture(std::chrono::seconds(10));

    auto hist = fix.Buffered->Histogram(kHistogram, {0.1, 0.5, 1, 5}, {}, "", "");
    for (int i = 0; i < 200; ++i) {
        hist->Record(static_cast<double>(i) / 100.0);
    }

    auto fakeHist = fix.Fake->GetHistogram(kHistogram, {});
    ASSERT_NE(fakeHist, nullptr);
    EXPECT_EQ(fakeHist->Count(), 0u);
    EXPECT_EQ(fakeHist->RecordCalls(), 0u);
    EXPECT_EQ(fakeHist->RecordManyCalls(), 0u);

    fix.Buffered.reset();

    EXPECT_EQ(fakeHist->Count(), 200u);
    EXPECT_EQ(fakeHist->RecordCalls(), 0u);
    EXPECT_EQ(fakeHist->RecordManyCalls(), 1u);
}

// ---------------------------------------------------------------------------
// Interval-triggered flush.
// ---------------------------------------------------------------------------

TEST(MetricBufferTest, IntervalFlushDrainsPendingUpdates) {
    auto fix = MakeFixture(std::chrono::milliseconds(20));

    auto counter = fix.Buffered->Counter(kCounter, {}, "", "");
    counter->Add(7);
    counter->Inc();
    counter->Inc();

    auto fakeCounter = fix.Fake->GetCounter(kCounter, {});
    ASSERT_NE(fakeCounter, nullptr);

    SpinUntil([&]{ return fakeCounter->Get() == 9; });

    EXPECT_EQ(fakeCounter->Get(), 9);
    EXPECT_EQ(fakeCounter->IncCalls(), 0u);
    EXPECT_GE(fakeCounter->AddCalls(), 1u);
}

// ---------------------------------------------------------------------------
// Multi-threaded fan-in.
// ---------------------------------------------------------------------------

TEST(MetricBufferTest, MultiThreadedIncsAreLosslessAndAggregated) {
    constexpr int kThreads = 8;
    constexpr int kIncsPerThread = 25'000;

    auto fix = MakeFixture(std::chrono::milliseconds(10));
    auto counter = fix.Buffered->Counter(kCounter, {}, "", "");

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([counter] {
            for (int i = 0; i < kIncsPerThread; ++i) {
                counter->Inc();
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }

    fix.Buffered.reset();

    auto fakeCounter = fix.Fake->GetCounter(kCounter, {});
    ASSERT_NE(fakeCounter, nullptr);
    EXPECT_EQ(fakeCounter->Get(), static_cast<int64_t>(kThreads) * kIncsPerThread);
    EXPECT_EQ(fakeCounter->IncCalls(), 0u);

    EXPECT_GE(fakeCounter->AddCalls(), 1u);
    EXPECT_LE(fakeCounter->AddCalls(), static_cast<std::uint64_t>(kThreads * 100));
}

TEST(MetricBufferTest, MultiThreadedHistogramSamplesAreLosslessAndAggregated) {
    constexpr int kThreads = 4;
    constexpr int kRecordsPerThread = 10'000;

    auto fix = MakeFixture(std::chrono::milliseconds(10));
    auto hist = fix.Buffered->Histogram(kHistogram, {0.1, 1, 10}, {}, "", "");

    std::vector<std::thread> workers;
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([hist, t] {
            for (int i = 0; i < kRecordsPerThread; ++i) {
                hist->Record(static_cast<double>(t * 1000 + i));
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }

    fix.Buffered.reset();

    auto fakeHist = fix.Fake->GetHistogram(kHistogram, {});
    ASSERT_NE(fakeHist, nullptr);
    EXPECT_EQ(fakeHist->Count(),
              static_cast<std::size_t>(kThreads) * kRecordsPerThread);
    EXPECT_EQ(fakeHist->RecordCalls(), 0u);
    EXPECT_GE(fakeHist->RecordManyCalls(), 1u);
}

// ---------------------------------------------------------------------------
// Shutdown drain.
// ---------------------------------------------------------------------------

TEST(MetricBufferTest, ShutdownDrainsLeftoverData) {
    auto fix = MakeFixture(std::chrono::seconds(60)); // never fire by timer
    auto counter = fix.Buffered->Counter(kCounter, {}, "", "");
    auto hist = fix.Buffered->Histogram(kHistogram, {}, {}, "", "");

    counter->Add(100);
    hist->Record(0.5);
    hist->Record(1.5);

    auto fakeCounter = fix.Fake->GetCounter(kCounter, {});
    auto fakeHist = fix.Fake->GetHistogram(kHistogram, {});
    ASSERT_NE(fakeCounter, nullptr);
    ASSERT_NE(fakeHist, nullptr);

    EXPECT_EQ(fakeCounter->Get(), 0);
    EXPECT_EQ(fakeHist->Count(), 0u);

    fix.Buffered.reset();

    EXPECT_EQ(fakeCounter->Get(), 100);
    EXPECT_EQ(fakeHist->Count(), 2u);
}

// ---------------------------------------------------------------------------
// Threshold-triggered flush.
// ---------------------------------------------------------------------------

TEST(MetricBufferTest, ThresholdTriggersImmediateFlush) {
    auto fix = MakeFixture(std::chrono::seconds(60), /*threshold=*/128);
    auto counter = fix.Buffered->Counter(kCounter, {}, "", "");
    auto fakeCounter = fix.Fake->GetCounter(kCounter, {});
    ASSERT_NE(fakeCounter, nullptr);

    for (int i = 0; i < 100; ++i) {
        counter->Inc();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(fakeCounter->Get(), 0);

    for (int i = 0; i < 50; ++i) {
        counter->Inc();
    }
    SpinUntil([&]{ return fakeCounter->Get() == 150; });
    EXPECT_EQ(fakeCounter->Get(), 150);
}

// ---------------------------------------------------------------------------
// Repeat lookup returns the same wrapper instance.
// ---------------------------------------------------------------------------

TEST(MetricBufferTest, RepeatedLookupReturnsSameHandle) {
    auto fix = MakeFixture(std::chrono::seconds(60));

    auto a = fix.Buffered->Counter(kCounter, {}, "", "");
    auto b = fix.Buffered->Counter(kCounter, {}, "", "");
    EXPECT_EQ(a.get(), b.get());

    auto h1 = fix.Buffered->Histogram(kHistogram, {1.0}, {}, "", "");
    auto h2 = fix.Buffered->Histogram(kHistogram, {1.0}, {}, "", "");
    EXPECT_EQ(h1.get(), h2.get());
}

// ---------------------------------------------------------------------------
// Gauge passthrough semantics.
// ---------------------------------------------------------------------------

TEST(MetricBufferTest, GaugeIsPassThrough) {
    auto fix = MakeFixture(std::chrono::seconds(60));

    auto g = fix.Buffered->Gauge("test.gauge", {}, "", "");
    g->Set(42.0);
    g->Add(8.0);

    auto fakeGauge = fix.Fake->GetGauge("test.gauge", {});
    ASSERT_NE(fakeGauge, nullptr);
    EXPECT_DOUBLE_EQ(fakeGauge->Get(), 50.0);
}

// ---------------------------------------------------------------------------
// Self-observability metrics are wired up.
// ---------------------------------------------------------------------------

TEST(MetricBufferTest, SelfObservabilityMetricsAreEmitted) {
    auto fix = MakeFixture(std::chrono::milliseconds(10));

    auto counter = fix.Buffered->Counter(kCounter, {}, "", "");
    auto hist = fix.Buffered->Histogram(kHistogram, {1.0}, {}, "", "");
    counter->Add(50);
    for (int i = 0; i < 20; ++i) {
        hist->Record(0.1);
    }

    fix.Buffered.reset();

    auto buffered = fix.Fake->GetCounter("ydb_sdk_metric_buffer_events_buffered_total", {});
    ASSERT_NE(buffered, nullptr);
    EXPECT_EQ(buffered->Get(), 70);

    auto addCalls = fix.Fake->GetCounter(
        "ydb_sdk_metric_buffer_underlying_calls_total", {{"kind", "add"}});
    ASSERT_NE(addCalls, nullptr);
    EXPECT_GE(addCalls->Get(), 1);

    auto recordManyCalls = fix.Fake->GetCounter(
        "ydb_sdk_metric_buffer_underlying_calls_total", {{"kind", "record_many"}});
    ASSERT_NE(recordManyCalls, nullptr);
    EXPECT_GE(recordManyCalls->Get(), 1);

    auto shutdownFlushes = fix.Fake->GetCounter(
        "ydb_sdk_metric_buffer_flushes_total", {{"trigger", "shutdown"}});
    ASSERT_NE(shutdownFlushes, nullptr);
    EXPECT_GE(shutdownFlushes->Get(), 1);
}
