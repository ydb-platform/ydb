#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metric_buffer.h>
#include <ydb/public/sdk/cpp/tests/common/fake_metric_registry.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <random>
#include <string>
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

TFixture MakeFixture(TMetricBufferSettings settings) {
    auto fake = std::make_shared<TFakeMetricRegistry>();
    auto buffered = CreateBufferedMetricRegistry(fake, std::move(settings));
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

// ---------------------------------------------------------------------------
// Best-effort overflow handling and dropped updates accounting.
// ---------------------------------------------------------------------------

TEST(MetricBufferTest, OverflowDropsUpdatesAndReportsDroppedMetrics) {
    TMetricBufferSettings settings;
    settings.FlushInterval = std::chrono::seconds(60); // rely on shutdown drain
    settings.ThreadPendingThreshold = 0;
    settings.ThreadPendingLimit = 8;
    auto fix = MakeFixture(settings);

    auto counter = fix.Buffered->Counter(kCounter, {}, "", "");
    auto hist = fix.Buffered->Histogram(kHistogram, {1.0}, {}, "", "");

    for (int i = 0; i < 50; ++i) {
        counter->Inc();
        hist->Record(0.01 * i);
    }

    fix.Buffered.reset();

    auto fakeCounter = fix.Fake->GetCounter(kCounter, {});
    auto fakeHist = fix.Fake->GetHistogram(kHistogram, {});
    ASSERT_NE(fakeCounter, nullptr);
    ASSERT_NE(fakeHist, nullptr);
    EXPECT_LE(fakeCounter->Get(), 8);
    EXPECT_LE(fakeHist->Count(), 8u);

    auto droppedCounter = fix.Fake->GetCounter(
        "ydb_sdk_metric_buffer_dropped_updates_total", {{"instrument", "counter"}});
    auto droppedHistogram = fix.Fake->GetCounter(
        "ydb_sdk_metric_buffer_dropped_updates_total", {{"instrument", "histogram"}});
    ASSERT_NE(droppedCounter, nullptr);
    ASSERT_NE(droppedHistogram, nullptr);
    EXPECT_GT(droppedCounter->Get(), 0);
    EXPECT_GT(droppedHistogram->Get(), 0);
}

TEST(MetricBufferTest, ZeroPendingLimitDisablesDropping) {
    TMetricBufferSettings settings;
    settings.FlushInterval = std::chrono::seconds(60);
    settings.ThreadPendingThreshold = 0;
    settings.ThreadPendingLimit = 0; // unlimited
    auto fix = MakeFixture(settings);

    auto counter = fix.Buffered->Counter(kCounter, {}, "", "");
    for (int i = 0; i < 1000; ++i) {
        counter->Inc();
    }
    fix.Buffered.reset();

    auto fakeCounter = fix.Fake->GetCounter(kCounter, {});
    ASSERT_NE(fakeCounter, nullptr);
    EXPECT_EQ(fakeCounter->Get(), 1000);

    auto droppedCounter = fix.Fake->GetCounter(
        "ydb_sdk_metric_buffer_dropped_updates_total", {{"instrument", "counter"}});
    if (droppedCounter) {
        EXPECT_EQ(droppedCounter->Get(), 0);
    }
}

// ---------------------------------------------------------------------------
// Post-shutdown handle behavior: pass-through mode for still-live handles.
// ---------------------------------------------------------------------------

TEST(MetricBufferTest, LiveHandlesBecomePassThroughAfterBufferedRegistryDestruction) {
    auto fix = MakeFixture(std::chrono::seconds(60)); // do not auto-flush
    auto counter = fix.Buffered->Counter(kCounter, {}, "", "");
    auto hist = fix.Buffered->Histogram(kHistogram, {1.0}, {}, "", "");

    counter->Inc();      // buffered
    hist->Record(0.5);   // buffered

    // Destroy buffered registry first; handles remain alive.
    fix.Buffered.reset();

    // After shutdown handles should bypass the buffer.
    counter->Add(3);
    hist->Record(1.5);

    auto fakeCounter = fix.Fake->GetCounter(kCounter, {});
    auto fakeHist = fix.Fake->GetHistogram(kHistogram, {});
    ASSERT_NE(fakeCounter, nullptr);
    ASSERT_NE(fakeHist, nullptr);
    EXPECT_EQ(fakeCounter->Get(), 4); // 1 buffered + 3 pass-through
    EXPECT_EQ(fakeHist->Count(), 2u); // 1 buffered + 1 pass-through
}

// ---------------------------------------------------------------------------
// Race-ish test: concurrent registration and periodic flushing.
// ---------------------------------------------------------------------------

TEST(MetricBufferTest, ConcurrentMetricRegistrationAndFlushIsLossless) {
    auto fix = MakeFixture(std::chrono::milliseconds(5));

    constexpr int kThreads = 4;
    constexpr int kMetricsPerThread = 50;
    constexpr int kIncsPerMetric = 200;

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t] {
            for (int m = 0; m < kMetricsPerThread; ++m) {
                auto name = std::string("race.counter.") + std::to_string(t) + "." + std::to_string(m);
                auto c = fix.Buffered->Counter(name, {}, "", "");
                for (int i = 0; i < kIncsPerMetric; ++i) {
                    c->Inc();
                }
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }
    fix.Buffered.reset();

    const int64_t expected = static_cast<int64_t>(kIncsPerMetric);
    for (int t = 0; t < kThreads; ++t) {
        for (int m = 0; m < kMetricsPerThread; ++m) {
            auto name = std::string("race.counter.") + std::to_string(t) + "." + std::to_string(m);
            auto c = fix.Fake->GetCounter(name, {});
            ASSERT_NE(c, nullptr) << name;
            EXPECT_EQ(c->Get(), expected) << name;
        }
    }
}

// ---------------------------------------------------------------------------
// Flush worker resiliency: underlying metric calls may throw.
// ---------------------------------------------------------------------------

namespace {

class TThrowingCounter final : public ICounter {
public:
    explicit TThrowingCounter(std::atomic<std::uint64_t>* calls) : Calls_(calls) {}
    void Inc() override { throw std::runtime_error("inc throw"); }
    void Add(std::uint64_t) override {
        Calls_->fetch_add(1, std::memory_order_relaxed);
        throw std::runtime_error("add throw");
    }
private:
    std::atomic<std::uint64_t>* Calls_;
};

class TThrowingHistogram final : public IHistogram {
public:
    explicit TThrowingHistogram(std::atomic<std::uint64_t>* calls) : Calls_(calls) {}
    void Record(double) override { throw std::runtime_error("record throw"); }
    void RecordMany(const std::vector<double>&) override {
        Calls_->fetch_add(1, std::memory_order_relaxed);
        throw std::runtime_error("record many throw");
    }
private:
    std::atomic<std::uint64_t>* Calls_;
};

class TThrowingRegistry final : public IMetricRegistry {
public:
    std::shared_ptr<ICounter> Counter(const std::string&, const TLabels&, const std::string&, const std::string&) override {
        if (!Counter_) {
            Counter_ = std::make_shared<TThrowingCounter>(&CounterCalls_);
        }
        return Counter_;
    }
    std::shared_ptr<IGauge> Gauge(const std::string&, const TLabels&, const std::string&, const std::string&) override {
        return nullptr;
    }
    std::shared_ptr<IHistogram> Histogram(const std::string&, const std::vector<double>&, const TLabels&, const std::string&, const std::string&) override {
        if (!Histogram_) {
            Histogram_ = std::make_shared<TThrowingHistogram>(&HistogramCalls_);
        }
        return Histogram_;
    }
    std::uint64_t CounterCalls() const { return CounterCalls_.load(std::memory_order_relaxed); }
    std::uint64_t HistogramCalls() const { return HistogramCalls_.load(std::memory_order_relaxed); }
private:
    std::shared_ptr<TThrowingCounter> Counter_;
    std::shared_ptr<TThrowingHistogram> Histogram_;
    std::atomic<std::uint64_t> CounterCalls_{0};
    std::atomic<std::uint64_t> HistogramCalls_{0};
};

} // namespace

TEST(MetricBufferTest, FlushThreadSurvivesUnderlyingExceptions) {
    auto throwing = std::make_shared<TThrowingRegistry>();
    TMetricBufferSettings settings;
    settings.FlushInterval = std::chrono::milliseconds(2);
    settings.ThreadPendingThreshold = 4;
    auto buffered = CreateBufferedMetricRegistry(throwing, settings);

    auto c = buffered->Counter("throw.counter", {}, "", "");
    auto h = buffered->Histogram("throw.hist", {1.0}, {}, "", "");
    for (int i = 0; i < 100; ++i) {
        c->Inc();
        h->Record(0.1 * i);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_GT(throwing->CounterCalls(), 0u);
    EXPECT_GT(throwing->HistogramCalls(), 0u);

    EXPECT_NO_THROW(buffered.reset());
}

// ---------------------------------------------------------------------------
// Property-style invariant test for deterministic random operations.
// ---------------------------------------------------------------------------

TEST(MetricBufferTest, RandomOperationSequencePreservesTotalsWithoutDropping) {
    TMetricBufferSettings settings;
    settings.FlushInterval = std::chrono::milliseconds(3);
    settings.ThreadPendingThreshold = 128;
    settings.ThreadPendingLimit = 0;
    auto fix = MakeFixture(settings);

    auto counter = fix.Buffered->Counter(kCounter, {}, "", "");
    auto hist = fix.Buffered->Histogram(kHistogram, {1.0}, {}, "", "");

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> opDist(0, 3);
    std::uniform_int_distribution<int> addDist(1, 5);

    int64_t expectedCounter = 0;
    std::size_t expectedHist = 0;
    for (int i = 0; i < 5000; ++i) {
        const int op = opDist(rng);
        switch (op) {
            case 0:
                counter->Inc();
                ++expectedCounter;
                break;
            case 1: {
                const int d = addDist(rng);
                counter->Add(d);
                expectedCounter += d;
                break;
            }
            case 2:
                hist->Record(static_cast<double>(i) * 0.01);
                ++expectedHist;
                break;
            default: {
                std::vector<double> batch;
                const int n = addDist(rng);
                batch.reserve(n);
                for (int j = 0; j < n; ++j) {
                    batch.push_back(static_cast<double>(i + j) * 0.001);
                }
                hist->RecordMany(batch);
                expectedHist += batch.size();
                break;
            }
        }
    }

    fix.Buffered.reset();

    auto fakeCounter = fix.Fake->GetCounter(kCounter, {});
    auto fakeHist = fix.Fake->GetHistogram(kHistogram, {});
    ASSERT_NE(fakeCounter, nullptr);
    ASSERT_NE(fakeHist, nullptr);
    EXPECT_EQ(fakeCounter->Get(), expectedCounter);
    EXPECT_EQ(fakeHist->Count(), expectedHist);
}
