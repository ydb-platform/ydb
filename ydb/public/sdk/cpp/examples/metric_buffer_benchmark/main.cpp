#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metric_buffer.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metrics.h>

#include <library/cpp/getopt/last_getopt.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace NYdb;
using namespace NYdb::NMetrics;
using namespace NYdb::NObservability;

namespace {

class TBenchCounter : public ICounter {
public:
    void Inc() override {
        Value_.fetch_add(1, std::memory_order_relaxed);
        IncCalls_.fetch_add(1, std::memory_order_relaxed);
    }
    void Add(std::uint64_t delta) override {
        if (delta == 0) return;
        Value_.fetch_add(delta, std::memory_order_relaxed);
        AddCalls_.fetch_add(1, std::memory_order_relaxed);
    }
    std::uint64_t Get() const { return Value_.load(); }
    std::uint64_t IncCalls() const { return IncCalls_.load(); }
    std::uint64_t AddCalls() const { return AddCalls_.load(); }

private:
    std::atomic<std::uint64_t> Value_{0};
    std::atomic<std::uint64_t> IncCalls_{0};
    std::atomic<std::uint64_t> AddCalls_{0};
};

class TBenchHistogram : public IHistogram {
public:
    void Record(double value) override {
        (void)value;
        Count_.fetch_add(1, std::memory_order_relaxed);
        RecordCalls_.fetch_add(1, std::memory_order_relaxed);
    }
    void RecordMany(const std::vector<double>& values) override {
        if (values.empty()) return;
        Count_.fetch_add(values.size(), std::memory_order_relaxed);
        RecordManyCalls_.fetch_add(1, std::memory_order_relaxed);
    }
    std::uint64_t Count() const {
        return Count_.load(std::memory_order_relaxed);
    }
    std::uint64_t RecordCalls() const {
        return RecordCalls_.load(std::memory_order_relaxed);
    }
    std::uint64_t RecordManyCalls() const {
        return RecordManyCalls_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<std::uint64_t> Count_{0};
    std::atomic<std::uint64_t> RecordCalls_{0};
    std::atomic<std::uint64_t> RecordManyCalls_{0};
};

class TBenchGauge : public IGauge {
public:
    void Add(double delta) override { Value_ += delta; }
    void Set(double value) override { Value_ = value; }
    double Get() const { return Value_; }
private:
    double Value_ = 0.0;
};

class TBenchRegistry : public IMetricRegistry {
public:
    std::shared_ptr<ICounter> Counter(const std::string& name,
                                       const TLabels& labels,
                                       const std::string&,
                                       const std::string&) override {
        std::lock_guard<std::mutex> lock(Mu_);
        auto& slot = Counters_[Key(name, labels)];
        if (!slot) slot = std::make_shared<TBenchCounter>();
        return slot;
    }
    std::shared_ptr<IHistogram> Histogram(const std::string& name,
                                           const std::vector<double>&,
                                           const TLabels& labels,
                                           const std::string&,
                                           const std::string&) override {
        std::lock_guard<std::mutex> lock(Mu_);
        auto& slot = Histograms_[Key(name, labels)];
        if (!slot) slot = std::make_shared<TBenchHistogram>();
        return slot;
    }
    std::shared_ptr<IGauge> Gauge(const std::string& name,
                                   const TLabels& labels,
                                   const std::string&,
                                   const std::string&) override {
        std::lock_guard<std::mutex> lock(Mu_);
        auto& slot = Gauges_[Key(name, labels)];
        if (!slot) slot = std::make_shared<TBenchGauge>();
        return slot;
    }

    std::shared_ptr<TBenchCounter> GetCounter(const std::string& name,
                                                const TLabels& labels = {}) const {
        std::lock_guard<std::mutex> lock(Mu_);
        auto it = Counters_.find(Key(name, labels));
        return it == Counters_.end() ? nullptr : it->second;
    }
    std::shared_ptr<TBenchHistogram> GetHistogram(const std::string& name,
                                                    const TLabels& labels = {}) const {
        std::lock_guard<std::mutex> lock(Mu_);
        auto it = Histograms_.find(Key(name, labels));
        return it == Histograms_.end() ? nullptr : it->second;
    }

private:
    static std::string Key(const std::string& name, const TLabels& labels) {
        std::string k = name;
        for (const auto& [a, b] : labels) {
            k.push_back('|');
            k.append(a);
            k.push_back('=');
            k.append(b);
        }
        return k;
    }
    mutable std::mutex Mu_;
    std::map<std::string, std::shared_ptr<TBenchCounter>>   Counters_;
    std::map<std::string, std::shared_ptr<TBenchHistogram>> Histograms_;
    std::map<std::string, std::shared_ptr<TBenchGauge>>     Gauges_;
};

struct TResult {
    std::string Mode;
    std::uint64_t TotalOps = 0;
    std::uint64_t IncCalls = 0;
    std::uint64_t AddCalls = 0;
    std::uint64_t RecordCalls = 0;
    std::uint64_t RecordManyCalls = 0;
    double DurationMs = 0.0;
};

TResult RunWorkload(const std::string& mode,
                     int threads,
                     std::uint64_t opsPerThread,
                     std::shared_ptr<IMetricRegistry> registry,
                     std::shared_ptr<TBenchRegistry> sink) {
    auto counter = registry->Counter("bench.counter", {}, "", "");
    auto hist = registry->Histogram("bench.histogram",
                                     {0.001, 0.01, 0.1, 1.0, 10.0},
                                     {}, "", "");

    std::atomic<bool> go{false};

    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            while (!go.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (std::uint64_t i = 0; i < opsPerThread; ++i) {
                counter->Inc();
                hist->Record(static_cast<double>(i % 1000) * 0.001
                              + static_cast<double>(t) * 0.0001);
            }
        });
    }

    const auto t0 = std::chrono::steady_clock::now();
    go.store(true, std::memory_order_release);

    for (auto& w : workers) {
        w.join();
    }

    counter.reset();
    hist.reset();
    registry.reset();

    const auto duration = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - t0).count();

    auto sinkCounter = sink->GetCounter("bench.counter", {});
    auto sinkHist = sink->GetHistogram("bench.histogram", {});

    TResult r;
    r.Mode = mode;
    r.TotalOps = static_cast<std::uint64_t>(threads) * opsPerThread;
    r.IncCalls = sinkCounter ? sinkCounter->IncCalls() : 0;
    r.AddCalls = sinkCounter ? sinkCounter->AddCalls() : 0;
    r.RecordCalls = sinkHist ? sinkHist->RecordCalls() : 0;
    r.RecordManyCalls = sinkHist ? sinkHist->RecordManyCalls() : 0;
    r.DurationMs = duration;
    return r;
}

void PrintRow(const TResult& r) {
    const double thr = r.DurationMs > 0
        ? (static_cast<double>(r.TotalOps) * 2.0 * 1000.0 / r.DurationMs)
        : 0.0;

    const std::uint64_t underlying = r.IncCalls + r.AddCalls
        + r.RecordCalls + r.RecordManyCalls;
    const double coalesce = underlying > 0
        ? (static_cast<double>(r.TotalOps) * 2.0 / static_cast<double>(underlying))
        : 0.0;

    std::cout
        << std::left << std::setw(10) << r.Mode
        << "  total_ops=" << std::setw(10) << (r.TotalOps * 2)
        << "  duration_ms=" << std::fixed << std::setprecision(1) << std::setw(8) << r.DurationMs
        << "  throughput=" << std::setprecision(0) << std::setw(11) << thr << " ops/s"
        << "\n            "
        << "  counter[Inc=" << r.IncCalls << ", Add=" << r.AddCalls << "]"
        << "  histogram[Record=" << r.RecordCalls << ", RecordMany=" << r.RecordManyCalls << "]"
        << "  coalesce=" << std::setprecision(2) << coalesce << "x"
        << std::endl;
}

} // namespace

int main(int argc, char** argv) {
    int threads = 8;
    std::uint64_t ops = 200'000;
    int flushMs = 100;
    bool runDirect = true;
    bool runBuffered = true;

    NLastGetopt::TOpts opts;
    opts.AddLongOption("threads", "Number of concurrent worker threads")
        .DefaultValue(std::to_string(threads)).StoreResult(&threads);
    opts.AddLongOption("ops", "Counter+histogram updates per worker thread")
        .DefaultValue(std::to_string(ops)).StoreResult(&ops);
    opts.AddLongOption("flush-ms",
                       "TMetricBuffer FlushInterval (ms) for the buffered mode")
        .DefaultValue(std::to_string(flushMs)).StoreResult(&flushMs);
    opts.AddLongOption("no-direct", "Skip the baseline 'direct' run").NoArgument()
        .Handler0([&]{ runDirect = false; });
    opts.AddLongOption("no-buffered", "Skip the 'buffered' run").NoArgument()
        .Handler0([&]{ runBuffered = false; });
    NLastGetopt::TOptsParseResult(&opts, argc, argv);

    std::cout
        << "TMetricBuffer micro-benchmark\n"
        << "  threads               = " << threads << "\n"
        << "  ops_per_thread        = " << ops << "\n"
        << "  flush_interval_ms     = " << flushMs << "\n"
        << "  (each op = 1 Inc() + 1 Record())\n"
        << std::endl;

    std::cout
        << std::left << std::setw(10) << "mode"
        << "  result"
        << std::endl;
    std::cout << std::string(100, '-') << std::endl;

    if (runDirect) {
        auto sink = std::make_shared<TBenchRegistry>();
        auto registry = std::static_pointer_cast<IMetricRegistry>(sink);
        auto r = RunWorkload("direct", threads, ops, registry, sink);
        PrintRow(r);
    }

    if (runBuffered) {
        auto sink = std::make_shared<TBenchRegistry>();
        TMetricBufferSettings settings;
        settings.FlushInterval = std::chrono::milliseconds(flushMs);
        auto registry = CreateBufferedMetricRegistry(sink, settings);
        auto r = RunWorkload("buffered", threads, ops, registry, sink);
        PrintRow(r);
    }

    return 0;
}
