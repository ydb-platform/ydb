#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metric_buffer.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace NYdb::inline Dev::NObservability {

namespace {

using NMetrics::ICounter;
using NMetrics::IGauge;
using NMetrics::IHistogram;
using NMetrics::IMetricRegistry;
using NMetrics::TLabels;

constexpr const char* kFlushDurationMetric = "ydb_sdk_metric_buffer_flush_duration_seconds";
constexpr const char* kFlushesTotalMetric  = "ydb_sdk_metric_buffer_flushes_total";
constexpr const char* kEventsBufferedTotal = "ydb_sdk_metric_buffer_events_buffered_total";
constexpr const char* kUnderlyingCallsTotal= "ydb_sdk_metric_buffer_underlying_calls_total";
constexpr const char* kPendingUpdatesMetric= "ydb_sdk_metric_buffer_pending_updates";

const std::vector<double>& FlushDurationBuckets() {
    static const std::vector<double> kBuckets = {
        0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0
    };
    return kBuckets;
}

// ---------------------------------------------------------------------------
// TMetricBufferCore
//
// Owns:
//   * the underlying IMetricRegistry pointer (where flushed data goes);
//   * the table of registered handles (counter / histogram) and their stable
//     ids — ids index into the per-thread vectors;
//   * the list of per-thread state slots;
//   * the background flush thread.
// ---------------------------------------------------------------------------

class TMetricBufferCore : public std::enable_shared_from_this<TMetricBufferCore> {
public:
    enum class EFlushTrigger {
        Interval,
        Threshold,
        Manual,
        Shutdown,
    };

    // Stable, monotonically assigned handle ids. We use a single index space
    // per kind to keep per-thread vectors compact.
    struct TCounterHandleInfo {
        std::shared_ptr<ICounter> Underlying;
    };
    struct THistogramHandleInfo {
        std::shared_ptr<IHistogram> Underlying;
    };

    // Per-thread state. Lives inside a thread_local shared_ptr; the buffer
    // core holds a strong reference too, so the state survives thread exit
    // until at least one more flush has drained any leftover data.
    struct TThreadState {
        std::mutex Mutex;
        std::vector<std::uint64_t> CounterDeltas;
        std::vector<std::vector<double>> HistogramSamples;
        std::atomic<std::size_t> PendingOps{0};
        std::atomic<bool> Active{true}; // becomes false when owning thread exits
    };

    explicit TMetricBufferCore(std::shared_ptr<IMetricRegistry> underlying,
                                TMetricBufferSettings settings)
        : Underlying_(std::move(underlying))
        , Settings_(std::move(settings))
    {
        if (!Settings_.SelfMetricsRegistry) {
            Settings_.SelfMetricsRegistry = Underlying_;
        }
        InitSelfMetrics();
    }

    void Start() {
        // NOTE: capture `this` raw rather than a shared_ptr — the worker
        // must NOT extend the lifetime of the core, otherwise the last
        // external shared_ptr release would leave the refcount stuck at 1
        // (the lambda) and the destructor would never run, which is the
        // very thing that joins the worker. The destructor is responsible
        // for setting Stopping_ and joining the thread before the object
        // becomes invalid, so capturing `this` is safe by construction.
        FlushThread_ = std::thread([this] {
            Run();
        });
    }

    ~TMetricBufferCore() {
        Shutdown();
    }

    void Shutdown() noexcept {
        bool expected = false;
        if (!Stopping_.compare_exchange_strong(expected, true)) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(WaitMutex_);
            Wakeup_.notify_all();
        }
        if (FlushThread_.joinable()) {
            try {
                FlushThread_.join();
            } catch (...) {
                // best-effort
            }
        }
        // One final drain on the calling thread to capture anything pushed
        // after the worker exited (e.g. during static destruction).
        FlushAll(EFlushTrigger::Shutdown);
    }

    // -- Handle registration --------------------------------------------------

    std::size_t RegisterCounter(std::shared_ptr<ICounter> underlying) {
        std::lock_guard<std::mutex> lock(HandlesMutex_);
        Counters_.push_back({std::move(underlying)});
        return Counters_.size() - 1;
    }

    std::size_t RegisterHistogram(std::shared_ptr<IHistogram> underlying) {
        std::lock_guard<std::mutex> lock(HandlesMutex_);
        Histograms_.push_back({std::move(underlying)});
        return Histograms_.size() - 1;
    }

    // -- Hot-path entry points ------------------------------------------------

    void OnCounterAdd(std::size_t handle, std::uint64_t delta) {
        if (delta == 0) {
            return;
        }
        // After Shutdown() the buffer becomes a transparent pass-through:
        // any updates from still-living handle wrappers go straight to the
        // underlying counter so we don't silently accumulate data that will
        // never be flushed. This also matters for graceful teardown in the
        // SDK where the buffered registry is released before the last
        // counter handle held by TStatCollector.
        if (Stopping_.load(std::memory_order_acquire)) {
            std::shared_ptr<ICounter> underlying;
            {
                std::lock_guard<std::mutex> lock(HandlesMutex_);
                if (handle < Counters_.size()) {
                    underlying = Counters_[handle].Underlying;
                }
            }
            if (underlying) {
                underlying->Add(delta);
            }
            return;
        }
        TThreadState& state = AcquireThreadState();
        bool overThreshold = false;
        {
            std::lock_guard<std::mutex> lock(state.Mutex);
            if (state.CounterDeltas.size() <= handle) {
                state.CounterDeltas.resize(handle + 1, 0);
            }
            state.CounterDeltas[handle] += delta;
        }
        const auto pending = state.PendingOps.fetch_add(1, std::memory_order_relaxed) + 1;
        if (Settings_.ThreadPendingThreshold != 0
            && pending >= Settings_.ThreadPendingThreshold) {
            overThreshold = true;
        }
        if (overThreshold) {
            TriggerFlush(EFlushTrigger::Threshold);
        }
    }

    void OnHistogramRecord(std::size_t handle, double value) {
        if (Stopping_.load(std::memory_order_acquire)) {
            std::shared_ptr<IHistogram> underlying;
            {
                std::lock_guard<std::mutex> lock(HandlesMutex_);
                if (handle < Histograms_.size()) {
                    underlying = Histograms_[handle].Underlying;
                }
            }
            if (underlying) {
                underlying->Record(value);
            }
            return;
        }
        TThreadState& state = AcquireThreadState();
        bool overThreshold = false;
        {
            std::lock_guard<std::mutex> lock(state.Mutex);
            if (state.HistogramSamples.size() <= handle) {
                state.HistogramSamples.resize(handle + 1);
            }
            auto& bucket = state.HistogramSamples[handle];
            if (bucket.capacity() == 0 && Settings_.HistogramReserveSamples > 0) {
                bucket.reserve(Settings_.HistogramReserveSamples);
            }
            bucket.push_back(value);
        }
        const auto pending = state.PendingOps.fetch_add(1, std::memory_order_relaxed) + 1;
        if (Settings_.ThreadPendingThreshold != 0
            && pending >= Settings_.ThreadPendingThreshold) {
            overThreshold = true;
        }
        if (overThreshold) {
            TriggerFlush(EFlushTrigger::Threshold);
        }
    }

    void OnHistogramRecordMany(std::size_t handle, const std::vector<double>& values) {
        if (values.empty()) {
            return;
        }
        if (Stopping_.load(std::memory_order_acquire)) {
            std::shared_ptr<IHistogram> underlying;
            {
                std::lock_guard<std::mutex> lock(HandlesMutex_);
                if (handle < Histograms_.size()) {
                    underlying = Histograms_[handle].Underlying;
                }
            }
            if (underlying) {
                underlying->RecordMany(values);
            }
            return;
        }
        TThreadState& state = AcquireThreadState();
        bool overThreshold = false;
        {
            std::lock_guard<std::mutex> lock(state.Mutex);
            if (state.HistogramSamples.size() <= handle) {
                state.HistogramSamples.resize(handle + 1);
            }
            auto& bucket = state.HistogramSamples[handle];
            bucket.insert(bucket.end(), values.begin(), values.end());
        }
        const auto pending = state.PendingOps.fetch_add(values.size(), std::memory_order_relaxed) + values.size();
        if (Settings_.ThreadPendingThreshold != 0
            && pending >= Settings_.ThreadPendingThreshold) {
            overThreshold = true;
        }
        if (overThreshold) {
            TriggerFlush(EFlushTrigger::Threshold);
        }
    }

    // -- Public flush --------------------------------------------------------

    void Flush() {
        FlushAll(EFlushTrigger::Manual);
    }

private:
    // Each thread has its own state object kept alive by a thread_local
    // shared_ptr inside this helper. We register a weak reference into
    // ThreadStates_ on first acquisition so the flush thread can iterate
    // all live threads. On thread exit the shared_ptr held by the thread
    // is released, but a strong copy remains in ThreadStates_ until the
    // next flush drains the leftover data and the buffer's destructor or
    // a subsequent scrub purges the slot.
    struct TThreadLocalHolder {
        std::shared_ptr<TThreadState> State;
        std::weak_ptr<TMetricBufferCore> Owner;

        ~TThreadLocalHolder() {
            if (State) {
                State->Active.store(false, std::memory_order_release);
            }
            if (auto owner = Owner.lock()) {
                owner->NudgeOnThreadExit();
            }
        }
    };

    TThreadState& AcquireThreadState() {
        // Pointer to the per-thread state for this specific buffer instance.
        // We key the thread_local on the buffer's `this` so multiple
        // TMetricBufferCore instances coexist in one process.
        thread_local std::vector<std::pair<TMetricBufferCore*, std::shared_ptr<TThreadState>>>
            tlsTable;

        for (auto& kv : tlsTable) {
            if (kv.first == this) {
                return *kv.second;
            }
        }

        auto state = std::make_shared<TThreadState>();
        {
            std::lock_guard<std::mutex> lock(ThreadsMutex_);
            ThreadStates_.push_back(state);
        }
        tlsTable.emplace_back(this, state);

        // Holder is alive as long as the thread; on destruction marks the
        // state inactive and wakes the flush thread so the leftover data
        // is drained promptly.
        thread_local std::vector<TThreadLocalHolder> holders;
        holders.push_back(TThreadLocalHolder{state, weak_from_this()});

        return *state;
    }

    void NudgeOnThreadExit() noexcept {
        std::lock_guard<std::mutex> lock(WaitMutex_);
        Wakeup_.notify_all();
    }

    void TriggerFlush(EFlushTrigger trigger) noexcept {
        // Coalesce manual / threshold triggers via a flag — the worker
        // checks it on wake and performs the flush itself.
        ManualTrigger_.store(true, std::memory_order_release);
        LastManualTrigger_.store(static_cast<int>(trigger), std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(WaitMutex_);
        Wakeup_.notify_all();
    }

    void Run() noexcept {
        while (!Stopping_.load(std::memory_order_acquire)) {
            EFlushTrigger trigger = EFlushTrigger::Interval;
            {
                std::unique_lock<std::mutex> lock(WaitMutex_);
                Wakeup_.wait_for(lock, Settings_.FlushInterval, [this]{
                    return Stopping_.load(std::memory_order_acquire)
                        || ManualTrigger_.load(std::memory_order_acquire);
                });
                if (Stopping_.load(std::memory_order_acquire)) {
                    break;
                }
                if (ManualTrigger_.exchange(false, std::memory_order_acq_rel)) {
                    trigger = static_cast<EFlushTrigger>(
                        LastManualTrigger_.load(std::memory_order_relaxed));
                }
            }
            try {
                FlushAll(trigger);
            } catch (...) {
                // Telemetry buffers must never throw out of the worker. Drop
                // exceptions and keep the loop alive — losing one tick of
                // metrics is far better than terminating the process.
            }
        }
    }

    // Drains all per-thread buckets and pushes accumulated data to the
    // underlying registry. Safe to call from any thread (manual Flush /
    // shutdown), but typically runs on the background worker.
    void FlushAll(EFlushTrigger trigger) {
        const auto t0 = std::chrono::steady_clock::now();

        std::vector<std::shared_ptr<TThreadState>> snapshot;
        {
            std::lock_guard<std::mutex> lock(ThreadsMutex_);
            snapshot = ThreadStates_;
            // Purge slots whose owning thread has exited and which have no
            // pending data, so they don't accumulate indefinitely.
            if (!ThreadStates_.empty()) {
                ThreadStates_.erase(std::remove_if(ThreadStates_.begin(), ThreadStates_.end(),
                    [](const std::shared_ptr<TThreadState>& st) {
                        if (st->Active.load(std::memory_order_acquire)) {
                            return false;
                        }
                        std::lock_guard<std::mutex> lk(st->Mutex);
                        if (st->PendingOps.load(std::memory_order_acquire) != 0) {
                            return false;
                        }
                        // Inactive and empty — release the strong reference.
                        return true;
                    }), ThreadStates_.end());
            }
        }

        // Pull a snapshot of the handle tables. Handles are append-only,
        // so size() is a valid upper bound.
        std::vector<std::shared_ptr<ICounter>> counters;
        std::vector<std::shared_ptr<IHistogram>> histograms;
        {
            std::lock_guard<std::mutex> lock(HandlesMutex_);
            counters.reserve(Counters_.size());
            for (auto& c : Counters_) {
                counters.push_back(c.Underlying);
            }
            histograms.reserve(Histograms_.size());
            for (auto& h : Histograms_) {
                histograms.push_back(h.Underlying);
            }
        }

        // Aggregate across threads first so each underlying counter sees
        // at most one Add(N) call per flush.
        std::vector<std::uint64_t> totalCounter(counters.size(), 0);
        std::vector<std::vector<double>> totalSamples(histograms.size());
        std::uint64_t totalEvents = 0;
        std::uint64_t pendingCounters = 0;
        std::uint64_t pendingHistogramSamples = 0;

        for (const auto& state : snapshot) {
            std::lock_guard<std::mutex> lock(state->Mutex);
            for (std::size_t i = 0; i < state->CounterDeltas.size() && i < counters.size(); ++i) {
                if (state->CounterDeltas[i] != 0) {
                    totalCounter[i] += state->CounterDeltas[i];
                    totalEvents += state->CounterDeltas[i];
                    pendingCounters += state->CounterDeltas[i];
                    state->CounterDeltas[i] = 0;
                }
            }
            for (std::size_t i = 0; i < state->HistogramSamples.size() && i < histograms.size(); ++i) {
                auto& src = state->HistogramSamples[i];
                if (!src.empty()) {
                    auto& dst = totalSamples[i];
                    pendingHistogramSamples += src.size();
                    dst.insert(dst.end(),
                               std::make_move_iterator(src.begin()),
                               std::make_move_iterator(src.end()));
                    totalEvents += src.size();
                    src.clear();
                }
            }
            state->PendingOps.store(0, std::memory_order_release);
        }

        std::uint64_t addCalls = 0;
        for (std::size_t i = 0; i < counters.size(); ++i) {
            if (totalCounter[i] != 0 && counters[i]) {
                counters[i]->Add(totalCounter[i]);
                ++addCalls;
            }
        }
        std::uint64_t recordManyCalls = 0;
        for (std::size_t i = 0; i < histograms.size(); ++i) {
            if (!totalSamples[i].empty() && histograms[i]) {
                histograms[i]->RecordMany(totalSamples[i]);
                ++recordManyCalls;
            }
        }

        const auto elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - t0).count();

        EmitSelfMetrics(trigger, totalEvents, addCalls, recordManyCalls, elapsed,
                         pendingCounters, pendingHistogramSamples);
    }

    // -- Self-observability of the buffer -----------------------------------

    void InitSelfMetrics() {
        auto& reg = Settings_.SelfMetricsRegistry;
        if (!reg) {
            return;
        }
        FlushDurationHist_ = reg->Histogram(
            kFlushDurationMetric, FlushDurationBuckets(),
            {},
            "Wall-clock time spent in a single TMetricBuffer drain pass.",
            "s");
        EventsBufferedCounter_ = reg->Counter(
            kEventsBufferedTotal, {},
            "Total number of logical metric updates that passed through the buffer.",
            "1");
        PendingCounterGauge_ = reg->Gauge(
            kPendingUpdatesMetric, {{"instrument", "counter"}},
            "Pending counter increments aggregated across all threads at flush start.",
            "1");
        PendingHistogramGauge_ = reg->Gauge(
            kPendingUpdatesMetric, {{"instrument", "histogram"}},
            "Pending histogram samples aggregated across all threads at flush start.",
            "1");
    }

    std::shared_ptr<ICounter> FlushesTotal(EFlushTrigger trigger) {
        auto& reg = Settings_.SelfMetricsRegistry;
        if (!reg) {
            return nullptr;
        }
        const char* trig = "interval";
        switch (trigger) {
            case EFlushTrigger::Interval:  trig = "interval";  break;
            case EFlushTrigger::Threshold: trig = "threshold"; break;
            case EFlushTrigger::Manual:    trig = "manual";    break;
            case EFlushTrigger::Shutdown:  trig = "shutdown";  break;
        }
        TLabels labels = {{"trigger", trig}};
        return reg->Counter(kFlushesTotalMetric, labels,
            "Total number of TMetricBuffer flush passes, by trigger.", "1");
    }

    std::shared_ptr<ICounter> UnderlyingCalls(const char* kind) {
        auto& reg = Settings_.SelfMetricsRegistry;
        if (!reg) {
            return nullptr;
        }
        TLabels labels = {{"kind", kind}};
        return reg->Counter(kUnderlyingCallsTotal, labels,
            "Total number of batched calls TMetricBuffer made into the underlying IMetricRegistry.",
            "1");
    }

    void EmitSelfMetrics(EFlushTrigger trigger,
                          std::uint64_t totalEvents,
                          std::uint64_t addCalls,
                          std::uint64_t recordManyCalls,
                          double durationSeconds,
                          std::uint64_t pendingCounters,
                          std::uint64_t pendingHistogramSamples) {
        if (FlushDurationHist_) {
            FlushDurationHist_->Record(durationSeconds);
        }
        if (auto c = FlushesTotal(trigger)) {
            c->Inc();
        }
        if (EventsBufferedCounter_ && totalEvents != 0) {
            EventsBufferedCounter_->Add(totalEvents);
        }
        if (addCalls != 0) {
            if (auto c = UnderlyingCalls("add")) {
                c->Add(addCalls);
            }
        }
        if (recordManyCalls != 0) {
            if (auto c = UnderlyingCalls("record_many")) {
                c->Add(recordManyCalls);
            }
        }
        if (PendingCounterGauge_) {
            PendingCounterGauge_->Set(static_cast<double>(pendingCounters));
        }
        if (PendingHistogramGauge_) {
            PendingHistogramGauge_->Set(static_cast<double>(pendingHistogramSamples));
        }
    }

    std::shared_ptr<IMetricRegistry> Underlying_;
    TMetricBufferSettings Settings_;

    mutable std::mutex HandlesMutex_;
    std::vector<TCounterHandleInfo> Counters_;
    std::vector<THistogramHandleInfo> Histograms_;

    mutable std::mutex ThreadsMutex_;
    std::vector<std::shared_ptr<TThreadState>> ThreadStates_;

    std::mutex WaitMutex_;
    std::condition_variable Wakeup_;
    std::atomic<bool> Stopping_{false};
    std::atomic<bool> ManualTrigger_{false};
    std::atomic<int> LastManualTrigger_{static_cast<int>(EFlushTrigger::Manual)};

    std::thread FlushThread_;

    std::shared_ptr<IHistogram> FlushDurationHist_;
    std::shared_ptr<ICounter> EventsBufferedCounter_;
    std::shared_ptr<IGauge> PendingCounterGauge_;
    std::shared_ptr<IGauge> PendingHistogramGauge_;

public:
    // Exposed for the registry decorator below.
    const std::shared_ptr<IMetricRegistry>& Underlying() const { return Underlying_; }
};

// ---------------------------------------------------------------------------
// Buffered metric handles. Each one holds a shared_ptr to TMetricBufferCore
// and the stable handle id, which lets the hot-path methods (Inc, Add,
// Record, RecordMany) route updates into the per-thread state.
// ---------------------------------------------------------------------------

class TBufferedCounter : public ICounter {
public:
    TBufferedCounter(std::shared_ptr<TMetricBufferCore> core, std::size_t id)
        : Core_(std::move(core)), Id_(id) {}

    void Inc() override {
        Core_->OnCounterAdd(Id_, 1);
    }
    void Add(std::uint64_t delta) override {
        Core_->OnCounterAdd(Id_, delta);
    }

private:
    std::shared_ptr<TMetricBufferCore> Core_;
    std::size_t Id_;
};

class TBufferedHistogram : public IHistogram {
public:
    TBufferedHistogram(std::shared_ptr<TMetricBufferCore> core, std::size_t id)
        : Core_(std::move(core)), Id_(id) {}

    void Record(double value) override {
        Core_->OnHistogramRecord(Id_, value);
    }
    void RecordMany(const std::vector<double>& values) override {
        Core_->OnHistogramRecordMany(Id_, values);
    }

private:
    std::shared_ptr<TMetricBufferCore> Core_;
    std::size_t Id_;
};

// Gauges are *not* buffered (see the header comment). We just keep the
// underlying handle and forward calls; the registry wrapper still routes
// them through this object so that the user sees a consistent
// shared_ptr<IGauge> identity per (name, labels).
class TPassthroughGauge : public IGauge {
public:
    explicit TPassthroughGauge(std::shared_ptr<IGauge> underlying)
        : Underlying_(std::move(underlying)) {}

    void Add(double delta) override { if (Underlying_) Underlying_->Add(delta); }
    void Set(double value) override { if (Underlying_) Underlying_->Set(value); }

private:
    std::shared_ptr<IGauge> Underlying_;
};

// ---------------------------------------------------------------------------
// Registry decorator. The first call for a given (name, labels) resolves the
// underlying handle, wraps it in a buffered handle, and caches the wrapper so
// subsequent calls return the same shared_ptr — preserving the contract of
// IMetricRegistry where repeat calls return the same metric instance.
// ---------------------------------------------------------------------------

class TBufferedMetricRegistry : public IMetricRegistry {
public:
    TBufferedMetricRegistry(std::shared_ptr<TMetricBufferCore> core)
        : Core_(std::move(core)) {}

    // The registry decorator owns the buffering lifecycle. When the user
    // releases the shared_ptr<IMetricRegistry> returned by
    // CreateBufferedMetricRegistry() the buffer is shut down here: the
    // background worker is joined and a synchronous final drain is
    // performed. Any handle wrappers still held by the caller (e.g. cached
    // inside TStatCollector) become transparent pass-throughs to the
    // underlying registry — see TMetricBufferCore::OnCounterAdd().
    ~TBufferedMetricRegistry() override {
        if (Core_) {
            Core_->Shutdown();
        }
    }

    std::shared_ptr<ICounter> Counter(const std::string& name,
                                       const TLabels& labels,
                                       const std::string& description,
                                       const std::string& unit) override {
        const auto key = MakeKey(name, labels);
        std::lock_guard<std::mutex> lock(WrappersLock_);
        auto& slot = CounterWrappers_[key];
        if (!slot) {
            auto underlying = Core_->Underlying()->Counter(name, labels, description, unit);
            const auto id = Core_->RegisterCounter(underlying);
            slot = std::make_shared<TBufferedCounter>(Core_, id);
        }
        return slot;
    }

    std::shared_ptr<IGauge> Gauge(const std::string& name,
                                   const TLabels& labels,
                                   const std::string& description,
                                   const std::string& unit) override {
        const auto key = MakeKey(name, labels);
        std::lock_guard<std::mutex> lock(WrappersLock_);
        auto& slot = GaugeWrappers_[key];
        if (!slot) {
            auto underlying = Core_->Underlying()->Gauge(name, labels, description, unit);
            slot = std::make_shared<TPassthroughGauge>(std::move(underlying));
        }
        return slot;
    }

    std::shared_ptr<IHistogram> Histogram(const std::string& name,
                                           const std::vector<double>& buckets,
                                           const TLabels& labels,
                                           const std::string& description,
                                           const std::string& unit) override {
        const auto key = MakeKey(name, labels);
        std::lock_guard<std::mutex> lock(WrappersLock_);
        auto& slot = HistogramWrappers_[key];
        if (!slot) {
            auto underlying = Core_->Underlying()->Histogram(
                name, buckets, labels, description, unit);
            const auto id = Core_->RegisterHistogram(underlying);
            slot = std::make_shared<TBufferedHistogram>(Core_, id);
        }
        return slot;
    }

private:
    static std::string MakeKey(const std::string& name, const TLabels& labels) {
        std::string key;
        key.reserve(name.size() + labels.size() * 24);
        key.append(name);
        key.push_back('\x1f');
        for (const auto& [k, v] : labels) {
            key.append(k);
            key.push_back('\x1e');
            key.append(v);
            key.push_back('\x1f');
        }
        return key;
    }

    std::shared_ptr<TMetricBufferCore> Core_;
    std::mutex WrappersLock_;
    std::unordered_map<std::string, std::shared_ptr<ICounter>>   CounterWrappers_;
    std::unordered_map<std::string, std::shared_ptr<IGauge>>     GaugeWrappers_;
    std::unordered_map<std::string, std::shared_ptr<IHistogram>> HistogramWrappers_;
};

} // anonymous namespace

std::shared_ptr<IMetricRegistry> CreateBufferedMetricRegistry(
    std::shared_ptr<IMetricRegistry> underlying,
    TMetricBufferSettings settings)
{
    if (!underlying) {
        return nullptr;
    }
    auto core = std::make_shared<TMetricBufferCore>(std::move(underlying),
                                                    std::move(settings));
    core->Start();
    return std::make_shared<TBufferedMetricRegistry>(std::move(core));
}

} // namespace NYdb::NObservability
