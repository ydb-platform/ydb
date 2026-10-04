#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metrics.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace NYdb::inline Dev::NTopic::NTests {

enum class ECounterFailure { None, BeforeAdd, AfterAdd };

class TRecordingCounter final: public NMetrics::ICounter {
public:
    explicit TRecordingCounter(ECounterFailure failure = ECounterFailure::None)
        : Failure(failure)
    {
    }

    void Inc() override {
        Add(1);
    }

    void Add(std::uint64_t delta) override {
        AddCalls_.fetch_add(1, std::memory_order_relaxed);
        if (Failure == ECounterFailure::BeforeAdd) {
            throw std::runtime_error("counter update failed");
        }
        {
            std::lock_guard guard(WaitLock_);
            Value_.fetch_add(delta, std::memory_order_relaxed);
        }
        Condition_.notify_all();
        if (Failure == ECounterFailure::AfterAdd) {
            throw std::runtime_error("counter update mutated then failed");
        }
        if (OnAdd) {
            OnAdd();
        }
    }

    bool WaitForValue(std::uint64_t value) {
        std::unique_lock lock(WaitLock_);
        return Condition_.wait_for(lock, std::chrono::seconds(5), [&] { return Value() >= value; });
    }

    std::uint64_t Value() const {
        return Value_.load(std::memory_order_relaxed);
    }

    std::uint64_t AddCalls() const {
        return AddCalls_.load(std::memory_order_relaxed);
    }

    // Configure before releasing the response/task that invokes this counter.
    ECounterFailure Failure;
    std::function<void()> OnAdd;

private:
    std::atomic<std::uint64_t> Value_ = 0;
    std::atomic<std::uint64_t> AddCalls_ = 0;
    std::mutex WaitLock_;
    std::condition_variable Condition_;
};

class TRecordingMetricRegistry final: public NMetrics::IMetricRegistry {
public:
    enum class EFailure { None, NullCounter, Registration, Add };

    struct TReaderRegistrationCounts {
        size_t Counters = 0;
    };

    explicit TRecordingMetricRegistry(EFailure failure = EFailure::None, std::string affectedName = {})
        : Failure_(failure)
        , AffectedName_(std::move(affectedName))
    {
    }

    std::shared_ptr<NMetrics::ICounter> Counter(
        const std::string& name, const NMetrics::TLabels& labels,
        const std::string& description, const std::string& unit) override {
        std::lock_guard guard(Lock_);
        ++RegistrationAttempts_;
        const auto failure = AffectedName_.empty() || name == AffectedName_ ? Failure_ : EFailure::None;
        if (failure == EFailure::NullCounter) {
            return {};
        }
        if (failure == EFailure::Registration) {
            throw std::runtime_error("counter registration failed");
        }
        for (const auto& entry : Counters_) {
            if (entry.Name == name && entry.Labels == labels) {
                return entry.Counter;
            }
        }
        auto counter = std::make_shared<TRecordingCounter>(
            failure == EFailure::Add ? ECounterFailure::BeforeAdd : ECounterFailure::None);
        Counters_.push_back({name, labels, description, unit, counter});
        return counter;
    }

    std::shared_ptr<NMetrics::IGauge> Gauge(
        const std::string&, const NMetrics::TLabels&, const std::string&, const std::string&) override {
        return {};
    }

    std::shared_ptr<NMetrics::IHistogram> Histogram(
        const std::string&, const std::vector<double>&, const NMetrics::TLabels&,
        const std::string&, const std::string&) override {
        // Table session/transaction RPCs also use this driver registry.
        struct TNoopHistogram final: NMetrics::IHistogram {
            void Record(double) override {
            }
        };
        return std::make_shared<TNoopHistogram>();
    }

    std::shared_ptr<TRecordingCounter> Find(const std::string& name, const NMetrics::TLabels& labels) const {
        std::lock_guard guard(Lock_);
        const auto* entry = FindEntry(name, labels);
        return entry ? entry->Counter : nullptr;
    }

    std::string Unit(const std::string& name, const NMetrics::TLabels& labels) const {
        std::lock_guard guard(Lock_);
        const auto* entry = FindEntry(name, labels);
        return entry ? entry->Unit : std::string{};
    }

    std::string Description(const std::string& name, const NMetrics::TLabels& labels) const {
        std::lock_guard guard(Lock_);
        const auto* entry = FindEntry(name, labels);
        return entry ? entry->Description : std::string{};
    }

    std::uint64_t RegistrationAttempts() const {
        std::lock_guard guard(Lock_);
        return RegistrationAttempts_;
    }

    TReaderRegistrationCounts ReaderMetricRegistrations() const {
        std::lock_guard guard(Lock_);
        return {static_cast<size_t>(std::count_if(Counters_.begin(), Counters_.end(), [](const auto& entry) {
            return entry.Name.rfind("ydb.topic.reader.", 0) == 0;
        }))};
    }

private:
    struct TEntry {
        std::string Name;
        NMetrics::TLabels Labels;
        std::string Description;
        std::string Unit;
        std::shared_ptr<TRecordingCounter> Counter;
    };

    const TEntry* FindEntry(const std::string& name, const NMetrics::TLabels& labels) const {
        for (const auto& entry : Counters_) {
            if (entry.Name == name && entry.Labels == labels) {
                return &entry;
            }
        }
        return nullptr;
    }

    mutable std::mutex Lock_;
    std::vector<TEntry> Counters_;
    std::uint64_t RegistrationAttempts_ = 0;
    const EFailure Failure_;
    const std::string AffectedName_;
};

class TManualExecutor final: public IExecutor {
public:
    void Discard() {
        std::deque<TFunction> tasks;
        {
            std::lock_guard guard(Lock_);
            tasks.swap(Tasks_);
        }
    }

    void Stop() override {
        std::lock_guard guard(Lock_);
        Stopped_ = true;
        Condition_.notify_all();
    }

    void Post(TFunction&& function) override {
        {
            std::lock_guard guard(Lock_);
            if (Stopped_) {
                return;
            }
            Tasks_.push_back(std::move(function));
        }
        Condition_.notify_one();
    }

    bool IsAsync() const override {
        return true;
    }

    bool WaitForTask() {
        std::unique_lock lock(Lock_);
        return Condition_.wait_for(lock, std::chrono::seconds(5), [this] {
            return !Tasks_.empty() || Stopped_;
        }) && !Tasks_.empty();
    }

    void RunOne() {
        TFunction function;
        {
            std::lock_guard guard(Lock_);
            UNIT_ASSERT(!Tasks_.empty());
            function = std::move(Tasks_.front());
            Tasks_.pop_front();
        }
        function();
    }

protected:
    void DoStart() override {
    }

private:
    std::mutex Lock_;
    std::condition_variable Condition_;
    std::deque<TFunction> Tasks_;
    bool Stopped_ = false;
};

class TInlineExecutor final: public IExecutor {
public:
    void Stop() override {
    }

    void Post(TFunction&& function) override {
        function();
    }

    bool IsAsync() const override {
        return false;
    }

protected:
    void DoStart() override {
    }
};

} // namespace NYdb::NTopic::NTests
