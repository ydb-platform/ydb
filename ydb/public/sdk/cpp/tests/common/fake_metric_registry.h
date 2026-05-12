#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metrics.h>

#include <atomic>
#include <map>
#include <mutex>
#include <vector>

namespace NYdb::NTests {

class TFakeCounter : public NMetrics::ICounter {
public:
    void Inc() override {
        Count_.fetch_add(1, std::memory_order_relaxed);
    }

    int64_t Get() const {
        return Count_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<int64_t> Count_{0};
};

class TFakeHistogram : public NMetrics::IHistogram {
public:
    void Record(double value) override {
        std::lock_guard lock(Mutex_);
        Values_.push_back(value);
    }

    std::vector<double> GetValues() const {
        std::lock_guard lock(Mutex_);
        return Values_;
    }

    size_t Count() const {
        std::lock_guard lock(Mutex_);
        return Values_.size();
    }

private:
    mutable std::mutex Mutex_;
    std::vector<double> Values_;
};

class TFakeGauge : public NMetrics::IGauge {
public:
    void Add(double delta) override { Value_ += delta; }
    void Set(double value) override { Value_ = value; }
    double Get() const { return Value_; }

private:
    double Value_ = 0.0;
};

struct TMetricKey {
    std::string Name;
    NMetrics::TLabels Labels;

    bool operator==(const TMetricKey& other) const = default;
    bool operator<(const TMetricKey& other) const {
        if (Name != other.Name) return Name < other.Name;
        return Labels < other.Labels;
    }
};

class TFakeMetricRegistry : public NMetrics::IMetricRegistry {
public:
    std::shared_ptr<NMetrics::ICounter> Counter(const std::string& name
        , const NMetrics::TLabels& labels
        , const std::string& /*description*/
        , const std::string& /*unit*/
    ) override {
        std::lock_guard lock(Mutex_);
        auto key = TMetricKey{name, labels};
        auto it = Counters_.find(key);
        if (it != Counters_.end()) {
            return it->second;
        }
        auto counter = std::make_shared<TFakeCounter>();
        Counters_[key] = counter;
        return counter;
    }

    std::shared_ptr<NMetrics::IGauge> Gauge(const std::string& name
        , const NMetrics::TLabels& labels
        , const std::string& /*description*/
        , const std::string& /*unit*/
    ) override {
        std::lock_guard lock(Mutex_);
        auto key = TMetricKey{name, labels};
        auto gauge = std::make_shared<TFakeGauge>();
        Gauges_[key] = gauge;
        return gauge;
    }

    std::shared_ptr<NMetrics::IHistogram> Histogram(const std::string& name
        , const std::vector<double>& /*buckets*/
        , const NMetrics::TLabels& labels
        , const std::string& /*description*/
        , const std::string& /*unit*/
    ) override {
        std::lock_guard lock(Mutex_);
        auto key = TMetricKey{name, labels};
        auto it = Histograms_.find(key);
        if (it != Histograms_.end()) {
            return it->second;
        }
        auto histogram = std::make_shared<TFakeHistogram>();
        Histograms_[key] = histogram;
        return histogram;
    }

    std::shared_ptr<TFakeCounter> GetCounter(const std::string& name, const NMetrics::TLabels& labels = {}) const {
        std::lock_guard lock(Mutex_);
        auto it = Counters_.find(TMetricKey{name, labels});
        return it != Counters_.end() ? it->second : nullptr;
    }

    std::shared_ptr<TFakeHistogram> GetHistogram(const std::string& name, const NMetrics::TLabels& labels = {}) const {
        std::lock_guard lock(Mutex_);
        auto it = Histograms_.find(TMetricKey{name, labels});
        return it != Histograms_.end() ? it->second : nullptr;
    }

private:
    mutable std::mutex Mutex_;
    std::map<TMetricKey, std::shared_ptr<TFakeCounter>> Counters_;
    std::map<TMetricKey, std::shared_ptr<TFakeGauge>> Gauges_;
    std::map<TMetricKey, std::shared_ptr<TFakeHistogram>> Histograms_;
};

} // namespace NYdb::NTests
