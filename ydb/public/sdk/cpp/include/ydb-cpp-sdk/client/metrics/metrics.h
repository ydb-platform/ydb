#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace NYdb::inline Dev::NMetrics {

using TLabels = std::map<std::string, std::string>;

class ICounter {
public:
    virtual ~ICounter() = default;
    virtual void Inc() = 0;

    virtual void Add(std::uint64_t delta) {
        for (std::uint64_t i = 0; i < delta; ++i) {
            Inc();
        }
    }
};

class IGauge {
public:
    virtual ~IGauge() = default;
    virtual void Add(double delta) = 0;
    virtual void Set(double value) = 0;
};

class IHistogram {
public:
    virtual ~IHistogram() = default;
    virtual void Record(double value) = 0;

    virtual void RecordMany(const std::vector<double>& values) {
        for (double v : values) {
            Record(v);
        }
    }
};

class IMetricRegistry {
public:
    virtual ~IMetricRegistry() = default;

    virtual std::shared_ptr<ICounter> Counter(
        const std::string& name,
        const TLabels& labels = {},
        const std::string& description = {},
        const std::string& unit = {}
    ) = 0;
    virtual std::shared_ptr<IGauge> Gauge(
        const std::string& name,
        const TLabels& labels = {},
        const std::string& description = {},
        const std::string& unit = {}
    ) = 0;
    virtual std::shared_ptr<IHistogram> Histogram(
        const std::string& name,
        const std::vector<double>& buckets,
        const TLabels& labels = {},
        const std::string& description = {},
        const std::string& unit = {}
    ) = 0;
};

} // namespace NYdb::NMetrics
