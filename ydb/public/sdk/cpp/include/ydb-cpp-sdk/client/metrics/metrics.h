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

    // Adds an aggregated increment in a single call. This is the batched
    // entry point used by NObservability::TMetricBuffer to coalesce many
    // hot-path Inc() updates into one call to the underlying registry.
    // The default fallback calls Inc() `delta` times so that backends
    // that do not override this method keep working unchanged.
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

    // Records a batch of observations in a single call. Used by
    // NObservability::TMetricBuffer to coalesce many hot-path Record()
    // calls into one. The default fallback simply iterates Record().
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
