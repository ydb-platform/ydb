#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>

#include <chrono>
#include <unordered_map>
#include <variant>

namespace Ydb::RateLimiter {
    class CreateResourceRequest;
    class DescribeResourceResult;
    class HierarchicalDrrSettings;
    class ReplicatedBucketSettings;
    class MeteringConfig;
    class MeteringConfig_Metric;
} // namespace Ydb::RateLimiter

namespace NYdb::inline Dev::NRateLimiter {

struct TReplicatedBucketSettings {
    using TSelf = TReplicatedBucketSettings;

    TReplicatedBucketSettings() = default;
    TReplicatedBucketSettings(const Ydb::RateLimiter::ReplicatedBucketSettings&);

    void SerializeTo(Ydb::RateLimiter::ReplicatedBucketSettings&) const;

    // Interval between syncs from kesus and between consumption reports.
    // Default value equals 5000 ms and not inherited.
    FLUENT_SETTING_OPTIONAL(std::chrono::milliseconds, ReportInterval);
};

class TLeafBehavior {
public:
    enum EBehavior {
        REPLICATED_BUCKET,
    };

    EBehavior GetBehavior() const;

    TLeafBehavior(const TReplicatedBucketSettings&);
    TLeafBehavior(const Ydb::RateLimiter::ReplicatedBucketSettings&);
    const TReplicatedBucketSettings& GetReplicatedBucket() const;

    void SerializeTo(Ydb::RateLimiter::HierarchicalDrrSettings&) const;

private:
    std::variant<TReplicatedBucketSettings> BehaviorSettings_;
};

// Settings for hierarchical deficit round robin (HDRR) algorithm.
template <class TDerived>
struct THierarchicalDrrSettings {
    using TSelf = TDerived;

    THierarchicalDrrSettings() = default;
    THierarchicalDrrSettings(const Ydb::RateLimiter::HierarchicalDrrSettings&);

    void SerializeTo(Ydb::RateLimiter::HierarchicalDrrSettings&) const;

    // Resource consumption speed limit.
    // Value is required for root resource.
    // Must be nonnegative.
    FLUENT_SETTING_OPTIONAL(double, MaxUnitsPerSecond);

    // Maximum burst size of resource consumption across the whole cluster
    // divided by max_units_per_second.
    // Default value is 1.
    // This means that maximum burst size might be equal to max_units_per_second.
    // Must be nonnegative.
    FLUENT_SETTING_OPTIONAL(double, MaxBurstSizeCoefficient);

    // Prefetch in local bucket up to PrefetchCoefficient*MaxUnitsPerSecond units (full size).
    // Default value is inherited from parent or 0.2 for root.
    // Disables prefetching if any negative value is set
    // (It is useful to avoid bursts in case of large number of local buckets).
    FLUENT_SETTING_OPTIONAL(double, PrefetchCoefficient);

    void DisablePrefetching() {
        PrefetchCoefficient_ = -1.0;
    }

    // Prefetching starts if there is less than PrefetchWatermark fraction of full local bucket left.
    // Default value is inherited from parent or 0.75 for root.
    // Must be nonnegative and less than or equal to 1.
    FLUENT_SETTING_OPTIONAL(double, PrefetchWatermark);

    // Prevents bucket from going too deep in negative values. If somebody reports value that will exceed
    // this limit the final amount in bucket will be equal to this limit.
    // Should be negative value.
    // Unset means no limit.
    FLUENT_SETTING_OPTIONAL(double, ImmediatelyFillUpTo);

    // Behavior of leafs in tree.
    // Not inherited.
    FLUENT_SETTING_OPTIONAL(TLeafBehavior, LeafBehavior);
};

struct TMetric {
    using TSelf = TMetric;
    using TLabels = std::unordered_map<std::string, std::string>;

    TMetric() = default;
    TMetric(const Ydb::RateLimiter::MeteringConfig_Metric&);

    void SerializeTo(Ydb::RateLimiter::MeteringConfig_Metric&) const;

    // Send this metric to billing.
    // Default value is false (not inherited).
    FLUENT_SETTING_DEFAULT(bool, Enabled, false);

    // Billing metric period (aligned to hour boundary).
    // Default value is inherited from parent or equals 60 seconds for root.
    FLUENT_SETTING_OPTIONAL(std::chrono::seconds, BillingPeriod);

    // User-defined labels.
    FLUENT_SETTING(TLabels, Labels);

    // Billing metric JSON fields (inherited from parent if not set)
    FLUENT_SETTING(std::string, MetricFieldsJson);
};

struct TMeteringConfig {
    using TSelf = TMeteringConfig;

    TMeteringConfig() = default;
    TMeteringConfig(const Ydb::RateLimiter::MeteringConfig&);

    void SerializeTo(Ydb::RateLimiter::MeteringConfig&) const;

    // Meter consumed resources and send billing metrics.
    FLUENT_SETTING_DEFAULT(bool, Enabled, false);

    // Period to report consumption history from clients to kesus
    // Default value is inherited from parent or equals 5000 ms for root.
    FLUENT_SETTING_OPTIONAL(std::chrono::milliseconds, ReportPeriod);

    // Consumption history period that is sent in one message to metering actor.
    // Default value is inherited from parent or equals 1000 ms for root.
    FLUENT_SETTING_OPTIONAL(std::chrono::milliseconds, MeterPeriod);

    // Time window to collect data from every client.
    // Any client metering message that is `collect_period` late is discarded (not metered or billed).
    // Default value is inherited from parent or equals 30 seconds for root.
    FLUENT_SETTING_OPTIONAL(std::chrono::seconds, CollectPeriod);

    // Provisioned consumption limit in units per second.
    // Effective value is limited by corresponding `max_units_per_second`.
    // Default value is 0 (not inherited).
    FLUENT_SETTING_OPTIONAL(double, ProvisionedUnitsPerSecond);

    // Provisioned allowed burst equals `provisioned_coefficient * provisioned_units_per_second` units.
    // Effective value is limited by corresponding PrefetchCoefficient.
    // Default value is inherited from parent or equals 60 for root.
    FLUENT_SETTING_OPTIONAL(double, ProvisionedCoefficient);

    // On-demand allowed burst equals `overshoot_coefficient * prefetch_coefficient * max_units_per_second` units.
    // Should be greater or equal to 1.0
    // Default value is inherited from parent or equals 1.1 for root
    FLUENT_SETTING_OPTIONAL(double, OvershootCoefficient);

    // Consumption within provisioned limit.
    // Informative metric that should be sent to billing (not billed).
    FLUENT_SETTING_OPTIONAL(TMetric, Provisioned);

    // Consumption that exceeds provisioned limit is billed as on-demand.
    FLUENT_SETTING_OPTIONAL(TMetric, OnDemand);

    // Consumption that exceeds even on-demand limit.
    // Normally it is free and should not be billed.
    FLUENT_SETTING_OPTIONAL(TMetric, Overshoot);
};

// Settings for create resource request.
struct TCreateResourceSettings
    : public TOperationRequestSettings<TCreateResourceSettings>
    , public THierarchicalDrrSettings<TCreateResourceSettings>
{
    TCreateResourceSettings() = default;
    TCreateResourceSettings(const Ydb::RateLimiter::CreateResourceRequest&);

    FLUENT_SETTING_OPTIONAL(TMeteringConfig, MeteringConfig);
};

// Settings for alter resource request.
struct TAlterResourceSettings
    : public TOperationRequestSettings<TAlterResourceSettings>
    , public THierarchicalDrrSettings<TAlterResourceSettings>
{
    FLUENT_SETTING_OPTIONAL(TMeteringConfig, MeteringConfig);
};

// Settings for drop resource request.
struct TDropResourceSettings : public TOperationRequestSettings<TDropResourceSettings> {};

// Settings for list resources request.
struct TListResourcesSettings : public TOperationRequestSettings<TListResourcesSettings> {
    using TSelf = TListResourcesSettings;

    // List resources recursively, including children.
    FLUENT_SETTING_FLAG(Recursive);
};

// Settings for describe resource request.
struct TDescribeResourceSettings : public TOperationRequestSettings<TDescribeResourceSettings> {};

// Result for list resources request.
struct TListResourcesResult : public TStatus {
    TListResourcesResult(TStatus status, std::vector<std::string> paths);

    // Paths of listed resources inside a specified coordination node.
    const std::vector<std::string>& GetResourcePaths() const {
        return ResourcePaths_;
    }

private:
    std::vector<std::string> ResourcePaths_;
};

// Settings for acquire resource request.
struct TAcquireResourceSettings : public TOperationRequestSettings<TAcquireResourceSettings> {
    using TSelf = TAcquireResourceSettings;

    FLUENT_SETTING_OPTIONAL(uint64_t, Amount);
    FLUENT_SETTING_FLAG(IsUsedAmount);
};

using TAsyncListResourcesResult = NThreading::TFuture<TListResourcesResult>;

// Result for describe resource request.
struct TDescribeResourceResult : public TStatus {
    // Note for YDB developers: THierarchicalDrrProps wrapper class exists for compatibility with older client code.
    // Newer code should use the THierarchicalDrrSettings class directly.
    struct THierarchicalDrrProps : public THierarchicalDrrSettings<THierarchicalDrrProps> {
        THierarchicalDrrProps(const Ydb::RateLimiter::HierarchicalDrrSettings&);

        // Resource consumption speed limit.
        std::optional<double> GetMaxUnitsPerSecond() const {
            return MaxUnitsPerSecond_;
        }

        // Maximum burst size of resource consumption across the whole cluster
        // divided by max_units_per_second.
        std::optional<double> GetMaxBurstSizeCoefficient() const {
            return MaxBurstSizeCoefficient_;
        }

        // Prefetch in local bucket up to PrefetchCoefficient*MaxUnitsPerSecond units (full size).
        std::optional<double> GetPrefetchCoefficient() const {
            return PrefetchCoefficient_;
        }

        // Prefetching starts if there is less than PrefetchWatermark fraction of full local bucket left.
        std::optional<double> GetPrefetchWatermark() const {
            return PrefetchWatermark_;
        }

        std::optional<double> GetImmediatelyFillUpTo() const {
            return ImmediatelyFillUpTo_;
        }

        const std::optional<TLeafBehavior>& GetLeafBehavior() const {
            return LeafBehavior_;
        }
    };

    TDescribeResourceResult(TStatus status, const Ydb::RateLimiter::DescribeResourceResult& result);

    // Path of resource inside a coordination node.
    const std::string& GetResourcePath() const {
        return ResourcePath_;
    }

    const THierarchicalDrrProps& GetHierarchicalDrrProps() const {
        return HierarchicalDrrProps_;
    }

    const std::optional<TMeteringConfig>& GetMeteringConfig() const {
        return MeteringConfig_;
    }

private:
    std::string ResourcePath_;
    THierarchicalDrrProps HierarchicalDrrProps_;
    std::optional<TMeteringConfig> MeteringConfig_;
};

using TAsyncDescribeResourceResult = NThreading::TFuture<TDescribeResourceResult>;

// Rate limiter client.
class TRateLimiterClient {
public:
    TRateLimiterClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    // Create a new resource in existing coordination node.
    TAsyncStatus CreateResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TCreateResourceSettings& = {});

    // Update a resource in coordination node.
    TAsyncStatus AlterResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TAlterResourceSettings& = {});

    // Delete a resource from coordination node.
    TAsyncStatus DropResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TDropResourceSettings& = {});

    // List resources in given coordination node.
    TAsyncListResourcesResult ListResources(const std::string& coordinationNodePath, const std::string& resourcePath, const TListResourcesSettings& = {});

    // Describe properties of resource in coordination node.
    TAsyncDescribeResourceResult DescribeResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TDescribeResourceSettings& = {});

    // Acquire resources's units inside a coordination node.
    // If CancelAfter is set greater than zero and less than OperationTimeout
    // and resource is not ready after CancelAfter time,
    // the result code of this operation will be CANCELLED and resource will not be spent.
    // It is recommended to specify both OperationTimeout and CancelAfter.
    // CancelAfter should be less than OperationTimeout.
    TAsyncStatus AcquireResource(const std::string& coordinationNodePath, const std::string& resourcePath, const TAcquireResourceSettings& = {});

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NRateLimiter
