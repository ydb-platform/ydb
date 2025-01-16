#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/generic/maybe.h>

namespace Ydb::RateLimiter {
class DescribeResourceResult;
class HierarchicalDrrSettings;
} // namespace Ydb::RateLimiter

namespace NYdb::NRateLimiter {

// Settings for hierarchical deficit round robin (HDRR) algorithm.
template <class TDerived>
struct THierarchicalDrrSettings : public TOperationRequestSettings<TDerived> {
    using TSelf = TDerived;

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
};

// Settings for create resource request.
struct TCreateResourceSettings : public THierarchicalDrrSettings<TCreateResourceSettings> {
};

// Settings for alter resource request.
struct TAlterResourceSettings : public THierarchicalDrrSettings<TAlterResourceSettings> {
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
    TListResourcesResult(TStatus status, TVector<TString> paths);

    // Paths of listed resources inside a specified coordination node.
    const TVector<TString>& GetResourcePaths() const {
        return ResourcePaths_;
    }

private:
    TVector<TString> ResourcePaths_;
};

// Settings for acquire resource request.
struct TAcquireResourceSettings : public TOperationRequestSettings<TAcquireResourceSettings> {
    using TSelf = TAcquireResourceSettings;

    FLUENT_SETTING_OPTIONAL(ui64, Amount);
    FLUENT_SETTING_FLAG(IsUsedAmount);
};

using TAsyncListResourcesResult = NThreading::TFuture<TListResourcesResult>;

// Result for describe resource request.
struct TDescribeResourceResult : public TStatus {
    struct THierarchicalDrrProps {
        THierarchicalDrrProps(const Ydb::RateLimiter::HierarchicalDrrSettings&);

        // Resource consumption speed limit.
        TMaybe<double> GetMaxUnitsPerSecond() const {
            return MaxUnitsPerSecond_;
        }

        // Maximum burst size of resource consumption across the whole cluster
        // divided by max_units_per_second.
        TMaybe<double> GetMaxBurstSizeCoefficient() const {
            return MaxBurstSizeCoefficient_;
        }

        // Prefetch in local bucket up to PrefetchCoefficient*MaxUnitsPerSecond units (full size).
        TMaybe<double> GetPrefetchCoefficient() const {
            return PrefetchCoefficient_;
        }

        // Prefetching starts if there is less than PrefetchWatermark fraction of full local bucket left.
        TMaybe<double> GetPrefetchWatermark() const {
            return PrefetchWatermark_;
        }

    private:
        TMaybe<double> MaxUnitsPerSecond_;
        TMaybe<double> MaxBurstSizeCoefficient_;
        TMaybe<double> PrefetchCoefficient_;
        TMaybe<double> PrefetchWatermark_;
    };

    TDescribeResourceResult(TStatus status, const Ydb::RateLimiter::DescribeResourceResult& result);

    // Path of resource inside a coordination node.
    const TString& GetResourcePath() const {
        return ResourcePath_;
    }

    const THierarchicalDrrProps& GetHierarchicalDrrProps() const {
        return HierarchicalDrrProps_;
    }

private:
    TString ResourcePath_;
    THierarchicalDrrProps HierarchicalDrrProps_;
};

using TAsyncDescribeResourceResult = NThreading::TFuture<TDescribeResourceResult>;

// Rate limiter client.
class TRateLimiterClient {
public:
    TRateLimiterClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    // Create a new resource in existing coordination node.
    TAsyncStatus CreateResource(const TString& coordinationNodePath, const TString& resourcePath, const TCreateResourceSettings& = {});

    // Update a resource in coordination node.
    TAsyncStatus AlterResource(const TString& coordinationNodePath, const TString& resourcePath, const TAlterResourceSettings& = {});

    // Delete a resource from coordination node.
    TAsyncStatus DropResource(const TString& coordinationNodePath, const TString& resourcePath, const TDropResourceSettings& = {});

    // List resources in given coordination node.
    TAsyncListResourcesResult ListResources(const TString& coordinationNodePath, const TString& resourcePath, const TListResourcesSettings& = {});

    // Describe properties of resource in coordination node.
    TAsyncDescribeResourceResult DescribeResource(const TString& coordinationNodePath, const TString& resourcePath, const TDescribeResourceSettings& = {});

    // Acquire resources's units inside a coordination node.
    // If CancelAfter is set greater than zero and less than OperationTimeout
    // and resource is not ready after CancelAfter time,
    // the result code of this operation will be CANCELLED and resource will not be spent.
    // It is recommended to specify both OperationTimeout and CancelAfter.
    // CancelAfter should be less than OperationTimeout.
    TAsyncStatus AcquireResource(const TString& coordinationNodePath, const TString& resourcePath, const TAcquireResourceSettings& = {});

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NRateLimiter
