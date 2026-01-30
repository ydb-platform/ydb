#pragma once

#include "public.h"

#include "throughput_throttler.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TFairThrottlerConfig
    : public NYTree::TYsonStruct
{
    i64 TotalLimit;

    TDuration DistributionPeriod;

    int BucketAccumulationTicks;
    int GlobalAccumulationTicks;

    std::optional<std::string> IpcPath;

    REGISTER_YSON_STRUCT(TFairThrottlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFairThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TFairThrottlerBucketConfig
    : public NYTree::TYsonStruct
{
    double Weight;

    std::optional<i64> Limit;
    std::optional<double> RelativeLimit;
    std::optional<i64> GetLimit(i64 totalLimit);

    std::optional<i64> Guarantee;
    std::optional<double> RelativeGuarantee;
    std::optional<i64> GetGuarantee(i64 totalLimit);

    REGISTER_YSON_STRUCT(TFairThrottlerBucketConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFairThrottlerBucketConfig)

////////////////////////////////////////////////////////////////////////////////

//! Fair Throttler manages a group of throttlers, distributing traffic according to fair share policy.
/*!
 *  Fair Throttler distributes TotalLimit * DistributionPeriod bytes every DistributionPeriod.
 *
 *  At the beginning of the period, Fair Throttler distributes new quota between buckets. Buckets
 *  accumulate quota for N ticks. After N ticks overflown quota is transferred into shared bucket.
 *  Shared bucket accumulates quota for M ticks. Overflown quota from shared bucket is discarded.
 *
 *  Throttled requests may consume quota from both local bucket and shared bucket.
 */
struct IFairThrottler
    : public TRefCounted
{
    virtual IThroughputThrottlerPtr CreateBucketThrottler(
        const std::string& name,
        TFairThrottlerBucketConfigPtr config) = 0;

    virtual void Reconfigure(
        TFairThrottlerConfigPtr config,
        const THashMap<std::string, TFairThrottlerBucketConfigPtr>& bucketConfigs) = 0;

    static std::vector<i64> ComputeFairDistribution(
        i64 totalLimit,
        const std::vector<double>& weights,
        const std::vector<i64>& demands,
        const std::vector<std::optional<i64>>& limits);
};

DEFINE_REFCOUNTED_TYPE(IFairThrottler)

////////////////////////////////////////////////////////////////////////////////

IFairThrottlerPtr CreateFairThrottler(
    TFairThrottlerConfigPtr config,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
