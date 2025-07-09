#pragma once

#include "public.h"

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TPeriodicExecutorOptions
{
    static constexpr double DefaultJitter = 0.2;

    //! Interval between usual consequent invocations.
    //! If nullopt then no invocations will be happening.
    std::optional<TDuration> Period;
    TDuration Splay;
    double Jitter = 0.0;

    bool operator==(const TPeriodicExecutorOptions& other) const = default;

    //! Sets #Period and applies #DefaultJitter.
    static TPeriodicExecutorOptions WithJitter(TDuration period);

    //! Generates the delay for the next invocation from #Period and #Jitter.
    TDuration GenerateDelay() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TRetryingPeriodicExecutorOptions
    : public TPeriodicExecutorOptions
    , public TExponentialBackoffOptions
{ };

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TPeriodicExecutorOptionsSerializer
    : public virtual NYTree::TExternalizedYsonStruct
{
    REGISTER_EXTERNALIZED_YSON_STRUCT(TPeriodicExecutorOptions, TPeriodicExecutorOptionsSerializer);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TRetryingPeriodicExecutorOptionsSerializer
    : public TPeriodicExecutorOptionsSerializer
    , public ::NYT::NDetail::TExponentialBackoffOptionsSerializer
{
    REGISTER_DERIVED_EXTERNALIZED_YSON_STRUCT(
        TRetryingPeriodicExecutorOptions,
        TRetryingPeriodicExecutorOptionsSerializer,
        (TPeriodicExecutorOptionsSerializer)
        (::NYT::NDetail::TExponentialBackoffOptionsSerializer));

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

ASSIGN_EXTERNAL_YSON_SERIALIZER(TPeriodicExecutorOptions, NDetail::TPeriodicExecutorOptionsSerializer);
ASSIGN_EXTERNAL_YSON_SERIALIZER(TRetryingPeriodicExecutorOptions, NDetail::TRetryingPeriodicExecutorOptionsSerializer);

////////////////////////////////////////////////////////////////////////////////

struct TThroughputThrottlerConfig
    : public NYTree::TYsonStruct
{
    //! Limit on average throughput (per sec). Null means unlimited.
    std::optional<double> Limit;

    //! Period for leaky bucket algorithm.
    TDuration Period;

    std::optional<i64> GetMaxAvailable() const;

    static TThroughputThrottlerConfigPtr Create(std::optional<double> limit);

    bool operator==(const TThroughputThrottlerConfig& other);

    REGISTER_YSON_STRUCT(TThroughputThrottlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TThroughputThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

//! This wrapper may be helpful if we have some parametrization over the limit
//! (e.g. in network bandwidth limit on nodes).
//! The exact logic of limit/relative_limit clash resolution
//! and the parameter access are external to the config itself.
struct TRelativeThroughputThrottlerConfig
    : public TThroughputThrottlerConfig
{
    std::optional<double> RelativeLimit;

    static TRelativeThroughputThrottlerConfigPtr Create(std::optional<double> limit);

    REGISTER_YSON_STRUCT(TRelativeThroughputThrottlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRelativeThroughputThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TPrefetchingThrottlerConfig
    : public NYTree::TYsonStruct
{
    //! RPS limit for requests to the underlying throttler.
    double TargetRps;

    //! Minimum amount to be prefetched from the underlying throttler.
    i64 MinPrefetchAmount;

    //! Maximum amount to be prefetched from the underlying throttler.
    //! Guards from a uncontrolled growth of the requested amount.
    i64 MaxPrefetchAmount;

    //! Time window for the RPS estimation.
    TDuration Window;

    //! Enable the prefetching throttler.
    //! If disabled #CreatePrefetchingThrottler() will not create #TPrefetchingThrottler
    //! and will return the underlying throttler instead.
    //! #TPrefetchingThrottler itself does not check this field.
    bool Enable;

    REGISTER_YSON_STRUCT(TPrefetchingThrottlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPrefetchingThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TFiberManagerConfig
    : public NYTree::TYsonStruct
{
    THashMap<EExecutionStackKind, int> FiberStackPoolSizes;
    int MaxIdleFibers;

    TFiberManagerConfigPtr ApplyDynamic(const TFiberManagerDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TFiberManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFiberManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TFiberManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    THashMap<EExecutionStackKind, int> FiberStackPoolSizes;
    std::optional<int> MaxIdleFibers;

    REGISTER_YSON_STRUCT(TFiberManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFiberManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
