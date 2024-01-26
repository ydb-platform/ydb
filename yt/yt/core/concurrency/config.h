#pragma once

#include "public.h"

// TODO(arkady-e1ppa): Move backoff config+serialization into core/misc/config.h.
#include <yt/yt/core/misc/backoff_strategy_config.h>

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

    //! Sets #Period and Applies set#DefaultJitter.
    static TPeriodicExecutorOptions WithJitter(TDuration period);
};

////////////////////////////////////////////////////////////////////////////////

struct TRetryingPeriodicExecutorOptions
{
    TPeriodicExecutorOptions Periodic;
    TExponentialBackoffOptions BackoffStrategy;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TPeriodicExecutorOptionsSerializer
    : public NYTree::TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TPeriodicExecutorOptions, TPeriodicExecutorOptionsSerializer);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TRetryingPeriodicExecutorOptionsSerializer
    : public NYTree::TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TRetryingPeriodicExecutorOptions, TRetryingPeriodicExecutorOptionsSerializer);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TThroughputThrottlerConfig
    : public NYTree::TYsonStruct
{
public:
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
class TRelativeThroughputThrottlerConfig
    : public TThroughputThrottlerConfig
{
public:
    std::optional<double> RelativeLimit;

    static TRelativeThroughputThrottlerConfigPtr Create(std::optional<double> limit);

    REGISTER_YSON_STRUCT(TRelativeThroughputThrottlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRelativeThroughputThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

class TPrefetchingThrottlerConfig
    : public NYTree::TYsonStruct
{
public:
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

} // namespace NYT::NConcurrency

ASSIGN_EXTERNAL_YSON_SERIALIZER(NYT::NConcurrency::TPeriodicExecutorOptions, NYT::NConcurrency::NDetail::TPeriodicExecutorOptionsSerializer);
ASSIGN_EXTERNAL_YSON_SERIALIZER(NYT::NConcurrency::TRetryingPeriodicExecutorOptions, NYT::NConcurrency::NDetail::TRetryingPeriodicExecutorOptionsSerializer);
