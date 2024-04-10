#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TExponentialBackoffOptions
{
    static constexpr int DefaultInvocationCount = 10;
    static constexpr auto DefaultMinBackoff = TDuration::Seconds(1);
    static constexpr auto DefaultMaxBackoff = TDuration::Seconds(5);
    static constexpr double DefaultBackoffMultiplier = 1.5;
    static constexpr double DefaultBackoffJitter = 0.1;

    int InvocationCount = DefaultInvocationCount;
    TDuration MinBackoff = DefaultMinBackoff;
    TDuration MaxBackoff = DefaultMaxBackoff;
    double BackoffMultiplier = DefaultBackoffMultiplier;
    double BackoffJitter = DefaultBackoffJitter;
};

////////////////////////////////////////////////////////////////////////////////

struct TConstantBackoffOptions
{
    static constexpr int DefaultInvocationCount = 10;
    static constexpr auto DefaultBackoff = TDuration::Seconds(3);
    static constexpr double DefaultBackoffJitter = 0.1;

    int InvocationCount = DefaultInvocationCount;
    TDuration Backoff = DefaultBackoff;
    double BackoffJitter = DefaultBackoffJitter;

    operator TExponentialBackoffOptions() const;
};

////////////////////////////////////////////////////////////////////////////////

//! TODO(arkady-e1ppa): Make configs below pairs of POD-structs and TExternalizedYsonStruct.

class TLogDigestConfig
    : public NYTree::TYsonStruct
{
public:
    // We will round each sample x to the range from [(1 - RelativePrecision)*x, (1 + RelativePrecision)*x].
    // This parameter affects the memory usage of the digest, it is proportional to
    // log(UpperBound / LowerBound) / log(1 + RelativePrecision).
    double RelativePrecision;

    // The bounds of the range operated by the class.
    double LowerBound;
    double UpperBound;

    // The value that is returned when there are no samples in the digest.
    std::optional<double> DefaultValue;

    REGISTER_YSON_STRUCT(TLogDigestConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLogDigestConfig)

////////////////////////////////////////////////////////////////////////////////

class THistogramDigestConfig
    : public NYTree::TYsonStruct
{
public:
    // We will round each sample x to a value from [x - AbsolutePrecision / 2, x + AbsolutePrecision / 2].
    // More precisely, size of each bucket in the histogram will be equal to AbsolutePrecision.
    // This parameter affects the memory usage of the digest, it is proportional to ((UpperBound - LowerBound) / AbsolutePrecision).
    double AbsolutePrecision;

    // The bounds of the range operated by the class.
    double LowerBound;
    double UpperBound;

    // The value that is returned when there are no samples in the digest.
    std::optional<double> DefaultValue;

    REGISTER_YSON_STRUCT(THistogramDigestConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THistogramDigestConfig)

////////////////////////////////////////////////////////////////////////////////

class TAdaptiveHedgingManagerConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! Percentage of primary requests that should have a hedging counterpart.
    //! Null is for disabled hedging.
    std::optional<double> MaxBackupRequestRatio;

    //! Period for hedging delay tuning and profiling.
    TDuration TickPeriod;

    //! Each tick hedging delay is tuned according to |MaxBackupRequestRatio| by |HedgingDelayTuneFactor|.
    double HedgingDelayTuneFactor;
    TDuration MinHedgingDelay;
    TDuration MaxHedgingDelay;

    REGISTER_YSON_STRUCT(TAdaptiveHedgingManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAdaptiveHedgingManagerConfig)

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

class TExponentialBackoffOptionsSerializer
    : public virtual NYTree::TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TExponentialBackoffOptions, TExponentialBackoffOptionsSerializer);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TConstantBackoffOptionsSerializer
    : public NYTree::TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TConstantBackoffOptions, TConstantBackoffOptionsSerializer);

    static void Register(TRegistrar registrar);
};

} // namespace NDetail

ASSIGN_EXTERNAL_YSON_SERIALIZER(TExponentialBackoffOptions, NDetail::TExponentialBackoffOptionsSerializer);
ASSIGN_EXTERNAL_YSON_SERIALIZER(TConstantBackoffOptions, NDetail::TConstantBackoffOptionsSerializer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
