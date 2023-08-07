#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

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

DEFINE_ENUM(EHistoricUsageAggregationMode,
    ((None)                     (0))
    ((ExponentialMovingAverage) (1))
);

class THistoricUsageConfig
    : public NYTree::TYsonStruct
{
public:
    EHistoricUsageAggregationMode AggregationMode;

    //! Parameter of exponential moving average (EMA) of the aggregated usage.
    //! Roughly speaking, it means that current usage ratio is twice as relevant for the
    //! historic usage as the usage ratio alpha seconds ago.
    //! EMA for unevenly spaced time series was adapted from here: https://clck.ru/HaGZs
    double EmaAlpha;

    REGISTER_YSON_STRUCT(THistoricUsageConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THistoricUsageConfig)

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

} // namespace NYT
