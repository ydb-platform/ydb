#pragma once

#include "config.h"

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct THistoricUsageAggregationParameters
{
    THistoricUsageAggregationParameters() = default;
    explicit THistoricUsageAggregationParameters(EHistoricUsageAggregationMode mode, double emaAlpha = 0.0);
    explicit THistoricUsageAggregationParameters(const THistoricUsageConfigPtr& config);

    bool operator==(const THistoricUsageAggregationParameters& other) const;

    EHistoricUsageAggregationMode Mode = EHistoricUsageAggregationMode::None;

    //! Parameter of exponential moving average (EMA) of the aggregated usage.
    //! Roughly speaking, it means that current usage ratio is twice as relevant for the
    //! historic usage as the usage ratio alpha seconds ago.
    //! EMA for unevenly spaced time series was adapted from here: https://clck.ru/HaGZs
    double EmaAlpha = 0.0;
};

////////////////////////////////////////////////////////////////////////////////

class THistoricUsageAggregator
{
public:
    THistoricUsageAggregator();
    THistoricUsageAggregator(const THistoricUsageAggregator& other) = default;
    THistoricUsageAggregator& operator=(const THistoricUsageAggregator& other) = default;

    //! Update the parameters. If the parameters have changed, resets the state.
    void UpdateParameters(const THistoricUsageAggregationParameters& newParameters);

    void Reset();

    void UpdateAt(TInstant now, double value);

    double GetHistoricUsage() const;

private:
    THistoricUsageAggregationParameters Parameters_;

    double ExponentialMovingAverage_;

    TInstant LastExponentialMovingAverageUpdateTime_;
};

////////////////////////////////////////////////////////////////////////////////

class TAverageHistoricUsageAggregator
{
public:
    explicit TAverageHistoricUsageAggregator(TDuration period = TDuration::Seconds(1));
    TAverageHistoricUsageAggregator(const TAverageHistoricUsageAggregator& other) = default;
    TAverageHistoricUsageAggregator& operator=(const TAverageHistoricUsageAggregator& other) = default;

    void UpdateParameters(THistoricUsageAggregationParameters params);

    double GetHistoricUsage();

    void UpdateAt(TInstant now, double value);

private:
    TDuration Period_;

    TInstant IntervalStart_ = TInstant::Zero();
    double CurrentUsage_ = 0;

    THistoricUsageAggregator HistoricUsageAggregator_;

    void MaybeFlush(TInstant now);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
