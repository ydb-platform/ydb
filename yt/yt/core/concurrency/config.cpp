#include "config.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutorOptions TPeriodicExecutorOptions::WithJitter(TDuration period)
{
    return {
        .Period = period,
        .Jitter = DefaultJitter
    };
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

void TPeriodicExecutorOptionsSerializer::Register(TRegistrar registrar)
{
    registrar.ExternalClassParameter("period", &TThat::Period)
        .Default();
    registrar.ExternalClassParameter("splay", &TThat::Splay)
        .Default(TDuration::Zero());
    registrar.ExternalClassParameter("jitter", &TThat::Jitter)
        .Default(TThat::DefaultJitter);
}

////////////////////////////////////////////////////////////////////////////////

void TRetryingPeriodicExecutorOptionsSerializer::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TThroughputThrottlerConfigPtr TThroughputThrottlerConfig::Create(std::optional<double> limit)
{
    auto result = New<TThroughputThrottlerConfig>();
    result->Limit = limit;
    return result;
}

void TThroughputThrottlerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("limit", &TThis::Limit)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.Parameter("period", &TThis::Period)
        .Default(TDuration::MilliSeconds(1000));
}

std::optional<i64> TThroughputThrottlerConfig::GetMaxAvailable() const
{
    if (Limit.has_value()) {
        return static_cast<i64>(Period.SecondsFloat() * *Limit);
    } else {
        return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

TRelativeThroughputThrottlerConfigPtr TRelativeThroughputThrottlerConfig::Create(std::optional<double> limit)
{
    auto result = New<TRelativeThroughputThrottlerConfig>();
    result->Limit = limit;
    return result;
}

void TRelativeThroughputThrottlerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("relative_limit", &TThis::RelativeLimit)
        .InRange(0.0, 1.0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

bool TThroughputThrottlerConfig::operator==(const NConcurrency::TThroughputThrottlerConfig& other)
{
    return Limit == other.Limit && Period == other.Period;
}

////////////////////////////////////////////////////////////////////////////////

void TPrefetchingThrottlerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("target_rps", &TThis::TargetRps)
        .Default(1.0)
        .GreaterThan(1e-3);
    registrar.Parameter("min_prefetch_amount", &TThis::MinPrefetchAmount)
        .Default(1)
        .GreaterThanOrEqual(1);
    registrar.Parameter("max_prefetch_amount", &TThis::MaxPrefetchAmount)
        .Default(10)
        .GreaterThanOrEqual(1);
    registrar.Parameter("window", &TThis::Window)
        .GreaterThan(TDuration::MilliSeconds(1))
        .Default(TDuration::Seconds(1));

    registrar.Postprocessor([] (TThis* config) {
        if (config->MinPrefetchAmount > config->MaxPrefetchAmount) {
            THROW_ERROR_EXCEPTION("\"min_prefetch_amount\" should be less than or equal \"max_prefetch_amount\"")
                << TErrorAttribute("min_prefetch_amount", config->MinPrefetchAmount)
                << TErrorAttribute("max_prefetch_amount", config->MaxPrefetchAmount);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
