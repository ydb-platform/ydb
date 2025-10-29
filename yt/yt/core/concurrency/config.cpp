#include "config.h"

#include <yt/yt/core/misc/jitter.h>

namespace NYT::NConcurrency {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutorOptions TPeriodicExecutorOptions::WithJitter(TDuration period)
{
    return {
        .Period = period,
        .Jitter = DefaultJitter
    };
}

TDuration TPeriodicExecutorOptions::GenerateDelay() const
{
    if (!Period) {
        return TDuration::Max();
    }

    auto randomGenerator = [] {
        return 2.0 * RandomNumber<double>() - 1.0;
    };

    // Jitter is divided by 2 for historical reasons.
    return ApplyJitter(*Period, Jitter / 2.0, randomGenerator);
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

namespace {

void ValidateFiberStackPoolSizes(const THashMap<EExecutionStackKind, int>& poolSizes)
{
    for (auto [stackKind, poolSize] : poolSizes) {
        if (poolSize < 0) {
            THROW_ERROR_EXCEPTION("Pool size of %Qlv stack it not positive",
                stackKind);
        }
    }
}

} // namespace

TFiberManagerConfigPtr TFiberManagerConfig::ApplyDynamic(const TFiberManagerDynamicConfigPtr& dynamicConfig) const
{
    auto result = New<TFiberManagerConfig>();
    for (auto [key, value] : dynamicConfig->FiberStackPoolSizes) {
        result->FiberStackPoolSizes[key] = value;
    }
    UpdateYsonStructField(result->MaxIdleFibers, dynamicConfig->MaxIdleFibers);
    result->Postprocess();
    return result;
}

void TFiberManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("fiber_stack_pool_sizes", &TThis::FiberStackPoolSizes)
        .Default();
    registrar.Parameter("max_idle_fibers", &TThis::MaxIdleFibers)
        .Default(NConcurrency::DefaultMaxIdleFibers);

    registrar.Postprocessor([] (TThis* config) {
        ValidateFiberStackPoolSizes(config->FiberStackPoolSizes);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TFiberManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("fiber_stack_pool_sizes", &TThis::FiberStackPoolSizes)
        .Default();
    registrar.Parameter("max_idle_fibers", &TThis::MaxIdleFibers)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        ValidateFiberStackPoolSizes(config->FiberStackPoolSizes);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
