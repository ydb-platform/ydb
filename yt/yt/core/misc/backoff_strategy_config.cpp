#include "backoff_strategy_config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TExponentialBackoffOptionsSerializer::Register(TRegistrar registrar)
{
    registrar.ExternalClassParameter("invocation_count", &TThat::InvocationCount)
        .Alias("retry_count")
        .Default(TThat::DefaultInvocationCount);

    registrar.ExternalClassParameter("min_backoff", &TThat::MinBackoff)
        .Default(TThat::DefaultMinBackoff);

    registrar.ExternalClassParameter("max_backoff", &TThat::MaxBackoff)
        .Default(TThat::DefaultMaxBackoff);

    registrar.ExternalClassParameter("backoff_multiplier", &TThat::BackoffMultiplier)
        .Default(TThat::DefaultBackoffMultiplier)
        .GreaterThanOrEqual(1.0);

    registrar.ExternalClassParameter("backoff_jitter", &TThat::BackoffJitter)
        .Default(TThat::DefaultBackoffJitter);
}

////////////////////////////////////////////////////////////////////////////////

void TConstantBackoffOptionsSerializer::Register(TRegistrar registrar)
{
    registrar.ExternalClassParameter("invocation_count", &TThat::InvocationCount)
        .Alias("retry_count")
        .Default(TThat::DefaultInvocationCount);

    registrar.ExternalClassParameter("backoff", &TThat::Backoff)
        .Default(TThat::DefaultBackoff);

    registrar.ExternalClassParameter("backoff_jitter", &TThat::BackoffJitter)
        .Default(TThat::DefaultBackoffJitter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
