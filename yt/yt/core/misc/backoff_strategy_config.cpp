#include "backoff_strategy_config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TSerializableExponentialBackoffOptions::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("invocation_count", &TThis::InvocationCount)
        .Default(DefaultInvocationCount);
    registrar.BaseClassParameter("min_backoff", &TThis::MinBackoff)
        .Default(DefaultMinBackoff);
    registrar.BaseClassParameter("max_backoff", &TThis::MaxBackoff)
        .Default(DefaultMaxBackoff);
    registrar.BaseClassParameter("backoff_multiplier", &TThis::BackoffMultiplier)
        .Default(DefaultBackoffMultiplier);
    registrar.BaseClassParameter("backoff_jitter", &TThis::BackoffJitter)
        .Default(DefaultBackoffJitter);
}

////////////////////////////////////////////////////////////////////////////////

void TSerializableConstantBackoffOptions::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("retry_count", &TThis::InvocationCount)
        .Default(DefaultInvocationCount);
    registrar.BaseClassParameter("backoff", &TThis::Backoff)
        .Default(DefaultBackoff);
    registrar.BaseClassParameter("backoff_jitter", &TThis::BackoffJitter)
        .Default(DefaultBackoffJitter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
