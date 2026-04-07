#include "config.h"

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

void TBaggageManagerConfig::ApplyDynamicInplace(const TBaggageManagerDynamicConfigPtr& dynamicConfig)
{
    EnableBaggageAddition = dynamicConfig->EnableBaggageAddition.value_or(EnableBaggageAddition);
}

TBaggageManagerConfigPtr TBaggageManagerConfig::ApplyDynamic(const TBaggageManagerDynamicConfigPtr& dynamicConfig) const
{
    auto config = CloneYsonStruct(MakeStrong(this));
    config->ApplyDynamicInplace(dynamicConfig);
    config->Postprocess();
    return config;
}

void TBaggageManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_baggage_addition", &TThis::EnableBaggageAddition)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TBaggageManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_baggage_addition", &TThis::EnableBaggageAddition)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
