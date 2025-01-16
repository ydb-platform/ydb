#include "tracer.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NTracing {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TJaegerTracerConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TJaegerTracerDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TJaegerTracerConfigPtr& config)
{
    SetGlobalTracer(New<TJaegerTracer>(config));
}

void ReconfigureSingleton(
    const TJaegerTracerConfigPtr& config,
    const TJaegerTracerDynamicConfigPtr& dynamicConfig)
{
    auto tracer = NTracing::GetGlobalTracer();
    auto jaegerTracer = DynamicPointerCast<NTracing::TJaegerTracer>(tracer);
    YT_VERIFY(jaegerTracer);
    jaegerTracer->Configure(config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "jaeger",
    TJaegerTracerConfig,
    TJaegerTracerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
