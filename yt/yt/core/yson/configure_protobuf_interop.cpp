#include "protobuf_interop.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NYson {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TProtobufInteropConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TProtobufInteropDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TProtobufInteropConfigPtr& config)
{
    SetProtobufInteropConfig(config);
}

void ReconfigureSingleton(
    const TProtobufInteropConfigPtr& config,
    const TProtobufInteropDynamicConfigPtr& dynamicConfig)
{
    ConfigureSingleton(config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "protobuf_interop",
    TProtobufInteropConfig,
    TProtobufInteropDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
