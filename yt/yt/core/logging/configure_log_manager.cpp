#include "log_manager.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NLogging {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TLogManagerConfigPtr>& parameter)
{
    parameter
        .DefaultCtor([] { return NLogging::TLogManagerConfig::CreateDefault(); })
        .ResetOnLoad();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TLogManagerDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TLogManagerConfigPtr& config)
{
    if (!NLogging::TLogManager::Get()->IsConfiguredFromEnv()) {
        TLogManager::Get()->Configure(
            config,
            /*sync*/ true);
    }
}

void ReconfigureSingleton(
    const TLogManagerConfigPtr& config,
    const TLogManagerDynamicConfigPtr& dynamicConfig)
{
    if (!NLogging::TLogManager::Get()->IsConfiguredFromEnv()) {
        NLogging::TLogManager::Get()->Configure(
            config->ApplyDynamic(dynamicConfig),
            /*sync*/ false);
    }
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "logging",
    TLogManagerConfig,
    TLogManagerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
