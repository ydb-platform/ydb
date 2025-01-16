#include "tcmalloc_manager.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NTCMalloc {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TTCMallocConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TDynamicTCMallocConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TTCMallocConfigPtr& config)
{
    TTCMallocManager::Configure(config);
}

void ReconfigureSingleton(
    const TTCMallocConfigPtr& config,
    const TDynamicTCMallocConfigPtr& dynamicConfig)
{
    TTCMallocManager::Configure(config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "tcmalloc",
    TTCMallocConfig,
    TDynamicTCMallocConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTCMalloc
