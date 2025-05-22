#include "resource_tracker.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NProfiling {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TResourceTrackerConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TResourceTrackerConfigPtr& config)
{
    TResourceTracker::Configure(config);
}

YT_DEFINE_CONFIGURABLE_SINGLETON(
    "resource_tracker",
    TResourceTrackerConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
