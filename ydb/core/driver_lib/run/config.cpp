#include "config.h"

namespace NKikimr {

TKikimrRunConfig::TKikimrRunConfig(NKikimrConfig::TAppConfig& appConfig, ui32 nodeId, const TKikimrScopeId& scopeId)
    : AppConfig(appConfig)
    , NodeId(nodeId)
    , ScopeId(scopeId)
{
    for (const auto& key: NYdb::NGlobalPlugins::TPluginFactory::GetRegisteredKeys()) {
        Plugins.emplace_back(NYdb::NGlobalPlugins::TPluginFactory::Construct(key));
    }
}

} // namespace NKikimr
