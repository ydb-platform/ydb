#include "config.h"

namespace NKikimr {

TKikimrRunConfig::TKikimrRunConfig(NKikimrConfig::TAppConfig& appConfig, ui32 nodeId, const TKikimrScopeId& scopeId)
    : AppConfig(appConfig)
    , NodeId(nodeId)
    , ScopeId(scopeId)
{}

} // namespace NKikimr
