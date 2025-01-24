#pragma once

#include "defs.h"
#include "configs_config.h"
#include "console.h"

#include <ydb/library/actors/core/actor.h>

#include <optional>

namespace NKikimr::NConsole {

struct TNodeInfo {
    TString Tenant;
    TString NodeType;
};

IActor *CreateConfigsSubscriber(
    const TActorId &ownerId,
    const TVector<ui32> &kinds,
    const NKikimrConfig::TAppConfig &currentConfig,
    ui64 cookie = 0,
    bool processYaml = false,
    ui64 version = 0,
    const TString &yamlConfig = {},
    const TMap<ui64, TString> &volatileYamlConfigs = {},
    const std::optional<TNodeInfo> explicitNodeInfo = std::nullopt);

} // namespace NKikimr::NConsole
