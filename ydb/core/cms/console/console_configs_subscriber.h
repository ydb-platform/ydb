#pragma once

#include "defs.h"
#include "configs_config.h"
#include "console.h"

#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NConsole {
IActor * CreateConfigsSubscriber(const TActorId &ownerId, const TVector<ui32> &kinds, const NKikimrConfig::TAppConfig &currentConfig);
}
