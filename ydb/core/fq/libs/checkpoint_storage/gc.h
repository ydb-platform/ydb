#pragma once

#include "checkpoint_storage.h"
#include "state_storage.h"
#include "storage_settings.h"

#include <ydb/library/actors/core/actor.h>

#include <memory>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewGC(
    const TCheckpointStorageSettings::TGcSettings& config,
    const TCheckpointStoragePtr& checkpointStorage,
    const TStateStoragePtr& stateStorage);

} // namespace NFq
