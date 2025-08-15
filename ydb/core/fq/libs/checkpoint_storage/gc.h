#pragma once

#include "checkpoint_storage.h"
#include "state_storage.h"

#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/protos/config.pb.h>

#include <memory>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewGC(
    const NKikimrConfig::TCheckpointsConfig::TCheckpointGcConfig& config,
    const TCheckpointStoragePtr& checkpointStorage,
    const TStateStoragePtr& stateStorage);

} // namespace NFq
