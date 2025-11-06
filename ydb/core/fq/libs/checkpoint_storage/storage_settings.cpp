#include "storage_settings.h"

#include <ydb/core/fq/libs/config/protos/checkpoint_coordinator.pb.h>
#include <ydb/core/protos/config.pb.h>

namespace NFq {

TCheckpointStorageSettings::TGcSettings::TGcSettings(const NConfig::TCheckpointGcConfig& config)
    : Enabled(config.GetEnabled())
{}

TCheckpointStorageSettings::TStateStorageLimits::TStateStorageLimits(const NConfig::TStateStorageLimitsConfig& config) {
    if (config.GetMaxGraphCheckpointsSizeBytes()) {
        SetMaxGraphCheckpointsSizeBytes(config.GetMaxGraphCheckpointsSizeBytes());
    }

    if (config.GetMaxTaskStateSizeBytes()) {
        SetMaxTaskStateSizeBytes(config.GetMaxTaskStateSizeBytes());
    }

    if (config.GetMaxRowSizeBytes()) {
        SetMaxRowSizeBytes(config.GetMaxRowSizeBytes());
    }
}

TCheckpointStorageSettings::TCheckpointStorageSettings(const NKikimrConfig::TStreamingQueriesConfig::TExternalStorageConfig& config)
    : ExternalStorage(config)
{}

TCheckpointStorageSettings::TCheckpointStorageSettings(const NConfig::TCheckpointCoordinatorConfig& config)
    : StateStorageLimits(config.GetStateStorageLimits())
    , CheckpointGarbageConfig(config.GetCheckpointGarbageConfig())
    , ExternalStorage(config.GetStorage())
{}

} // namespace NFq
