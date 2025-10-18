#pragma once

#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/accessor/accessor.h>

#include <util/generic/size_literals.h>

namespace NKikimrConfig {

class TCheckpointCoordinatorConfig;

} // namespace NKikimrConfig

namespace NFq {

namespace NConfig {

class TCheckpointCoordinatorConfig;
class TCheckpointGcConfig;
class TStateStorageLimitsConfig;

} // namespace NConfig

class TCheckpointStorageSettings {
public:
    class TGcSettings {
    public:
        TGcSettings() = default;
        TGcSettings(const NConfig::TCheckpointGcConfig& config);

    private:
        YDB_ACCESSOR(bool, Enabled, true);
    };

    class TStateStorageLimits {
        static constexpr ui64 MaxYdbStringValueLength = 16 * 1000 * 1000;

    public:
        TStateStorageLimits() = default;
        TStateStorageLimits(const NConfig::TStateStorageLimitsConfig& config);

    private:
        YDB_ACCESSOR(ui64, MaxGraphCheckpointsSizeBytes, 1_TB);
        YDB_ACCESSOR(ui64, MaxTaskStateSizeBytes, 1_TB);
        YDB_ACCESSOR(ui64, MaxRowSizeBytes, MaxYdbStringValueLength)
    };

    TCheckpointStorageSettings() = default;
    TCheckpointStorageSettings(const NConfig::TCheckpointCoordinatorConfig& config);
    TCheckpointStorageSettings(const NKikimrConfig::TStreamingQueriesConfig_TExternalStorageConfig& config);

private:
    YDB_ACCESSOR_MUTABLE(TStateStorageLimits, StateStorageLimits, {});
    YDB_ACCESSOR_MUTABLE(TGcSettings, CheckpointGarbageConfig, {});
    YDB_ACCESSOR_MUTABLE(TExternalStorageSettings, ExternalStorage, {});
};

} // namespace NFq
