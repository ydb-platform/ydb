#pragma once

#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/accessor/accessor.h>

#include <util/generic/size_literals.h>

namespace NKikimrConfig {

class TStreamingQueriesConfig_TExternalStorageConfig;

} // namespace NKikimrConfig

namespace NFq {

namespace NConfig {

class TCompileServiceConfig;
class TJsonParserConfig;
class TRowDispatcherConfig;
class TRowDispatcherCoordinatorConfig;

} // namespace NConfig

class TRowDispatcherSettings {
public:
    class TJsonParserSettings {
    public:
        TJsonParserSettings() = default;
        TJsonParserSettings(const NConfig::TJsonParserConfig& config);

    private:
        YDB_ACCESSOR(ui64, BatchSizeBytes, 1_MB);
        YDB_ACCESSOR(TDuration, BatchCreationTimeout, TDuration::Seconds(1));
        YDB_ACCESSOR(ui64, BufferCellCount, 1000'000);
    };

    class TCompileServiceSettings {
    public:
        TCompileServiceSettings() = default;
        TCompileServiceSettings(const NConfig::TCompileServiceConfig& config);

    private:
        YDB_ACCESSOR(ui64, ParallelCompilationLimit, 1);
    };

    class TCoordinatorSettings {
    public:
        TCoordinatorSettings() = default;
        TCoordinatorSettings(const NConfig::TRowDispatcherCoordinatorConfig& config);
        TCoordinatorSettings(const NKikimrConfig::TStreamingQueriesConfig_TExternalStorageConfig& config);

    private:
        YDB_ACCESSOR(bool, LocalMode, false);
        YDB_ACCESSOR_MUTABLE(TExternalStorageSettings, Database, {});
        YDB_ACCESSOR_DEF(TString, CoordinationNodePath);
    };

    enum class EConsumerMode {
        Without,
        Required,
        Auto,
    };

    TRowDispatcherSettings() = default;
    TRowDispatcherSettings(const NConfig::TRowDispatcherConfig& config);
    TRowDispatcherSettings(const NKikimrConfig::TStreamingQueriesConfig_TExternalStorageConfig& config);

private:
    YDB_ACCESSOR_MUTABLE(TCoordinatorSettings, Coordinator, {});
    YDB_ACCESSOR_MUTABLE(TCompileServiceSettings, CompileService, {});
    YDB_ACCESSOR_MUTABLE(TJsonParserSettings, JsonParser, {});
    YDB_ACCESSOR(TDuration, SendStatusPeriod, TDuration::Seconds(10));
    YDB_ACCESSOR(TDuration, TimeoutBeforeStartSession, TDuration::Seconds(10));
    YDB_ACCESSOR(ui64, MaxSessionUsedMemory, 4_MB);
    YDB_ACCESSOR(EConsumerMode, ConsumerMode, EConsumerMode::Auto);
};

} // namespace NFq
