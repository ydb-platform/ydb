#include "row_dispatcher_settings.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/fq/libs/config/protos/row_dispatcher.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NFq {

TRowDispatcherSettings::TJsonParserSettings::TJsonParserSettings(const NConfig::TJsonParserConfig& config)
    : BatchCreationTimeout(TDuration::MilliSeconds(config.GetBatchCreationTimeoutMs()))
    , SkipErrors(config.GetSkipErrors())
{
    if (config.GetBatchSizeBytes()) {
        BatchSizeBytes = config.GetBatchSizeBytes();
    }
    if (config.GetBufferCellCount()) {
        BufferCellCount = config.GetBufferCellCount();
    }
}

TRowDispatcherSettings::TCompileServiceSettings::TCompileServiceSettings(const NConfig::TCompileServiceConfig& config) {
    if (config.GetParallelCompilationLimit()) {
        ParallelCompilationLimit = config.GetParallelCompilationLimit();
    }
}

TRowDispatcherSettings::TCoordinatorSettings::TCoordinatorSettings(const NConfig::TRowDispatcherCoordinatorConfig& config)
    : LocalMode(config.GetLocalMode())
    , Database(config.GetDatabase())
    , CoordinationNodePath(config.GetCoordinationNodePath())
    , RebalancingTimeout(TDuration::Seconds(config.GetRebalancingTimeoutSec()))
{}

TRowDispatcherSettings::TCoordinatorSettings::TCoordinatorSettings(const NKikimrConfig::TStreamingQueriesConfig::TExternalStorageConfig& config)
    : Database(config)
    , CoordinationNodePath(Database.GetPathPrefix())
{}

TRowDispatcherSettings::TRowDispatcherSettings(const NConfig::TRowDispatcherConfig& config)
    : Coordinator(config.GetCoordinator())
    , CompileService(config.GetCompileService())
    , JsonParser(config.GetJsonParser())
    , SendStatusPeriod(TDuration::Seconds(config.GetSendStatusPeriodSec()))
    , TimeoutBeforeStartSession(TDuration::Seconds(config.GetTimeoutBeforeStartSessionSec()))
    , MaxSessionUsedMemory(config.GetMaxSessionUsedMemory())
    , ConsumerMode(config.GetWithoutConsumer() ? EConsumerMode::Without : EConsumerMode::Required)
{}

TRowDispatcherSettings::TRowDispatcherSettings(const NKikimrConfig::TStreamingQueriesConfig::TExternalStorageConfig& config)
    : Coordinator(config)
{
    std::optional<size_t> userPoolSize;
    if (NKikimr::HasAppData() && NActors::TlsActivationContext && NActors::TlsActivationContext->ActorSystem()) {
        userPoolSize = NActors::TlsActivationContext->ActorSystem()->GetPoolThreadsCount(NKikimr::AppData()->UserPoolId);
    }

    CompileService.SetParallelCompilationLimit(std::max(userPoolSize.value_or(NSystemInfo::NumberOfCpus()) / 2, static_cast<size_t>(1)));
}

} // namespace NFq
