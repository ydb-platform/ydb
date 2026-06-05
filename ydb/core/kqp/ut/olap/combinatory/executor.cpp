#include "executor.h"

#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/limiter/grouped_memory/service/process.h>

#include <ydb/library/signals/object_counter.h>

namespace NKikimr::NKqp {

void TScriptExecutor::Execute(const TKikimrSettings& settings) {
    TKikimrRunner kikimr(settings);
    // Shorten LongTx delays so MinSnapshotForNewReads advances quickly enough for cleanup
    // (portions physical deletion, schema version collapsing) to happen within command timeouts.
    // Total delay = 1+1+1+10 = 13s; commands that wait for cleanup now use 30s windows.
    auto& longTxConfig = kikimr.GetTestServer().GetRuntime()->GetAppData().LongTxServiceConfig;
    longTxConfig.SetLocalSnapshotPromotionTimeSeconds(1);
    longTxConfig.SetSnapshotsExchangeIntervalSeconds(1);
    longTxConfig.SetSnapshotsRegistryUpdateIntervalSeconds(1);
    auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
    csController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
    csController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
    csController->SetOverrideMemoryLimitForPortionReading(1e+10);
    csController->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());
    for (auto&& i : Commands) {
        i->Execute(kikimr);
    }
    AFL_VERIFY(NColumnShard::TMonitoringObjectsCounter<NOlap::NGroupedMemoryManager::TProcessMemoryScope>::GetCounter().Val() == 0)(
                   "count", NColumnShard::TMonitoringObjectsCounter<NOlap::NGroupedMemoryManager::TProcessMemoryScope>::GetCounter().Val());
    AFL_VERIFY(NColumnShard::TMonitoringObjectsCounter<NOlap::NReader::NCommon::IDataSource>::GetCounter().Val() == 0)(
                   "count", NColumnShard::TMonitoringObjectsCounter<NOlap::NReader::NCommon::IDataSource>::GetCounter().Val());
}

void TScriptExecutor::Execute() {
    auto settings = TKikimrSettings().SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
    Execute(settings);
}

}   // namespace NKikimr::NKqp
