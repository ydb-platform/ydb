#include "executor.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/limiter/grouped_memory/service/process.h>

#include <ydb/library/signals/object_counter.h>

namespace NKikimr::NKqp {

void TScriptExecutor::Execute() {
    auto settings = TKikimrSettings().SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
    TKikimrRunner kikimr(settings);
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

}   // namespace NKikimr::NKqp
