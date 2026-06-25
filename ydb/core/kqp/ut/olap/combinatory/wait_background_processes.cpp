#include "wait_background_processes.h"

#include <ydb/core/tx/limiter/grouped_memory/service/process.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>

#include <ydb/library/signals/object_counter.h>

namespace NKikimr::NKqp {

TConclusionStatus TWaitBackgroundProcessesCommand::DoExecute(TKikimrRunner& /*kikimr*/) {
    const TInstant start = TInstant::Now();
    const TDuration timeout = TDuration::Seconds(TimeoutSeconds);
    ui64 processScopeCount = NColumnShard::TMonitoringObjectsCounter<NOlap::NGroupedMemoryManager::TProcessMemoryScope>::GetCounter().Val();
    ui64 dataSourceCount = NColumnShard::TMonitoringObjectsCounter<NOlap::NReader::NCommon::IDataSource>::GetCounter().Val();

    while (TInstant::Now() - start < timeout && (processScopeCount != 0 || dataSourceCount != 0)) {
        Cerr << "WAIT_BACKGROUND_PROCESSES: Waiting for background processes to complete... "
             << "ProcessScopeCount=" << processScopeCount << ", DataSourceCount=" << dataSourceCount << ", elapsed=" << (TInstant::Now() - start).Seconds() << "s" << Endl;

        Sleep(TDuration::Seconds(1));
        processScopeCount = NColumnShard::TMonitoringObjectsCounter<NOlap::NGroupedMemoryManager::TProcessMemoryScope>::GetCounter().Val();
        dataSourceCount = NColumnShard::TMonitoringObjectsCounter<NOlap::NReader::NCommon::IDataSource>::GetCounter().Val();
    }

    AFL_VERIFY(processScopeCount == 0 && dataSourceCount == 0)("error", TStringBuilder() << "WAIT_BACKGROUND_PROCESSES timeout after " << TimeoutSeconds << " seconds")(
        "ProcessScopeCount", processScopeCount)("DataSourceCount", dataSourceCount);

    Cerr << "WAIT_BACKGROUND_PROCESSES: All background processes completed successfully" << Endl;
    return TConclusionStatus::Success();
}

} // namespace NKikimr::NKqp
