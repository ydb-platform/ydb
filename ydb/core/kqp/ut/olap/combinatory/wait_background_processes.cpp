#include "wait_background_processes.h"

#include <ydb/core/tx/limiter/grouped_memory/service/process.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>

#include <ydb/library/signals/object_counter.h>

namespace NKikimr::NKqp {

TBackgroundProcessesState TBackgroundProcessesState::Current() {
    TBackgroundProcessesState state;
    state.ProcessScopeCount =
        NColumnShard::TMonitoringObjectsCounter<NOlap::NGroupedMemoryManager::TProcessMemoryScope>::GetCounter().Val();
    state.DataSourceCount = NColumnShard::TMonitoringObjectsCounter<NOlap::NReader::NCommon::IDataSource>::GetCounter().Val();
    return state;
}

TBackgroundProcessesState WaitBackgroundProcesses(const TDuration timeout) {
    const TInstant start = TInstant::Now();
    TInstant nextReport = start + TDuration::Seconds(1);
    TBackgroundProcessesState state = TBackgroundProcessesState::Current();
    while (!state.IsClean() && TInstant::Now() - start < timeout) {
        if (nextReport <= TInstant::Now()) {
            Cerr << "WAIT_BACKGROUND_PROCESSES: Waiting for background processes to complete... "
                 << "ProcessScopeCount=" << state.ProcessScopeCount << ", DataSourceCount=" << state.DataSourceCount
                 << ", elapsed=" << (TInstant::Now() - start).Seconds() << "s" << Endl;
            nextReport += TDuration::Seconds(1);
        }
        Sleep(TDuration::MilliSeconds(100));
        state = TBackgroundProcessesState::Current();
        state.Waited = TInstant::Now() - start;
    }
    return state;
}

TConclusionStatus TWaitBackgroundProcessesCommand::DoExecute(TKikimrRunner& /*kikimr*/) {
    const TBackgroundProcessesState state = WaitBackgroundProcesses(TDuration::Seconds(TimeoutSeconds));

    YDB_LOG_VERIFY(state.IsClean(),
        {"error", TStringBuilder() << "WAIT_BACKGROUND_PROCESSES timeout after " << TimeoutSeconds << " seconds"},
        {"processScopeCount", state.ProcessScopeCount},
        {"dataSourceCount", state.DataSourceCount});

    Cerr << "WAIT_BACKGROUND_PROCESSES: All background processes completed successfully" << Endl;
    return TConclusionStatus::Success();
}

} // namespace NKikimr::NKqp
