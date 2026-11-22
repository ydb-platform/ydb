#include "wait_background_processes.h"

#include <ydb/core/tx/limiter/grouped_memory/service/process.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>

#include <ydb/library/signals/object_counter.h>

#include <library/cpp/time_provider/monotonic.h>

namespace NKikimr::NKqp {

TBackgroundProcessesState TBackgroundProcessesState::Current() {
    TBackgroundProcessesState state;
    state.ProcessScopeCount =
        NColumnShard::TMonitoringObjectsCounter<NOlap::NGroupedMemoryManager::TProcessMemoryScope>::GetCounter().Val();
    state.DataSourceCount = NColumnShard::TMonitoringObjectsCounter<NOlap::NReader::NCommon::IDataSource>::GetCounter().Val();
    return state;
}

TBackgroundProcessesState WaitBackgroundProcesses(const TDuration timeout) {
    const TMonotonic start = TMonotonic::Now();
    TMonotonic nextReport = start + TDuration::Seconds(1);
    TBackgroundProcessesState state = TBackgroundProcessesState::Current();
    while (!state.IsClean() && TMonotonic::Now() - start < timeout) {
        if (nextReport <= TMonotonic::Now()) {
            Cerr << "WAIT_BACKGROUND_PROCESSES: Waiting for background processes to complete... "
                 << "ProcessScopeCount=" << state.ProcessScopeCount << ", DataSourceCount=" << state.DataSourceCount
                 << ", elapsed=" << (TMonotonic::Now() - start).Seconds() << "s" << Endl;
            nextReport += TDuration::Seconds(1);
        }
        Sleep(TDuration::MilliSeconds(100));
        state = TBackgroundProcessesState::Current();
        state.Waited = TMonotonic::Now() - start;
    }
    return state;
}

TConclusionStatus TWaitBackgroundProcessesCommand::DoExecute(TKikimrRunner& /*kikimr*/) {
    const TBackgroundProcessesState state = WaitBackgroundProcesses(TDuration::Seconds(TimeoutSeconds));

    AFL_VERIFY(state.IsClean())
        ("error", TStringBuilder() << "WAIT_BACKGROUND_PROCESSES timeout after " << TimeoutSeconds << " seconds")
        ("ProcessScopeCount", state.ProcessScopeCount)("DataSourceCount", state.DataSourceCount);

    Cerr << "WAIT_BACKGROUND_PROCESSES: All background processes completed successfully" << Endl;
    return TConclusionStatus::Success();
}

} // namespace NKikimr::NKqp
