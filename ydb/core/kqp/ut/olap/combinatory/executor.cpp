#include "executor.h"
#include "wait_background_processes.h"

#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

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
    // The memory scopes and data sources of the last read are released by the scan and limiter actors after
    // the result has already reached the client, so the counters may lag behind the last command for a while
    // (up to seconds on a loaded sanitizer runner). Give that cleanup a bounded time before calling it a leak.
    const TBackgroundProcessesState state = WaitBackgroundProcesses(TDuration::Seconds(30));
    if (!state.IsClean()) {
        Cerr << "Background processes of the last command are still alive " << state.Waited
             << " after the client got its result: ProcessScopeCount=" << state.ProcessScopeCount
             << ", DataSourceCount=" << state.DataSourceCount << Endl;
    } else if (state.Waited) {
        Cerr << "Background processes of the last command finished " << state.Waited << " after the client got its result"
             << Endl;
    }
    AFL_VERIFY(state.ProcessScopeCount == 0)("count", state.ProcessScopeCount);
    AFL_VERIFY(state.DataSourceCount == 0)("count", state.DataSourceCount);
}

void TScriptExecutor::Execute() {
    auto settings = TKikimrSettings().SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
    Execute(settings);
}

}   // namespace NKikimr::NKqp
