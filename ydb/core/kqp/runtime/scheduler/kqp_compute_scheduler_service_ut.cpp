#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>

namespace NKikimr::NKqp {

using namespace NWorkload;

Y_UNIT_TEST_SUITE(KqpComputeSchedulerService) {

    /* Scenario:
        - Create Resource Pool with zero CPU.
        - Disable Scheduler.
        - Run query inside this Pool.
        - Query shouldn't timeout.
     */
    Y_UNIT_TEST(DisabledByFeatureFlag) {
        auto ydb = TYdbSetupSettings()
            .EnableResourcePoolsScheduler(false)
            .Create();

        const TString& poolId = "zero_pool";
        NResourcePool::TPoolSettings poolSettings;
        poolSettings.TotalCpuLimitPercentPerNode = 0;
        poolSettings.QueryCancelAfter = TDuration::Seconds(10);
        ydb->CreateResourcePool(poolId, poolSettings);

        auto request = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().PoolId(poolId));
        TSampleQueries::TSelect42::CheckResult(request.GetResult());
    }

}

} // namespace NKikimr::NKqp
