#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>

#include <ydb/library/testlib/helpers.h>

namespace NKikimr::NKqp {

using namespace NWorkload;

Y_UNIT_TEST_SUITE(KqpComputeSchedulerService) {

    /* Scenario:
        - Create resource pool with zero CPU.
        - Enable or disable Scheduler on start.
        - Run query inside this pool.
        - Query shouldn't timeout or should be cancelled by timeout.
     */
    Y_UNIT_TEST_TWIN(FeatureFlagOnStart, Enabled) {
        auto ydb = TYdbSetupSettings()
            .EnableResourcePools(true)
            .EnableResourcePoolsScheduler(Enabled)
            .Create();

        const TString& poolId = "zero_pool";
        NResourcePool::TPoolSettings poolSettings;
        poolSettings.TotalCpuLimitPercentPerNode = 0;
        poolSettings.QueryCancelAfter = TDuration::Seconds(10);
        ydb->CreateResourcePool(poolId, poolSettings);

        auto request = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().PoolId(poolId));
        const auto& result = request.GetResult();
        UNIT_ASSERT_EQUAL(result.Response.GetResponse().GetEffectivePoolId(), poolId);
        if (!Enabled) {
            TSampleQueries::TSelect42::CheckResult(result);
        } else {
            TSampleQueries::CheckCancelled(result);
        }
    }

    /* Scenario:
        - Create resource pool with zero CPU.
        - Enable Scheduler on start, but disable Workload Manager.
        - Run query inside this pool - it should actually go to default pool.
        - Query shouldn't timeout.
     */
    Y_UNIT_TEST(EnabledSchedulerWithDisabledWorkloadManager) {
        auto ydb = TYdbSetupSettings()
            .EnableResourcePools(false)
            .EnableResourcePoolsScheduler(true)
            .Create();

        const TString& poolId = "zero_pool";
        NResourcePool::TPoolSettings poolSettings;
        poolSettings.TotalCpuLimitPercentPerNode = 0;
        poolSettings.QueryCancelAfter = TDuration::Seconds(10);
        ydb->CreateResourcePool(poolId, poolSettings);

        auto request = ydb->ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().PoolId(poolId));
        auto result = request.GetResult();
        UNIT_ASSERT_EQUAL(result.Response.GetResponse().GetEffectivePoolId(), NResourcePool::DEFAULT_POOL_ID);
        TSampleQueries::TSelect42::CheckResult(result);
    }

}

} // namespace NKikimr::NKqp
