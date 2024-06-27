#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>

#include <ydb/core/resource_pools/resource_pool_settings.h>


namespace NKikimr::NKqp {

using namespace NWorkload;


Y_UNIT_TEST_SUITE(KqpWorkloadServiceActors) {
    Y_UNIT_TEST(TestCreateDefaultPool) {
        auto ydb = TYdbSetupSettings().Create();

        const TString path = TStringBuilder() << ".resource_pools/" << NResourcePool::DEFAULT_POOL_ID;
        auto response = ydb->Navigate(path, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        UNIT_ASSERT_VALUES_EQUAL(response->ErrorCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(response->ResultSet.at(0).Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);

        // Create default pool
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQueryGrpc(TSampleQueries::TSelect42::Query, NResourcePool::DEFAULT_POOL_ID));

        // Check that default pool created
        response = ydb->Navigate(path, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        UNIT_ASSERT_VALUES_EQUAL(response->ResultSet.at(0).Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindResourcePool);
    }
}

}  // namespace NKikimr::NKqp
