#include <ydb/core/base/appdata_fwd.h>

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

    Y_UNIT_TEST(TestDefaultPoolUsePermissions) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& userSID = "user@test";
        ydb->GetRuntime()->GetAppData().DefaultUserSIDs.emplace_back(userSID);

        // Create default pool
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQueryGrpc(TSampleQueries::TSelect42::Query, NResourcePool::DEFAULT_POOL_ID));

        // Check default pool access
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings()
            .PoolId(NResourcePool::DEFAULT_POOL_ID)
            .UserSID(userSID)
        ));
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings()
            .PoolId(NResourcePool::DEFAULT_POOL_ID)
            .UserSID(ydb->GetRuntime()->GetAppData().AllAuthenticatedUsers)
        ));
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings()
            .PoolId(NResourcePool::DEFAULT_POOL_ID)
            .UserSID(BUILTIN_ACL_ROOT)
        ));
    }

    Y_UNIT_TEST(TestDefaultPoolAdminPermissions) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& userSID = "user@test";
        ydb->GetRuntime()->GetAppData().AdministrationAllowedSIDs.emplace_back(userSID);

        // Create default pool
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQueryGrpc(TSampleQueries::TSelect42::Query, NResourcePool::DEFAULT_POOL_ID));

        // Check default pool access
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings()
            .PoolId(NResourcePool::DEFAULT_POOL_ID)
            .UserSID(userSID)
        ));

        // Check alter access
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            ALTER RESOURCE POOL )" << NResourcePool::DEFAULT_POOL_ID << R"( SET (
                QUEUE_SIZE=1
            );
        )");

        // Check grant access
        const TString& anotherUserSID = "another@sid";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            GRANT SELECT ROW ON `.resource_pools/)" << NResourcePool::DEFAULT_POOL_ID << "` TO `" << anotherUserSID << "`;"
        );
        TSampleQueries::TSelect42::CheckResult(ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, TQueryRunnerSettings()
            .PoolId(NResourcePool::DEFAULT_POOL_ID)
            .UserSID(anotherUserSID)
        ));

        // Check drop access
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            DROP RESOURCE POOL )" << NResourcePool::DEFAULT_POOL_ID << ";"
        );
    }
}

}  // namespace NKikimr::NKqp
