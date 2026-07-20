#include <ydb/core/base/path.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_query_classifier_ut_common.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NKqp {

using namespace NWorkload;
using namespace NYdb;

Y_UNIT_TEST_SUITE(TQueryClassifierHasAppName) {

    Y_UNIT_TEST(ShouldMatchAppName) {
        TClassifyTestCase tc;
        tc.ClassifierHasAppName = "my_app";
        tc.ContextAppName = "my_app";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "pool_target");
    }

    Y_UNIT_TEST(ShouldNotMatchDifferentAppName) {
        TClassifyTestCase tc;
        tc.ClassifierHasAppName = "expected_app";
        tc.ContextAppName = "other_app";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "default");
    }

    Y_UNIT_TEST(ShouldMatchAnyAppWhenFilterNotSet) {
        TClassifyTestCase tc;
        tc.ContextAppName = "some_random_app";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "pool_target");
    }

    Y_UNIT_TEST(ShouldRequireExactMatch) {
        TClassifyTestCase tc;
        tc.ClassifierHasAppName = "ydb-ui";
        tc.ContextAppName = "ydb-ui-prod";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "default");
    }
}

Y_UNIT_TEST_SUITE(HasAppNameDdl) {

    Y_UNIT_TEST(TestHasAppNameClassifier) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& poolId = "app_pool";
        const TString& userSID = "test@user";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=0
            );
            CREATE RESOURCE POOL CLASSIFIER app_classifier WITH (
                RESOURCE_POOL=")" << poolId << R"(",
                HAS_APP_NAME="my_app"
            );
        )");

        auto matchSettings = TQueryRunnerSettings().PoolId("").UserSID(userSID).ApplicationName("my_app");
        WaitForClassifierFail(ydb, matchSettings, poolId);

        auto noMatchSettings = TQueryRunnerSettings().PoolId("").UserSID(userSID).ApplicationName("other_app");
        WaitForClassifierSuccess(ydb, noMatchSettings);
    }

    Y_UNIT_TEST(TestAlterHasAppName) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& poolId = "alter_pool";
        const TString& userSID = "test@user";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=0
            );
            CREATE RESOURCE POOL CLASSIFIER alter_classifier WITH (
                RESOURCE_POOL=")" << poolId << R"(",
                HAS_APP_NAME="old_app"
            );
        )");

        auto oldMatch = TQueryRunnerSettings().PoolId("").UserSID(userSID).ApplicationName("old_app");
        WaitForClassifierFail(ydb, oldMatch, poolId);

        ydb->ExecuteSchemeQuery(R"(
            ALTER RESOURCE POOL CLASSIFIER alter_classifier SET (
                HAS_APP_NAME="new_app"
            );
        )");

        auto newMatch = TQueryRunnerSettings().PoolId("").UserSID(userSID).ApplicationName("new_app");
        WaitForClassifierFail(ydb, newMatch, poolId);

        auto oldNoMatch = TQueryRunnerSettings().PoolId("").UserSID(userSID).ApplicationName("old_app");
        WaitForClassifierSuccess(ydb, oldNoMatch);
    }
}

}  // namespace NKikimr::NKqp
