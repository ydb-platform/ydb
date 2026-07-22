#include <ydb/core/base/path.h>
#include <ydb/services/workload_manager/ut/common/query_classifier_ut_common.h>
#include <ydb/services/workload_manager/ut/common/workload_service_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NWorkloadManager {

using namespace NWorkloadManager;
using namespace NYdb;

namespace {

constexpr TStringBuf USER_SID = "test@user";

void CreatePoolAndClassifier(
    TIntrusivePtr<IYdbSetup> ydb,
    const TString& poolId,
    const TString& classifierName,
    const TString& appName)
{
    ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
        GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << USER_SID << R"(`;
        CREATE RESOURCE POOL )" << poolId << R"( WITH (
            CONCURRENT_QUERY_LIMIT=0
        );
        CREATE RESOURCE POOL CLASSIFIER )" << classifierName << R"( WITH (
            RESOURCE_POOL=")" << poolId << R"(",
            HAS_APP_NAME=")" << appName << R"("
        );
    )");
}

TQueryRunnerSettings QuerySettings(const TString& appName) {
    return TQueryRunnerSettings().PoolId("").UserSID(TString(USER_SID)).ApplicationName(appName);
}

}  // anonymous namespace

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
        CreatePoolAndClassifier(ydb, poolId, "app_classifier", "my_app");

        WaitForClassifierFail(ydb, QuerySettings("my_app"), poolId);
        WaitForClassifierSuccess(ydb, QuerySettings("other_app"));
    }

    Y_UNIT_TEST(TestAlterHasAppName) {
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "alter_pool";
        CreatePoolAndClassifier(ydb, poolId, "alter_classifier", "old_app");

        WaitForClassifierFail(ydb, QuerySettings("old_app"), poolId);

        ydb->ExecuteSchemeQuery(R"(
            ALTER RESOURCE POOL CLASSIFIER alter_classifier SET (
                HAS_APP_NAME="new_app"
            );
        )");

        WaitForClassifierFail(ydb, QuerySettings("new_app"), poolId);
        WaitForClassifierSuccess(ydb, QuerySettings("old_app"));
    }

    Y_UNIT_TEST(TestAlterHasAppNameEmptyClearsFilter) {
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "empty_pool";
        CreatePoolAndClassifier(ydb, poolId, "empty_classifier", "old_app");

        WaitForClassifierFail(ydb, QuerySettings("old_app"), poolId);

        ydb->ExecuteSchemeQuery(R"(
            ALTER RESOURCE POOL CLASSIFIER empty_classifier SET (
                HAS_APP_NAME=""
            );
        )");

        WaitForClassifierFail(ydb, QuerySettings("any_app"), poolId);
        WaitForClassifierFail(ydb, QuerySettings(""), poolId);
    }

    Y_UNIT_TEST(TestResetHasAppNameClearsFilter) {
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "reset_pool";
        CreatePoolAndClassifier(ydb, poolId, "reset_classifier", "old_app");

        WaitForClassifierFail(ydb, QuerySettings("old_app"), poolId);

        ydb->ExecuteSchemeQuery(R"(
            ALTER RESOURCE POOL CLASSIFIER reset_classifier RESET (HAS_APP_NAME);
        )");

        WaitForClassifierFail(ydb, QuerySettings("any_app"), poolId);
        WaitForClassifierFail(ydb, QuerySettings(""), poolId);
    }
}

}  // namespace NKikimr::NWorkloadManager
