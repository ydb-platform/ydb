#include <ydb/core/base/path.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_query_classifier_ut_common.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NKqp {

using namespace NWorkload;
using namespace NYdb;

namespace {

const IQueryClassifier::TReject& AssertReject(const IQueryClassifier::TPreCompileClassifyResult& result) {
    UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TReject>(result),
        TStringBuilder() << "Expected TReject, got variant index " << result.index());
    return std::get<IQueryClassifier::TReject>(result);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TQueryClassifierActionReject) {

    Y_UNIT_TEST(ShouldRejectWhenActionSet) {
        TClassifyTestCase tc;
        tc.ClassifierAction = "reject";

        auto result = tc.RunPreClassify();
        const auto& reject = AssertReject(result);
        UNIT_ASSERT_EQUAL(reject.Code, Ydb::StatusIds::PRECONDITION_FAILED);
        UNIT_ASSERT_STRING_CONTAINS(reject.Message, "Request is rejected by classifier 'c_main'");
        UNIT_ASSERT_STRING_CONTAINS(reject.Message, "rank=100");
    }

    Y_UNIT_TEST(ShouldRejectEvenWithResourcePool) {
        // action wins over resource_pool when both are specified
        TClassifyTestCase tc;
        tc.ResourcePool = "pool_target";
        tc.ClassifierAction = "reject";

        AssertReject(tc.RunPreClassify());
    }

    Y_UNIT_TEST(ShouldNotRejectWhenClassifierDoesNotMatch) {
        TClassifyTestCase tc;
        tc.ClassifierAction = "reject";
        tc.ClassifierMemberName = "bob";
        tc.ContextMemberName = "alice";

        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "default");
    }
}

Y_UNIT_TEST_SUITE(ActionRejectDdl) {

    Y_UNIT_TEST(TestActionRejectClassifier) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& userSID = "test@user";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
            CREATE RESOURCE POOL CLASSIFIER cl_reject WITH (
                MEMBER_NAME=")" << userSID << R"(",
                ACTION="reject",
                RANK=100
            );
        )");

        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        ydb->WaitFor(TDuration::Seconds(10), "Resource pool classifier reject", [ydb, settings](TString& errorString) {
            auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings);
            errorString = result.GetIssues().ToOneLineString();
            return result.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED
                && errorString.Contains("Request is rejected by classifier 'cl_reject'")
                && errorString.Contains("rank=100");
        });
    }

    Y_UNIT_TEST(TestActionRejectCaseInsensitive) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& userSID = "test@user";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
            CREATE RESOURCE POOL CLASSIFIER cl_reject_ci WITH (
                MEMBER_NAME=")" << userSID << R"(",
                ACTION="REJECT",
                RANK=100
            );
        )");

        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        ydb->WaitFor(TDuration::Seconds(10), "Resource pool classifier reject (case-insensitive action)", [ydb, settings](TString& errorString) {
            auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings);
            errorString = result.GetIssues().ToOneLineString();
            return result.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED
                && errorString.Contains("Request is rejected by classifier 'cl_reject_ci'");
        });
    }

    Y_UNIT_TEST(TestActionRejectWithoutResourcePool) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& userSID = "test@user";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
            CREATE RESOURCE POOL CLASSIFIER cl_reject_no_pool WITH (
                MEMBER_NAME=")" << userSID << R"(",
                ACTION="reject",
                RANK=100
            );
        )");

        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        ydb->WaitFor(TDuration::Seconds(10), "Resource pool classifier reject (no pool)", [ydb, settings](TString& errorString) {
            auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings);
            errorString = result.GetIssues().ToOneLineString();
            return result.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED
                && errorString.Contains("Request is rejected by classifier 'cl_reject_no_pool'");
        });
    }

    Y_UNIT_TEST(TestClassifierMissingBothPoolAndAction) {
        auto ydb = TYdbSetupSettings().Create();

        auto result = ydb->ExecuteQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL CLASSIFIER cl_incomplete WITH (
                RANK=100
            );
        )", TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID));
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(), "Missing required property resource_pool");
    }

    Y_UNIT_TEST(TestActionRejectInvalidValue) {
        auto ydb = TYdbSetupSettings().Create();

        auto result = ydb->ExecuteQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL CLASSIFIER cl_bad_action WITH (
                RESOURCE_POOL=")" << NResourcePool::DEFAULT_POOL_ID << R"(",
                ACTION="not_a_real_action"
            );
        )", TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID));
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST(TestCreateActionRejectWithResourcePoolConflict) {
        auto ydb = TYdbSetupSettings().Create();

        auto result = ydb->ExecuteQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL CLASSIFIER cl_reject_conflict WITH (
                RESOURCE_POOL=")" << NResourcePool::DEFAULT_POOL_ID << R"(",
                ACTION="reject",
                RANK=100
            );
        )", TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID));
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(),
            "Property resource_pool must not be set when action='reject'");
    }

    Y_UNIT_TEST(TestAlterActionRejectWithResourcePoolConflict) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& userSID = "test@user";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
            CREATE RESOURCE POOL CLASSIFIER cl_alter_conflict WITH (
                RESOURCE_POOL=")" << NResourcePool::DEFAULT_POOL_ID << R"(",
                MEMBER_NAME=")" << userSID << R"(",
                RANK=100
            );
        )");

        auto result = ydb->ExecuteQuery(TStringBuilder() << R"(
            ALTER RESOURCE POOL CLASSIFIER cl_alter_conflict SET (
                ACTION="reject",
                RESOURCE_POOL=")" << NResourcePool::DEFAULT_POOL_ID << R"("
            );
        )", TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID));
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToOneLineString(),
            "Property resource_pool must not be set when action='reject'");
    }

    Y_UNIT_TEST(TestAlterSetActionRejectAlone) {
        auto ydb = TYdbSetupSettings().Create();

        const TString& userSID = "test@user";
        const TString& poolId = "prior_pool";
        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=10
            );
            CREATE RESOURCE POOL CLASSIFIER cl_alter_to_reject WITH (
                RESOURCE_POOL=")" << poolId << R"(",
                MEMBER_NAME=")" << userSID << R"(",
                RANK=100
            );
            ALTER RESOURCE POOL CLASSIFIER cl_alter_to_reject SET (
                ACTION="reject"
            );
        )");

        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        ydb->WaitFor(TDuration::Seconds(10), "Resource pool classifier reject (via alter)", [ydb, settings](TString& errorString) {
            auto result = ydb->ExecuteQuery(TSampleQueries::TSelect42::Query, settings);
            errorString = result.GetIssues().ToOneLineString();
            return result.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED
                && errorString.Contains("Request is rejected by classifier 'cl_alter_to_reject'");
        });

        // Verify the ALTER also reset resource_pool to "default" in the stored config.
        auto sysview = ydb->ExecuteQuery(R"(
            SELECT ResourcePool, Action FROM `.sys/resource_pool_classifiers`
            WHERE Name = "cl_alter_to_reject"
        )", TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID));
        UNIT_ASSERT_VALUES_EQUAL(sysview.GetStatus(), EStatus::SUCCESS);
        NYdb::TResultSetParser row(sysview.GetResultSet(0));
        UNIT_ASSERT(row.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(*row.ColumnParser("ResourcePool").GetOptionalUtf8(), NResourcePool::DEFAULT_POOL_ID);
        UNIT_ASSERT_VALUES_EQUAL(*row.ColumnParser("Action").GetOptionalUtf8(), "reject");
    }
}

}  // namespace NKikimr::NKqp
