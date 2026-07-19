#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <thread>

namespace NKikimr::NKqp {

using namespace NWorkload;
using namespace NYdb;


namespace {

TIntrusivePtr<NWorkload::IYdbSetup> MakeStreamingYdb() {
    return NWorkload::TYdbSetupSettings().Create([](auto) {});
}

void CreateTopic(TIntrusivePtr<NWorkload::IYdbSetup> ydb, TString name) {
    const auto& result = ydb->ExecuteQuery(TStringBuilder() << "CREATE TOPIC " << name);
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
}

}  // anonymous namespace


Y_UNIT_TEST_SUITE(StreamingQueryClassification) {
    using namespace std::chrono_literals;
    Y_UNIT_TEST(TestStreamingQueryClassificationByPath) {
        auto ydb = MakeStreamingYdb();

        const TString& poolId = "streaming_pool";
        CreateTopic(ydb, "input_topic");
        CreateTopic(ydb, "output_topic");

        {
            const auto& result = ydb->ExecuteQuery(TStringBuilder() << R"(
                CREATE RESOURCE POOL )" << poolId << R"( WITH (
                    CONCURRENT_QUERY_LIMIT = 10,
                    QUEUE_SIZE = 100,
                    TOTAL_CPU_LIMIT_PERCENT_PER_NODE = 10,
                    QUERY_CPU_LIMIT_PERCENT_PER_NODE = 1
                );
                CREATE RESOURCE POOL CLASSIFIER streaming_classifier WITH (
                    RESOURCE_POOL=")" << poolId << R"(",
                    HAS_PATH = "*input_topic*"
                );
            )");
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }
        ydb->WaitForClassifierPropagation();

        {
            const auto& result = ydb->ExecuteQuery(R"(
                CREATE STREAMING QUERY MyStreamingQuery
                AS DO BEGIN
                    INSERT INTO output_topic SELECT * FROM input_topic;
                END DO
            )");
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            ydb->WaitPoolState({.DelayedRequests = 0, .RunningRequests = 1}, poolId);

        }
        {
            std::this_thread::sleep_for(5s);
            ydb->WaitPoolState({.DelayedRequests = 0, .RunningRequests = 1}, poolId);
        }
    }

}

}  // namespace NKikimr::NKqp
