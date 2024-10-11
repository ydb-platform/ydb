#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/partition_scale_manager.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_query/tx.h>

#include <util/stream/output.h>

namespace NKikimr::NReplication {

Y_UNIT_TEST_SUITE(Replication) {

    using namespace NYdb::NTopic;
    using namespace NYdb::NTopic::NTests;
    using namespace NSchemeShardUT_Private;
    using namespace NKikimr::NPQ::NTest;
    using namespace NYdb;
    using namespace NYdb::NQuery;

    void ExecuteQuery(NYdb::NQuery::TQueryClient& client, const TString& query) {
        auto executeScrptsResult = client.ExecuteScript(query).ExtractValueSync();
        auto scriptStatus = executeScrptsResult.Status().GetStatus();

        UNIT_ASSERT_C(scriptStatus == NYdb::EStatus::SUCCESS, executeScrptsResult.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(SplitTopic) {
        TTopicSdkTestSetup setup = CreateSetup();

        auto driver = setup.MakeDriver();
        NYdb::NQuery::TQueryClient client(driver);

        ExecuteQuery(client, R"(
            CREATE TABLE `OriginalTable` (
                id UINT64,
                value TEXT,
                PRIMARY KEY (id)
            )
        )");

        ExecuteQuery(client, R"(
            CREATE TABLE `TargetTable` (
                id UINT64,
                value TEXT,
                PRIMARY KEY (id)
            )
        )");

        ExecuteQuery(client, R"(
            CREATE ASYNC REPLICATION /RootTestReplication
            FOR /Root/OriginalTable AS /Root/TargetTable
            WITH (
                    CONNECTION_STRING = "grpc://localhost:2135/?database=/Root",
                    ENDPOINT = "localhost:2135",
                    DATABASE = "/Root"
            );
        )");

        ExecuteQuery(client, R"(
            INSERT INTO `OriginalTable` (id, value)
            VALUES
                (1, "value 1"),
                (2, "value 2");
        )");

        Sleep(TDuration::Seconds(1));

        //ui64 txId = 1006;
        //SplitPartition(setup, ++txId, 0, "a");

        ExecuteQuery(client, R"(
            UPSERT INTO `OriginalTable` (id, value)
            VALUES
                (1, "value 3"),
                (2, "value 4");
        )");

        {
            auto queryResult = client.ExecuteQuery("SELECT `id`, `value` FROM `/Root/OriginalTable`",
                TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(queryResult.GetStatus(), EStatus::SUCCESS, queryResult.GetIssues().ToString());

            TResultSetParser resultSet(queryResult.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
        }

        Sleep(TDuration::Seconds(10));

        {
            auto queryResult = client.ExecuteQuery("SELECT `id`, `value` FROM `/Root/TargetTable`",
                TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(queryResult.GetStatus(), EStatus::SUCCESS, queryResult.GetIssues().ToString());

            TResultSetParser resultSet(queryResult.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
        }
    }
}

} // namespace NKikimr::NReplication
