#include "helpers/local.h"

#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/kqp/ut/common/columnshard.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapEraseSchema) {
    class TEraseSchemaTest {
    private:
        TKikimrSettings Settings = TKikimrSettings()
            .SetColumnShardAlterObjectEnabled(true)
            .SetWithSampleTables(false);
        NKikimr::NYDBTest::TControllers::TGuard<NKikimr::NYDBTest::NColumnShard::TController> CSController;
        const TString StoreName;
    public:
        TEraseSchemaTest()
            : CSController(NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NYDBTest::NColumnShard::TController>()) {
        }

        void Test(bool insertInFirst = false, bool insertInLast = false, const THashSet<ui64>& removedVersions = {1, 2, 3, 4}, const TString& expectedContent = "[]") {
            NKikimrConfig::TAppConfig appConfig;
            appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
            Settings.SetAppConfig(appConfig);
            TTestHelper helper(Settings);
//            TKikimrRunner kikimr(Settings);
            TKikimrRunner& kikimr = helper.GetKikimr();
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
            CSController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(15));
            auto tableClient = kikimr.GetTableClient();
            auto session = tableClient.CreateSession().GetValueSync().GetSession();

            auto query = TStringBuilder() << R"(
                --!syntax_v1
                CREATE TABLE `/Root/test_table`
                (
                    key Int64 NOT NULL,
                    val Utf8 NOT NULL,
                    PRIMARY KEY (key)
                )
                PARTITION BY HASH(key)
                WITH (
                    STORE = COLUMN,
                    PARTITION_COUNT =)" << 1
                                      << ")";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            if (insertInFirst) {
                auto query = "REPLACE INTO `/Root/test_table`(key, val) VALUES (1, '1')";
                auto insertResult = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), NYdb::EStatus::SUCCESS, insertResult.GetIssues().ToString());
            }

            auto alterQueryAddTemp = TStringBuilder() << "ALTER TABLE `/Root/test_table` ADD COLUMN temp Utf8;";
            auto alterResultAddTemp = session.ExecuteSchemeQuery(alterQueryAddTemp).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResultAddTemp.GetStatus(), NYdb::EStatus::SUCCESS, alterResultAddTemp.GetIssues().ToString());

            auto alterQuery = TStringBuilder() << "ALTER TABLE `/Root/test_table` ADD COLUMN data Utf8;";
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());

            auto alterQuery1 = TStringBuilder() << "ALTER TABLE `/Root/test_table` ADD COLUMN data2 Utf8;";
            alterResult = session.ExecuteSchemeQuery(alterQuery1).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());

            auto alterQueryDropTemp = TStringBuilder() << "ALTER TABLE `/Root/test_table` DROP COLUMN temp;";
            auto alterResultDropTemp = session.ExecuteSchemeQuery(alterQueryDropTemp).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResultDropTemp.GetStatus(), NYdb::EStatus::SUCCESS, alterResultDropTemp.GetIssues().ToString());

            if (insertInLast) {
                auto query1 = "REPLACE INTO `/Root/test_table`(key, val, data, data2) VALUES (2, '2', 'd', 'd2')";
                auto insertResult1 = session.ExecuteDataQuery(query1, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(insertResult1.GetStatus(), NYdb::EStatus::SUCCESS, insertResult1.GetIssues().ToString());
            }

            helper.RebootTablets("/Root/test_table");

            UNIT_ASSERT_VALUES_EQUAL_C(CSController->WaitRemovingSchemaVersions(removedVersions, TDuration::Seconds(30)), true, "Schema versions are not removed");

            helper.RebootTablets("/Root/test_table");

            NYdb::NTable::TDescribeTableResult describe = session.DescribeTable("/Root/test_table").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), NYdb::EStatus::SUCCESS);
            NYdb::NTable::TTableDescription tableDescription = describe.GetTableDescription();
            std::vector<NYdb::NTable::TTableColumn> columns = tableDescription.GetTableColumns();
            UNIT_ASSERT_EQUAL_C(columns.size(), 4, "Columns count should be 4");
            UNIT_ASSERT_EQUAL_C(columns[0].Name, "key", "First column should be key");
            UNIT_ASSERT_EQUAL_C(columns[1].Name, "val", "Second column should be val");
            UNIT_ASSERT_EQUAL_C(columns[2].Name, "data", "Third column should be data");
            UNIT_ASSERT_EQUAL_C(columns[3].Name, "data2", "Fourth column should be data2");

            auto it =
                kikimr.GetQueryClient().StreamExecuteQuery("SELECT data, data2, key, val FROM `/Root/test_table` ORDER BY key", NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                    .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            fprintf(stderr, "Output %s\n", output.c_str());
            UNIT_ASSERT_EQUAL(output, expectedContent);
         }
    };

    Y_UNIT_TEST(EraseEmptySchema) {
        TEraseSchemaTest test;
        test.Test();
    }

    Y_UNIT_TEST(EraseSchemaWithFirst) {
        TEraseSchemaTest test;
        test.Test(true, false, {2, 3, 4}, "[[#;#;1;\"1\"]]");
    }

    Y_UNIT_TEST(EraseSchemaWithLast) {
        TEraseSchemaTest test;
        test.Test(false, true, {1, 2, 3, 4}, "[[[\"d\"];[\"d2\"];2;\"2\"]]");
    }

    Y_UNIT_TEST(EraseSchemaWithFirstLast) {
        TEraseSchemaTest test;
        test.Test(true, true, {2, 3, 4}, "[[#;#;1;\"1\"];[[\"d\"];[\"d2\"];2;\"2\"]]");
    }


}
}

