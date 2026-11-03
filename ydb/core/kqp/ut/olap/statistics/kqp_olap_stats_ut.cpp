#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>

#include <set>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpOlapStats) {
    constexpr size_t inserted_rows = 1000;
    constexpr size_t tables_in_store = 1000;
    constexpr size_t size_single_table = 12688;

    const TVector<TTestHelper::TColumnSchema> schema = {
        TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
        TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
        TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)};

    class TOlapStatsController : public NYDBTest::NColumnShard::TController {
    public:
        TDuration DoGetPeriodicWakeupActivationPeriod(const TDuration /*defaultValue*/) const override {
            return TDuration::MilliSeconds(10);
        }
        TDuration DoGetStatsReportInterval(const TDuration /*defaultValue*/) const override {
            return TDuration::MilliSeconds(10);
        }
    };

    Y_UNIT_TEST(GenericQueryStepStatsModesAndEmptyResult) {
        TTestHelper helper(TKikimrSettings().SetWithSampleTables(false));
        TTestHelper::TColumnTable table;
        table.SetName("/Root/StepStats").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        helper.CreateTable(table);
        TTestHelper::TUpdatesBuilder updates(table.GetArrowSchema(schema));
        for (i32 i = 0; i < 64; ++i) {
            updates.AddRow().Add(i).Add("value").Add(i);
        }
        helper.BulkUpsert(table, updates);
        auto client = helper.GetKikimr().GetQueryClient(NYdb::NQuery::TClientSettings().AuthToken("root@builtin"));
        for (const auto mode : {NYdb::NQuery::EStatsMode::Full, NYdb::NQuery::EStatsMode::Profile,
                                NYdb::NQuery::EStatsMode::None, NYdb::NQuery::EStatsMode::Basic}) {
            for (const bool empty : {false, true}) {
                const TString query = empty
                                          ? "SELECT id FROM `/Root/StepStats` WHERE LENGTH(resource_id) > 10000"
                                          : "SELECT id FROM `/Root/StepStats`";
                auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                                                  NYdb::NQuery::TExecuteQuerySettings().StatsMode(mode))
                                  .ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                ui64 rows = 0;
                for (const auto& resultSet : result.GetResultSets()) {
                    rows += resultSet.RowsCount();
                }
                UNIT_ASSERT_VALUES_EQUAL(rows, empty ? 0 : 64);
                NJson::TJsonValue plan;
                if (result.GetStats()) {
                    if (const auto text = result.GetStats()->GetPlan()) {
                        NJson::ReadJsonTree(TString(text->data(), text->size()), &plan, true);
                    }
                }
                const auto scans = FindPlanNodes(plan, "ColumnShardScans");
                const bool detailed = mode == NYdb::NQuery::EStatsMode::Full || mode == NYdb::NQuery::EStatsMode::Profile;
                if (!detailed) {
                    UNIT_ASSERT(scans.empty());
                    continue;
                }
                UNIT_ASSERT_C(!scans.empty(), plan);
                ui64 records = 0;
                bool sawSelection = false;
                for (const auto& stageScans : scans) {
                    for (const auto& scan : stageScans.GetArraySafe()) {
                        if (scan["ExpectedContextOnly"].GetBooleanSafe()) {
                            UNIT_ASSERT(scan["TabletId"].GetStringSafe().empty());
                            UNIT_ASSERT_VALUES_EQUAL(scan["ParticipatingTablets"].GetArraySafe().size(),
                                scan["ParticipatingShards"].GetUIntegerSafe());
                            continue;
                        }
                        UNIT_ASSERT(!scan["StatsScanId"].GetStringSafe().empty());
                        UNIT_ASSERT(!scan["TabletId"].GetStringSafe().empty());
                        UNIT_ASSERT(!scan["ReaderKind"].GetStringSafe().empty());
                        UNIT_ASSERT_C(scan["Finished"].GetBooleanSafe(), scan);
                        UNIT_ASSERT(!scan["Truncated"].GetBooleanSafe());
                        std::set<ui64> ids;
                        for (const auto& step : scan["Steps"].GetArraySafe()) {
                            const ui64 id = step["StepId"].GetUIntegerSafe();
                            UNIT_ASSERT(id);
                            UNIT_ASSERT(ids.emplace(id).second);
                            if (step["StepKind"].GetStringSafe() == "SelectPortions") {
                                const auto& details = step["Details"].GetMapSafe();
                                UNIT_ASSERT(details.contains("SelectedPortions"));
                                UNIT_ASSERT(details.contains("SelectedVersionRows"));
                                UNIT_ASSERT(details.contains("SelectedBlobBytes"));
                                sawSelection = true;
                            }
                            ++records;
                        }
                    }
                }
                UNIT_ASSERT_C(records, plan);
                UNIT_ASSERT_C(sawSelection, plan);
            }
        }
    }

    Y_UNIT_TEST(AddRowsTableStandalone) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<TOlapStatsController>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);

        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));

            for (size_t i = 0; i < inserted_rows; i++) {
                tableInserter.AddRow().Add(i).Add("test_res_" + std::to_string(i)).AddNull();
            }

            testHelper.BulkUpsert(testTable, tableInserter);
        }

        Sleep(TDuration::Seconds(10));

        auto settings = TDescribeTableSettings().WithTableStatistics(true);
        auto describeResult = testHelper.GetSession().DescribeTable("/Root/ColumnTableTest", settings).GetValueSync();

        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

        const auto& description = describeResult.GetTableDescription();

        UNIT_ASSERT_VALUES_EQUAL(inserted_rows, description.GetTableRows());
        UNIT_ASSERT_VALUES_EQUAL(size_single_table, description.GetTableSize());
    }

    Y_UNIT_TEST(AddRowsTableInTableStore) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<TOlapStatsController>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);

        TTestHelper::TColumnTableStore testTableStore;

        testTableStore.SetName("/Root/TableStoreTest").SetPrimaryKey({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTableStore);
        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/TableStoreTest/ColumnTableTest")
            .SetPrimaryKey({"id"})
            .SetSharding({"id"})
            .SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            for (size_t i = 0; i < inserted_rows; i++) {
                tableInserter.AddRow().Add(i).Add("test_res_" + std::to_string(i)).AddNull();
            }
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        Sleep(TDuration::Seconds(10));

        auto settings = TDescribeTableSettings().WithTableStatistics(true);
        auto describeResult =
            testHelper.GetSession().DescribeTable("/Root/TableStoreTest/ColumnTableTest", settings).GetValueSync();

        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

        const auto& description = describeResult.GetTableDescription();

        UNIT_ASSERT_VALUES_EQUAL(inserted_rows, description.GetTableRows());
        UNIT_ASSERT_VALUES_EQUAL(size_single_table, description.GetTableSize());
    }

    Y_UNIT_TEST(AddRowsSomeTablesInTableStore) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<TOlapStatsController>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);

        TTestHelper::TColumnTableStore testTableStore;

        testTableStore.SetName("/Root/TableStoreTest").SetPrimaryKey({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTableStore);

        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).SetPriority(NActors::NLog::PRI_DEBUG).Initialize();

        for (size_t t = 0; t < tables_in_store; t++) {
            TTestHelper::TColumnTable testTable;
            testTable.SetName("/Root/TableStoreTest/ColumnTableTest_" + std::to_string(t))
                .SetPrimaryKey({"id"})
                .SetSharding({"id"})
                .SetSchema(schema);
            testHelper.CreateTable(testTable);

            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            for (size_t i = 0; i < t + inserted_rows; i++) {
                tableInserter.AddRow()
                    .Add(i + t * tables_in_store)
                    .Add("test_res_" + std::to_string(i + t * tables_in_store))
                    .AddNull();
            }
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        Sleep(TDuration::Seconds(20));

        auto settings = TDescribeTableSettings().WithTableStatistics(true);
        for (size_t t = 0; t < tables_in_store; t++) {
            auto describeResult =
                testHelper.GetSession()
                    .DescribeTable("/Root/TableStoreTest/ColumnTableTest_" + std::to_string(t), settings)
                    .GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
            const auto& description = describeResult.GetTableDescription();

            UNIT_ASSERT_VALUES_EQUAL(t + inserted_rows, description.GetTableRows());
        }
    }

    Y_UNIT_TEST(DescibeTableStore) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<TOlapStatsController>();

        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);

        TTestHelper::TColumnTableStore testTableStore;

        testTableStore.SetName("/Root/TableStoreTest").SetPrimaryKey({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTableStore);

        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).SetPriority(NActors::NLog::PRI_DEBUG).Initialize();

        for (size_t t = 0; t < 2; t++) {
            TTestHelper::TColumnTable testTable;
            testTable.SetName("/Root/TableStoreTest/ColumnTableTest_" + std::to_string(t))
                .SetPrimaryKey({"id"})
                .SetSharding({"id"})
                .SetSchema(schema);
            testHelper.CreateTable(testTable);

            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            for (size_t i = 0; i < inserted_rows; i++) {
                tableInserter.AddRow()
                    .Add(i + t * tables_in_store)
                    .Add("test_res_" + std::to_string(i + t * tables_in_store))
                    .AddNull();
            }
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        Sleep(TDuration::Seconds(10));

        auto settings = TDescribeTableSettings().WithTableStatistics(true);

        auto describeStoreResult =
            testHelper.GetSession().DescribeTable("/Root/TableStoreTest/", settings).GetValueSync();
        UNIT_ASSERT_C(describeStoreResult.IsSuccess(), describeStoreResult.GetIssues().ToString());
        const auto& storeDescription = describeStoreResult.GetTableDescription();

        UNIT_ASSERT_VALUES_EQUAL(2000, storeDescription.GetTableRows());

        {
            auto selectQuery = TString(R"(
                SELECT
                    SUM(ColumnBlobBytes) AS BlobBytes,
                    SUM(ColumnRawBytes) AS RawBytes,
                    SUM(Rows) AS Rows,
                    COUNT(*) AS Portions
                FROM `/Root/TableStoreTest/.sys/store_primary_index_portion_stats`
            )");

            auto client = testHelper.GetKikimr().GetTableClient();
            auto rows = ExecuteScanQuery(client, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("Rows")), storeDescription.GetTableRows());
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("BlobBytes")), storeDescription.GetTableSize());
        }
    }
}

}   // namespace NKqp
}   // namespace NKikimr
