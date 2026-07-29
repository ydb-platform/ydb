#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>

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

    // Compaction / retries may temporarily inflate reported row counts via inactive portions.
    // Poll DescribeTable until rows (and optionally size) match, or fail on timeout.
    TTableDescription WaitForTableStatistics(NYdb::NTable::TSession& session, const TString& path, ui64 expectedRows,
        const std::optional<ui64>& expectedSize = std::nullopt, TDuration timeout = TDuration::Seconds(60)) {
        const auto settings = TDescribeTableSettings().WithTableStatistics(true);
        const TInstant deadline = TInstant::Now() + timeout;
        TString lastState;
        while (TInstant::Now() < deadline) {
            auto describeResult = session.DescribeTable(path, settings).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
            const auto description = describeResult.GetTableDescription();
            const ui64 rows = description.GetTableRows();
            const ui64 size = description.GetTableSize();
            lastState = TStringBuilder() << "path=" << path << " rows=" << rows << " (expected " << expectedRows << ")"
                                         << " size=" << size
                                         << (expectedSize ? TStringBuilder() << " (expected " << *expectedSize << ")" : TStringBuilder());
            if (rows == expectedRows && (!expectedSize || size == *expectedSize)) {
                return description;
            }
            Sleep(TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT_C(false, "Timeout waiting for table statistics: " << lastState);
        Y_ABORT("unreachable");
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

        WaitForTableStatistics(
            testHelper.GetSession(), "/Root/ColumnTableTest", inserted_rows, size_single_table);
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

        WaitForTableStatistics(
            testHelper.GetSession(), "/Root/TableStoreTest/ColumnTableTest", inserted_rows, size_single_table);
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

        for (size_t t = 0; t < tables_in_store; t++) {
            WaitForTableStatistics(testHelper.GetSession(),
                "/Root/TableStoreTest/ColumnTableTest_" + std::to_string(t), t + inserted_rows);
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

        const auto storeDescription = WaitForTableStatistics(testHelper.GetSession(), "/Root/TableStoreTest/", 2000);

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
