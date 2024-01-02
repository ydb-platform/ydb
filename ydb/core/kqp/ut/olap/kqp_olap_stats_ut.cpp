#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/testlib/common_helper.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpOlapStats) {
    constexpr size_t inserted_rows = 1000;
    constexpr size_t tables_in_store = 1000;
    constexpr size_t size_single_table = 13152;

    const TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };

    class TOlapStatsController: public NYDBTest::NColumnShard::TController {
    public:
        TDuration GetPeriodicWakeupActivationPeriod(const TDuration /*defaultValue*/) const override {
            return TDuration::MilliSeconds(10);
        }
        TDuration GetStatsReportInterval(const TDuration /*defaultValue*/) const override {
            return TDuration::MilliSeconds(10);
        }
    };

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
            
            for(size_t i=0; i<inserted_rows; i++) {
                tableInserter.AddRow().Add(i).Add("test_res_" + std::to_string(i)).AddNull();
            }

            testHelper.InsertData(testTable, tableInserter);
        }

        Sleep(TDuration::Seconds(1));

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
        testTable.SetName("/Root/TableStoreTest/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            for(size_t i=0; i<inserted_rows; i++) {
                tableInserter.AddRow().Add(i).Add("test_res_" + std::to_string(i)).AddNull();
            }
            testHelper.InsertData(testTable, tableInserter);
        }
        
        Sleep(TDuration::Seconds(1));

        auto settings = TDescribeTableSettings().WithTableStatistics(true);
        auto describeResult = testHelper.GetSession().DescribeTable("/Root/TableStoreTest/ColumnTableTest", settings).GetValueSync();

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

        for(size_t t=0; t<tables_in_store; t++) {
            TTestHelper::TColumnTable testTable;
            testTable.SetName("/Root/TableStoreTest/ColumnTableTest_" + std::to_string(t)).SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
            testHelper.CreateTable(testTable);

            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            for(size_t i=0; i < t+ inserted_rows; i++) {
                tableInserter.AddRow().Add(i + t * tables_in_store).Add("test_res_" + std::to_string(i + t * tables_in_store)).AddNull();
            }
            testHelper.InsertData(testTable, tableInserter);;
        }
        
        Sleep(TDuration::Seconds(20));

        auto settings = TDescribeTableSettings().WithTableStatistics(true);
        for(size_t t=0; t<tables_in_store; t++) {
            auto describeResult = testHelper.GetSession().DescribeTable("/Root/TableStoreTest/ColumnTableTest_" + std::to_string(t), settings).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
            const auto& description = describeResult.GetTableDescription();

            UNIT_ASSERT_VALUES_EQUAL(t + inserted_rows, description.GetTableRows());
        }
    }
}

} // namespace NKqp
} // namespace NKikimr