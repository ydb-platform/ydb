#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/testlib/common_helper.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpOlapStats) {
    class TPeriodicWakeupActivationPeriodController: public NKikimr::NYDBTest::ICSController {
    public:
        virtual TDuration GetGuaranteeIndexationInterval(const TDuration /*defaultValue*/) const override {
            return TDuration::MilliSeconds(10);
        }
    };

    Y_UNIT_TEST(AddRowToSingleTable) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };

        Cerr << "\n=========== Start create a table ============\n" << Endl;

        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<TPeriodicWakeupActivationPeriodController>();
        
        {
            Cerr << "\n=========== Start insert to table ============\n" << Endl;

            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add("test_res_1").AddNull();

            testHelper.InsertData(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;#;[\"test_res_1\"]]]");

        Sleep(TDuration::Seconds(10));

        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).SetPriority(NActors::NLog::PRI_TRACE).Initialize();
        {
            Cerr << "\n=========== Start describe the table ============\n" << Endl;

            auto settings = TDescribeTableSettings().WithTableStatistics(true);
            auto describeResult = testHelper.GetSession().DescribeTable("/Root/ColumnTableTest", settings).GetValueSync();

            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            const auto& description = describeResult.GetTableDescription();

            // UNIT_ASSERT_VALUES_EQUAL(0, description.GetTableSize());
            UNIT_ASSERT_VALUES_EQUAL(1, description.GetTableRows());
        }
    }

    Y_UNIT_TEST(AddRowToTableStore) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };

        Cerr << "\n=========== Start create a table ============\n" << Endl;
        
        TTestHelper::TColumnTableStore testTableStore;

        testTableStore.SetName("/Root/TableStoreTest").SetPrimaryKey({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTableStore);
        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/TableStoreTest/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<TPeriodicWakeupActivationPeriodController>();

        {
            Cerr << "\n=========== Start insert to table ============\n" << Endl;

            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add("test_res_1").AddNull();
            testHelper.InsertData(testTable, tableInserter);
        }

        
        testHelper.ReadData("SELECT * FROM `/Root/TableStoreTest/ColumnTableTest` WHERE id=1", "[[1;#;[\"test_res_1\"]]]");
        Sleep(TDuration::Seconds(10));

        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).SetPriority(NActors::NLog::PRI_TRACE).Initialize();
        {
            Cerr << "\n=========== Start describe the table ============\n" << Endl;

            auto settings = TDescribeTableSettings().WithTableStatistics(true);
            auto describeResult = testHelper.GetSession().DescribeTable("/Root/TableStoreTest/ColumnTableTest", settings).GetValueSync();

            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            const auto& description = describeResult.GetTableDescription();

            // UNIT_ASSERT_VALUES_EQUAL(0, description.GetTableSize());
            UNIT_ASSERT_VALUES_EQUAL(1, description.GetTableRows());
        }
    }
}

} // namespace NKqp
} // namespace NKikimr