#include <ydb/core/kqp/ut/common/columnshard.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
Y_UNIT_TEST_SUITE(KqpOlapDelete) {
    Y_UNIT_TEST_TWIN(DeleteWithDiffrentTypesPKColumns, isStream) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto runnerSettings = TKikimrSettings().SetAppConfig(appConfig).SetWithSampleTables(true);

        TTestHelper testHelper(runnerSettings);
        auto client = testHelper.GetKikimr().GetQueryClient();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("time").SetType(NScheme::NTypeIds::Timestamp).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("class").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("uniq").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "time", "class", "uniq" }).SetSchema(schema);
        testHelper.CreateTable(testTable);

        auto ts = TInstant::Now();
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(ts.MicroSeconds()).Add("test").Add("test");
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        
        if (isStream) {
            auto deleteQuery = "DELETE FROM `/Root/ColumnTableTest` ON SELECT * FROM `/Root/ColumnTableTest`";
            auto deleteQueryResult = client.ExecuteQuery(deleteQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(deleteQueryResult.IsSuccess(), deleteQueryResult.GetIssues().ToString());
        } else {
            auto deleteQuery = TStringBuilder() << "DELETE FROM `/Root/ColumnTableTest` WHERE Cast(DateTime::MakeDate(DateTime::StartOfDay(time)) as String) == \""
                             << ts.FormatLocalTime("%Y-%m-%d")
                             << "\" and class == \"test\" and uniq = \"test\";";
            auto deleteQueryResult = client.ExecuteQuery(deleteQuery, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(deleteQueryResult.IsSuccess(), deleteQueryResult.GetIssues().ToString());
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest`", "[]");
    }
}
}