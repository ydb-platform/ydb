#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/library/uuid/uuid.h>
#include <ydb/library/binary_json/write.h>

#include <library/cpp/threading/local_executor/local_executor.h>

#include <util/generic/serialized_enum.h>
#include <util/string/printf.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpDatetime64ColumnShard) {

    Y_UNIT_TEST(UseTime64Columns) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("interval").SetType(NScheme::NTypeIds::Interval64),
            TTestHelper::TColumnSchema().SetName("timestamp").SetType(NScheme::NTypeIds::Timestamp64)
        };

        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();
        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(123).Add(-456);
            tableInserter.AddRow().Add(2).Add(-789).AddNull();
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;[123];[-456]]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=2", "[[2;[-789];#]]");
    }

    Y_UNIT_TEST(UseTimestamp64AsPrimaryKey) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("timestamp").SetType(NScheme::NTypeIds::Timestamp64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("interval").SetType(NScheme::NTypeIds::Interval64)
        };

        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();
        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"timestamp"}).SetSharding({"timestamp"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(-7000000).Add(3000001);
            tableInserter.AddRow().Add(-3000000).Add(-1000002);
            tableInserter.AddRow().Add(0).AddNull();
            tableInserter.AddRow().Add(4000000).Add(-2000003);
            tableInserter.AddRow().Add(9000000).Add(5000004);
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `timestamp` = Timestamp64('1970-01-01T00:00:00Z')", "[[#;0]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `timestamp` < Timestamp64('1970-01-01T00:00:00Z')",
            "[[[3000001];-7000000];[[-1000002];-3000000]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `timestamp` > Timestamp64('1970-01-01T00:00:00Z')",
            "[[[-2000003];4000000];[[5000004];9000000]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `timestamp` <= Timestamp64('1970-01-01T00:00:00Z')",
            "[[[3000001];-7000000];[[-1000002];-3000000];[#;0]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `timestamp` >= Timestamp64('1970-01-01T00:00:00Z')",
            "[[#;0];[[-2000003];4000000];[[5000004];9000000]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `timestamp` != Timestamp64('1970-01-01T00:00:00Z')",
            "[[[3000001];-7000000];[[-1000002];-3000000];[[-2000003];4000000];[[5000004];9000000]]");
    }

    Y_UNIT_TEST(UseDatetime64AsPrimaryKey) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("datetime").SetType(NScheme::NTypeIds::Datetime64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("interval").SetType(NScheme::NTypeIds::Interval64)
        };

        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();
        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"datetime"}).SetSharding({"datetime"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(-7).Add(3000001);
            tableInserter.AddRow().Add(-3).Add(-1000002);
            tableInserter.AddRow().Add(0).AddNull();
            tableInserter.AddRow().Add(4).Add(-2000003);
            tableInserter.AddRow().Add(9).Add(5000004);
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `datetime` = Datetime64('1970-01-01T00:00:00Z')", "[[0;#]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `datetime` < Datetime64('1970-01-01T00:00:00Z')",
            "[[-7;[3000001]];[-3;[-1000002]]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `datetime` > Datetime64('1970-01-01T00:00:00Z')",
            "[[4;[-2000003]];[9;[5000004]]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `datetime` <= Datetime64('1970-01-01T00:00:00Z')",
            "[[-7;[3000001]];[-3;[-1000002]];[0;#]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `datetime` >= Datetime64('1970-01-01T00:00:00Z')",
            "[[0;#];[4;[-2000003]];[9;[5000004]]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `datetime` != Datetime64('1970-01-01T00:00:00Z')",
            "[[-7;[3000001]];[-3;[-1000002]];[4;[-2000003]];[9;[5000004]]]");
    }

    Y_UNIT_TEST(UseDate32AsPrimaryKey) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("date").SetType(NScheme::NTypeIds::Date32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("interval").SetType(NScheme::NTypeIds::Interval64)
        };

        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();
        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"date"}).SetSharding({"date"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(-7).Add(3000001);
            tableInserter.AddRow().Add(-3).Add(-1000002);
            tableInserter.AddRow().Add(0).AddNull();
            tableInserter.AddRow().Add(4).Add(-2000003);
            tableInserter.AddRow().Add(9).Add(5000004);
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `date` = Date32('1969-12-29')", "[[-3;[-1000002]]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `date` < Date32('1970-01-03')", "[[-7;[3000001]];[-3;[-1000002]];[0;#]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `date` > Date32('1969-12-30')", "[[0;#];[4;[-2000003]];[9;[5000004]]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `date` <= Date32('1970-01-05')", "[[-7;[3000001]];[-3;[-1000002]];[0;#];[4;[-2000003]]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `date` >= Date32('1969-12-29')", "[[-3;[-1000002]];[0;#];[4;[-2000003]];[9;[5000004]]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `date` != Date32('1970-01-01')", "[[-7;[3000001]];[-3;[-1000002]];[4;[-2000003]];[9;[5000004]]]");
    }

    Y_UNIT_TEST(Csv) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);
        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).SetComponents({ NKikimrServices::GROUPED_MEMORY_LIMITER }, "CS").Initialize();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("date32").SetType(NScheme::NTypeIds::Date32),
            TTestHelper::TColumnSchema().SetName("timestamp64").SetType(NScheme::NTypeIds::Timestamp64),
            TTestHelper::TColumnSchema().SetName("datetime64").SetType(NScheme::NTypeIds::Datetime64),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);
        {
            TStringBuilder builder;
            builder << "1,2000-01-05,2000-01-15T01:15:12.122Z,2005-01-15T01:15:12Z" << Endl;
            const auto result = testHelper.GetKikimr().GetTableClient().BulkUpsert(testTable.GetName(), EDataFormat::CSV, builder).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess() , result.GetIssues().ToString());
        }
        testHelper.ReadData("SELECT id, date32, timestamp64, datetime64 FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;[10961];[947898912122000];[1105751712]]]");
    }

}

} // namespace NKqp
} // namespace NKikimr
