#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/testlib/common_helper.h>
#include <yql/essentials/types/uuid/uuid.h>

#include <library/cpp/threading/local_executor/local_executor.h>

#include <util/generic/serialized_enum.h>
#include <util/string/printf.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpUuidColumnShard) {

    Y_UNIT_TEST(UseUuidColumns) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        const TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
            TTestHelper::TColumnSchema().SetName("uuid").SetType(NScheme::NTypeIds::Uuid)
        };

        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();
        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(3).Add(TUuidValue("d034f360-423d-4c8c-ba6f-a07607c29c9f"));
            tableInserter.AddRow().Add(2).Add(4).AddNull();
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;[3];[\"d034f360-423d-4c8c-ba6f-a07607c29c9f\"]]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=2", "[[2;[4];#]]");
    }

    Y_UNIT_TEST(UseUuidAsPrimaryKey) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);

        const TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("uuid").SetType(NScheme::NTypeIds::Uuid).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64)
        };

        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).Initialize();
        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"uuid"}).SetSharding({"uuid"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            // UUIDs are sorted by internal binary representation
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(TUuidValue("00000000-0000-0000-0000-000000000000")).AddNull();
            tableInserter.AddRow().Add(TUuidValue("00000000-0000-0000-0000-000000000011")).Add(123);
            tableInserter.AddRow().Add(TUuidValue("00000000-0000-2200-0000-000000000000")).Add(-456);
            tableInserter.AddRow().Add(TUuidValue("00000000-0000-0033-0000-000000000000")).Add(-10);
            tableInserter.AddRow().Add(TUuidValue("00000044-0000-0000-0000-000000000000")).Add(9999);
            testHelper.BulkUpsert(testTable, tableInserter);
        }

        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `uuid` = Uuid('00000000-0000-0000-0000-000000000011')",
             "[[[123];\"00000000-0000-0000-0000-000000000011\"]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `uuid` != Uuid('00000000-0000-2200-0000-000000000000')",
            "[[#;\"00000000-0000-0000-0000-000000000000\"];[[123];\"00000000-0000-0000-0000-000000000011\"];"
            "[[-10];\"00000000-0000-0033-0000-000000000000\"];[[9999];\"00000044-0000-0000-0000-000000000000\"]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `uuid` < Uuid('00000000-0000-2200-0000-000000000000')",
            "[[#;\"00000000-0000-0000-0000-000000000000\"];[[123];\"00000000-0000-0000-0000-000000000011\"]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `uuid` > Uuid('00000000-0000-0033-0000-000000000000')",
            "[[[9999];\"00000044-0000-0000-0000-000000000000\"]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `uuid` <= Uuid('00000000-0000-0000-0000-000000000011')",
            "[[#;\"00000000-0000-0000-0000-000000000000\"];[[123];\"00000000-0000-0000-0000-000000000011\"]]");
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE `uuid` >= Uuid('00000000-0000-0033-0000-000000000000')",
            "[[[-10];\"00000000-0000-0033-0000-000000000000\"];[[9999];\"00000044-0000-0000-0000-000000000000\"]]");
    }

    Y_UNIT_TEST(UuidCsv) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;

        TTestHelper testHelper(runnerSettings);
        Tests::NCommon::TLoggerInit(testHelper.GetKikimr()).SetComponents({ NKikimrServices::GROUPED_MEMORY_LIMITER }, "CS").Initialize();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("uuid").SetType(NScheme::NTypeIds::Uuid)
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);
        {
            TStringBuilder builder;
            builder << "1,3ee41410-16f4-48cd-bc04-29a6cfef46d4" << Endl;
            const auto result = testHelper.GetKikimr().GetTableClient().BulkUpsert(testTable.GetName(), EDataFormat::CSV, builder).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess() , result.GetIssues().ToString());
        }
        testHelper.ReadData("SELECT id, uuid FROM `/Root/ColumnTableTest` WHERE id=1", "[[1;[\"3ee41410-16f4-48cd-bc04-29a6cfef46d4\"]]]");
    }

}

} // namespace NKqp
} // namespace NKikimr
