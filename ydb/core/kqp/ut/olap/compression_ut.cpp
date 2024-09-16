#include "helpers/typed_local.h"

#include <ydb/core/kqp/ut/common/columnshard.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpOlapCompression) {
    Y_UNIT_TEST(DisabledAlterCompression) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false).SetEnableOlapCompression(false);
        {
            TKikimrRunner kikimr(settings);
            TTypedLocalHelper helper("", kikimr, "olapTable", "olapStore");
            helper.CreateTestOlapTable();
            helper.FillPKOnly(0, 1);
            helper.ExecuteSchemeQuery(
                "ALTER OBJECT `/Root/olapStore/olapTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=pk_int, "
                "`SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`zstd`);", NYdb::EStatus::SCHEME_ERROR);
            helper.ExecuteSchemeQuery(
                "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=pk_int, "
                "`SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`zstd`);", NYdb::EStatus::PRECONDITION_FAILED);
        }

        {
            TTestHelper helper(settings);
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("pk_int").SetType(NScheme::NTypeIds::Uint64).SetNullable(false)
            };

            TTestHelper::TColumnTable table;
            table.SetName("/Root/olapTable").SetPrimaryKey({ "pk_int" }).SetSharding({ "pk_int" }).SetSchema(schema);
            helper.CreateTable(table);

            auto alterResult = helper.GetSession()
                                   .ExecuteSchemeQuery(
                                       "ALTER OBJECT `/Root/olapTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, "
                                       "NAME=pk_int,`SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`zstd`);")
                                   .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SCHEME_ERROR, alterResult.GetIssues().ToString());
        }
    }
}
}
