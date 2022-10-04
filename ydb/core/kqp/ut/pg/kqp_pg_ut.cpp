#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
}

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpPg) {

    auto makePgType = [] (ui32 oid, i32 typlen = -1) { return TPgType(oid, typlen, -1); };

    Y_UNIT_TEST_NEW_ENGINE(CreateTableBulkUpsertAndRead) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto builder = TTableBuilder()
            .AddNullableColumn("boolean", makePgType(BOOLOID))
            .AddNullableColumn("char", makePgType(CHAROID))
            .AddNullableColumn("int2", makePgType(INT2OID))
            .AddNullableColumn("int4", makePgType(INT4OID))
            .AddNullableColumn("int8", makePgType(INT8OID))
            .AddNullableColumn("float4", makePgType(FLOAT4OID))
            .AddNullableColumn("float8", makePgType(FLOAT8OID))
            .AddNullableColumn("text", makePgType(TEXTOID))
            .AddNullableColumn("bytea", makePgType(BYTEAOID))
            .AddNullableColumn("bpchar", makePgType(BPCHAROID))
            .AddNullableColumn("varchar", makePgType(VARCHAROID))
            .SetPrimaryKeyColumn("int2")
            .SetPrimaryKeyColumn("int4")
            .SetPrimaryKeyColumn("int8")
            .SetPrimaryKeyColumn("float4")
            .SetPrimaryKeyColumn("float8")
            .SetPrimaryKeyColumn("text")
            .SetPrimaryKeyColumn("bytea")
            .SetPrimaryKeyColumn("bpchar");

        auto result = session.CreateTable("/Root/Pg", builder.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 10; ++i) {
            auto boolVal = true;
            TString boolStr((const char*)&boolVal, sizeof(boolVal));
            auto charVal = (char)i;
            TString charStr((const char*)&charVal, sizeof(charVal));
            auto int2Val = (i16)i;
            TString int2Str((const char*)&int2Val, sizeof(int2Val));
            auto int4Val = (i32)i;
            TString int4Str((const char*)&int4Val, sizeof(int4Val));
            auto int8Val = (i64)i;
            TString int8Str((const char*)&int8Val, sizeof(int8Val));
            auto float4Val = (float)i;
            TString float4Str((const char*)&float4Val, sizeof(float4Val));
            auto float8Val = (double)i;
            TString float8Str((const char*)&float8Val, sizeof(float8Val));
            TString textStr = Sprintf("text %" PRIu64, i);
            TString byteaStr = Sprintf("bytea %" PRIu64, i);
            TString bpcharStr = Sprintf("bpchar %" PRIu64, i);
            TString varcharStr = Sprintf("varchar %" PRIu64, i);

            rows.AddListItem()
                .BeginStruct()
                .AddMember("boolean").Pg(TPgValue(TPgValue::VK_BINARY, boolStr, makePgType(BOOLOID)))
                .AddMember("char").Pg(TPgValue(TPgValue::VK_BINARY, charStr, makePgType(CHAROID)))
                .AddMember("int2").Pg(TPgValue(TPgValue::VK_BINARY, int2Str, makePgType(INT2OID)))
                .AddMember("int4").Pg(TPgValue(TPgValue::VK_BINARY, int4Str, makePgType(INT4OID)))
                .AddMember("int8").Pg(TPgValue(TPgValue::VK_BINARY, int8Str, makePgType(INT8OID)))
                .AddMember("float4").Pg(TPgValue(TPgValue::VK_BINARY, float4Str, makePgType(FLOAT4OID)))
                .AddMember("float8").Pg(TPgValue(TPgValue::VK_BINARY, float8Str, makePgType(FLOAT8OID)))
                .AddMember("text").Pg(TPgValue(TPgValue::VK_BINARY, textStr, makePgType(TEXTOID)))
                .AddMember("bytea").Pg(TPgValue(TPgValue::VK_BINARY, byteaStr, makePgType(BYTEAOID)))
                .AddMember("bpchar").Pg(TPgValue(TPgValue::VK_BINARY, bpcharStr, makePgType(BPCHAROID)))
                .AddMember("varchar").Pg(TPgValue(TPgValue::VK_BINARY, varcharStr, makePgType(VARCHAROID)))
                .EndStruct();
        }
        rows.EndList();

        result = db.BulkUpsert("/Root/Pg", rows.Build()).GetValueSync();;
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        session.Close().GetValueSync();

        auto readSettings = TReadTableSettings()
            .AppendColumns("boolean")
            .AppendColumns("char")
            .AppendColumns("int2")
            .AppendColumns("int4")
            .AppendColumns("int8")
            .AppendColumns("float4")
            .AppendColumns("float8")
            .AppendColumns("text")
            .AppendColumns("bytea")
            .AppendColumns("bpchar")
            .AppendColumns("varchar");

        auto it = session.ReadTable("/Root/Pg", readSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), result.GetIssues().ToString());

        bool eos = false;
        while (!eos) {
            auto part = it.ReadNext().ExtractValueSync();
            if (!part.IsSuccess()) {
                eos = true;
                UNIT_ASSERT_C(part.EOS(), result.GetIssues().ToString());
                continue;
            }
            auto resultSet = part.ExtractPart();
            TResultSetParser parser(resultSet);
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                auto boolVal = true;
                TString boolStr((const char*)&boolVal, sizeof(boolVal));
                auto charVal = (char)i;
                TString charStr((const char*)&charVal, sizeof(charVal));
                auto int2Val = (i16)i;
                TString int2Str((const char*)&int2Val, sizeof(int2Val));
                auto int4Val = (i32)i;
                TString int4Str((const char*)&int4Val, sizeof(int4Val));
                auto int8Val = (i64)i;
                TString int8Str((const char*)&int8Val, sizeof(int8Val));
                auto float4Val = (float)i;
                TString float4Str((const char*)&float4Val, sizeof(float4Val));
                auto float8Val = (double)i;
                TString float8Str((const char*)&float8Val, sizeof(float8Val));
                TString textStr = Sprintf("text %" PRIu64, i);
                TString byteaStr = Sprintf("bytea %" PRIu64, i);
                TString bpcharStr = Sprintf("bpchar %" PRIu64, i);
                TString varcharStr = Sprintf("varchar %" PRIu64, i);

#define COLUMN(type, valStr) \
                { \
                    auto& column = parser.ColumnParser(type); \
                    column.OpenOptional(); \
                    UNIT_ASSERT_VALUES_EQUAL(valStr, column.GetPg().Content_); \
                    column.CloseOptional(); \
                }
                COLUMN("boolean", boolStr);
                COLUMN("char", charStr);
                COLUMN("int2", int2Str);
                COLUMN("int4", int4Str);
                COLUMN("int8", int8Str);
                COLUMN("float4", float4Str);
                COLUMN("float8", float8Str);
                COLUMN("text", textStr);
                COLUMN("bytea", byteaStr);
                COLUMN("bpchar", bpcharStr);
                COLUMN("varchar", varcharStr);
#undef COLUMN
            }
        }
   }
}

} // namespace NKqp
} // namespace NKikimr
