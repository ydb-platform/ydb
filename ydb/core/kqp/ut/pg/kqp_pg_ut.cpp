#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/yql/parser/pg_catalog/catalog.h>

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

    Y_UNIT_TEST(CreateTableBulkUpsertAndRead) {
        TKikimrRunner kikimr;

        auto testSingleType = [&kikimr] (ui32 id, bool isKey,
            std::function<TString(size_t)> binaryIn,
            std::function<TString(size_t)> textOut)
        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            TTableBuilder builder;
            if (isKey) {
                builder.AddNullableColumn("key", makePgType(id));
            } else {
                builder.AddNullableColumn("key", makePgType(INT2OID));
            }
            builder.AddNullableColumn("value", makePgType(id));
            builder.SetPrimaryKeyColumn("key");

            auto tableName = Sprintf("/Root/Pg%u", id);
            auto result = session.CreateTable(tableName, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (size_t i = 0; i < 10; ++i) {
                auto str = binaryIn(i);
                if (isKey) {
                    rows.AddListItem()
                        .BeginStruct()
                        .AddMember("key").Pg(TPgValue(TPgValue::VK_BINARY, str, makePgType(id)))
                        .AddMember("value").Pg(TPgValue(TPgValue::VK_BINARY, str, makePgType(id)))
                        .EndStruct();
                } else {
                    auto int2Val = (i16)i;
                    TString int2Str((const char*)&int2Val, sizeof(int2Val));
                    rows.AddListItem()
                        .BeginStruct()
                        .AddMember("key").Pg(TPgValue(TPgValue::VK_BINARY, int2Str, makePgType(INT2OID)))
                        .AddMember("value").Pg(TPgValue(TPgValue::VK_BINARY, str, makePgType(id)))
                        .EndStruct();
                }
            }
            rows.EndList();

            result = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto readSettings = TReadTableSettings()
                .AppendColumns("key")
                .AppendColumns("value");

            auto it = session.ReadTable(tableName, readSettings).GetValueSync();
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
                    auto check = [&parser, &id] (const TString& column, const TString& expected) {
                        auto& c = parser.ColumnParser(column);
                        c.OpenOptional();
                        UNIT_ASSERT_VALUES_EQUAL(expected, NPg::PgNativeTextFromNativeBinary(c.GetPg().Content_, id));
                        Cerr << expected << Endl;
                        c.CloseOptional();
                    };
                    auto expected = textOut(i);
                    if (isKey) {
                        check("key", expected);
                    }
                    check("value", expected);
                }
            }

            session.Close().GetValueSync();
        };

        auto testType = [&] (ui32 id, bool isKey,
            std::function<TString(size_t)> binaryIn,
            std::function<TString(size_t)> textOut,
            std::function<TString(TString)> arrayPrint = [] (auto s) { return Sprintf("{%s,%s}", s.c_str(), s.c_str()); })
        {
            testSingleType(id, isKey, binaryIn, textOut);

            auto arrayId = NYql::NPg::LookupType(id).ArrayTypeId;

            auto binaryInArray = [&] (auto i) {
                auto binary = binaryIn(i);
                auto str = NPg::PgNativeTextFromNativeBinary(binary, id);
                auto arrayStr = arrayPrint(str);
                return NPg::PgNativeBinaryFromNativeText(arrayStr, arrayId);
            };
            auto textOutArray = [&] (auto i) {
                auto str = textOut(i);
                return arrayPrint(str);
            };

            testSingleType(arrayId, false, binaryInArray, textOutArray);
        };

        auto testByteaType = [&] () {
            testSingleType(BYTEAOID, true,
                [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("bytea %u", i), BYTEAOID); },
                [] (auto i) { return Sprintf("\\x627974656120%x", i + 48); });

            testSingleType(BYTEAARRAYOID, false,
                [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("{a%u, b%u}", i, i + 10), BYTEAARRAYOID); },
                [] (auto i) { return Sprintf("{\"\\\\x61%x\",\"\\\\x6231%x\"}", i + 48, i + 48); });
        };

        testType(BOOLOID, true,
            [] (auto i) { auto val = (bool)i; return TString((const char*)&val, sizeof(val)); },
            [] (auto i) { return TString(i ? "t" : "f"); });

        testType(CHAROID, true,
            [] (auto i) { auto val = (char)(i + '0'); return TString((const char*)&val, sizeof(val)); },
            [] (auto i) { return Sprintf("%c", (char)(i + '0')); });

        testType(INT2OID, true,
            [] (auto i) { auto val = (i16)i; return TString((const char*)&val, sizeof(val)); },
            [] (auto i) { return Sprintf("%u", i); });

        testType(INT4OID, true,
            [] (auto i) { auto val = (i32)i; return TString((const char*)&val, sizeof(val)); },
            [] (auto i) { return Sprintf("%u", i); });

        testType(INT8OID, true,
            [] (auto i) { auto val = (i64)i; return TString((const char*)&val, sizeof(val)); },
            [] (auto i) { return Sprintf("%u", i); });

        testType(FLOAT4OID, true,
            [] (auto i) { auto val = (float)i; return TString((const char*)&val, sizeof(val)); },
            [] (auto i) { return Sprintf("%g", (float)i); });

        testType(FLOAT8OID, true,
            [] (auto i) { auto val = (double)i; return TString((const char*)&val, sizeof(val)); },
            [] (auto i) { return Sprintf("%lg", (double)i); });

        testByteaType();

        testType(TEXTOID, true,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("text %u", i), TEXTOID); },
            [] (auto i) { return Sprintf("text %u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(BPCHAROID, true,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("bpchar %u", i), BPCHAROID); },
            [] (auto i) { return Sprintf("bpchar %u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(VARCHAROID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("varchar %u", i), VARCHAROID); },
            [] (auto i) { return Sprintf("varchar %u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(NAMEOID, true,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("name %u", i), NAMEOID); },
            [] (auto i) { return Sprintf("name %u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(NUMERICOID, true,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("%lg", i + 0.12345), NUMERICOID); },
            [] (auto i) { return Sprintf("%lg", i + 0.12345); });

//        testType(DATEOID, true,
//            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("1970-01-%02u", i + 1), DATEOID); },
//            [] (auto i) { return Sprintf("1970-01-%02u", i + 1); });

//        testType(TIMEOID, true,
//            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("%02u:01:02.345", i), TIMEOID); },
//            [] (auto i) { return ""; });

//        testType(TIMESTAMPOID, true,
//            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("1970-01-01 %02u:01:02.345", i), TIMESTAMPOID); },
//            [] (auto i) { return ""; });

        testType(TIMETZOID, true,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("%02u:01:02.345-03", i), TIMETZOID); },
            [] (auto i) { return Sprintf("%02u:01:02.345-03", i); });

//        testType(TIMESTAMPTZOID, true,
//            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("1970-01-01 %02u:01:02.345 -3:00", i), TIMESTAMPTZOID); },
//            [] (auto i) { return ""; });

        testType(INTERVALOID, true,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("P01-02-03T04:05:%02u", i), INTERVALOID); },
            [] (auto i) { return Sprintf("1 year 2 mons 3 days 04:05:%02u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(BITOID, true,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("%c%c%c%c", (i&8)?'1':'0', (i&4)?'1':'0', (i&2)?'1':'0', (i&1)?'1':'0'), BITOID); },
            [] (auto i) { return Sprintf("%c%c%c%c", (i&8)?'1':'0', (i&4)?'1':'0', (i&2)?'1':'0', (i&1)?'1':'0'); });

        testType(VARBITOID, true,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("%c%c%c%c", (i&8)?'1':'0', (i&4)?'1':'0', (i&2)?'1':'0', (i&1)?'1':'0'), VARBITOID); },
            [] (auto i) { return Sprintf("%c%c%c%c", (i&8)?'1':'0', (i&4)?'1':'0', (i&2)?'1':'0', (i&1)?'1':'0'); });

        testType(POINTOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("(10, %u)", i), POINTOID); },
            [] (auto i) { return Sprintf("(10,%u)", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(LINEOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("{1, 2, %u}", i), LINEOID); },
            [] (auto i) { return Sprintf("{1,2,%u}", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(LSEGOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("[(0, 0), (1, %u)]", i), LSEGOID); },
            [] (auto i) { return Sprintf("[(0,0),(1,%u)]", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(BOXOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("(1, %u), (0, 0)", i + 1), BOXOID); },
            [] (auto i) { return Sprintf("(1,%u),(0,0)", i + 1); },
            [] (auto s) { return Sprintf("{%s;%s}", s.c_str(), s.c_str()); });

        testType(PATHOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("((0, 1), (2, 3), (4, %u))", i), PATHOID); },
            [] (auto i) { return Sprintf("((0,1),(2,3),(4,%u))", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(POLYGONOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("((0, 1), (2, 3), (4, %u))", i), POLYGONOID); },
            [] (auto i) { return Sprintf("((0,1),(2,3),(4,%u))", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(CIRCLEOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("<(0, 1), %u>", i), CIRCLEOID); },
            [] (auto i) { return Sprintf("<(0,1),%u>", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(INETOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("128.%u.0.0/16", i), INETOID); },
            [] (auto i) { return Sprintf("128.%u.0.0/16", i); });

        testType(CIDROID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("128.%u.0.0/16", i), CIDROID); },
            [] (auto i) { return Sprintf("128.%u.0.0/16", i); });

        testType(MACADDROID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("08:00:2b:01:02:%02u", i), MACADDROID); },
            [] (auto i) { return Sprintf("08:00:2b:01:02:%02u", i); });

        testType(MACADDR8OID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("08:00:2b:01:02:03:04:%02u", i), MACADDR8OID); },
            [] (auto i) { return Sprintf("08:00:2b:01:02:03:04:%02u", i); });

        testType(UUIDOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("00000000-0000-0000-0000-0000000000%02u", i), UUIDOID); },
            [] (auto i) { return Sprintf("00000000-0000-0000-0000-0000000000%02u", i); });

        testType(JSONOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("[%u]", i), JSONOID); },
            [] (auto i) { return Sprintf("[%u]", i); });

        testType(JSONBOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("[%u]", i), JSONBOID); },
            [] (auto i) { return Sprintf("[%u]", i); });

        testType(JSONPATHOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("$[%u]", i), JSONPATHOID); },
            [] (auto i) { return Sprintf("$[%u]", i); });

        testType(XMLOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("<a>%u</a>", i), XMLOID); },
            [] (auto i) { return Sprintf("<a>%u</a>", i); });

        testType(TSQUERYOID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("a&b%u", i), TSQUERYOID); },
            [] (auto i) { return Sprintf("'a' & 'b%u'", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(TSVECTOROID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("a:1 b:%u", i + 2), TSVECTOROID); },
            [] (auto i) { return Sprintf("'a':1 'b':%u", i + 2); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        testType(INT2VECTOROID, false,
            [] (auto i) { return NPg::PgNativeBinaryFromNativeText(Sprintf("%u %u %u", i, i + 1, i + 2), INT2VECTOROID); },
            [] (auto i) { return Sprintf("%u %u %u", i, i + 1, i + 2); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); });

        // TODO: varchar as a key
        // TODO: date/time types (?)
        // TODO: money (uses PGLC_localeconv())
        // TODO: native range/multirange types (use get_range_io_data())
    }

    Y_UNIT_TEST(EmptyQuery) {
        auto kikimr = DefaultKikimrRunner();
        NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_pg
        )").GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        Y_ENSURE(result.GetResultSets().empty());
    }

    Y_UNIT_TEST(NoTableQuery) {
        auto kikimr = DefaultKikimrRunner();
        NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_pg
            SELECT * FROM (VALUES
                (1, 'one'),
                (2, 'two'),
                (3, 'three')
            ) AS t (int8, varchar);
        )").GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            ["1";"one"];
            ["2";"two"];
            ["3";"three"]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

} // namespace NKqp
} // namespace NKikimr
