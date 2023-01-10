#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>

extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
}

namespace {
    struct TPgTypeTestSpec {
        bool IsKey;
        std::function<TString(size_t)> TextIn, TextOut;
        std::function<TString(TString)> ArrayPrint;
        TPgTypeTestSpec() = default;
        TPgTypeTestSpec(
            bool isKey,
            std::function<TString(size_t)> in,
            std::function<TString(size_t)> out,
            std::function<TString(TString)> print = [] (auto s) { return Sprintf("{%s,%s}", s.c_str(), s.c_str()); })
        : IsKey(isKey)
        , TextIn(in)
        , TextOut(out)
        , ArrayPrint(print) {}
    };
}

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpPg) {

    auto makePgType = [] (ui32 oid, i32 typlen = -1) { return TPgType(oid, typlen, -1); };

    TMap<
        ui32,
        TPgTypeTestSpec
    > typeSpecs ={
        { BOOLOID, {
            true,
            [] (auto i) { return TString(i ? "true" : "false"); },
            [] (auto i) { return TString(i ? "t" : "f"); }
            }
        },
        { CHAROID, {
            true,
            [] (auto i) { return Sprintf("%c", (char)(i + '0')); },
            [] (auto i) { return Sprintf("%c", (char)(i + '0')); }
            }
        },
        { INT2OID, {
            true,
            [] (auto i) { return Sprintf("%u", i); },
            [] (auto i) { return Sprintf("%u", i); }
            }
        },
        { INT4OID, {
            true,
            [] (auto i) { return Sprintf("%u", i); },
            [] (auto i) { return Sprintf("%u", i); }
            }
        },
        { INT8OID, {
            true,
            [] (auto i) { return Sprintf("%u", i); },
            [] (auto i) { return Sprintf("%u", i); }
            }
        },
        { FLOAT4OID, {
            true,
            [] (auto i) { return Sprintf("%g", i + 0.5f); },
            [] (auto i) { return Sprintf("%g", i + 0.5f); }
            }
        },
        { FLOAT8OID, {
            true,
            [] (auto i) { return Sprintf("%lg", i + 0.5); },
            [] (auto i) { return Sprintf("%lg", i + 0.5); }
            }
        },
        { TEXTOID, {
            true,
            [] (auto i) { return Sprintf("text %u", i); },
            [] (auto i) { return Sprintf("text %u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { BPCHAROID, {
            true,
            [] (auto i) { return Sprintf("bpchar %u", i); },
            [] (auto i) { return Sprintf("bpchar %u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { VARCHAROID, {
            false,
            [] (auto i) { return Sprintf("varchar %u", i); },
            [] (auto i) { return Sprintf("varchar %u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { NAMEOID, {
            true,
            [] (auto i) { return Sprintf("name %u", i); },
            [] (auto i) { return Sprintf("name %u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { NUMERICOID, {
            true,
            [] (auto i) { return Sprintf("%lg", i + 0.12345); },
            [] (auto i) { return Sprintf("%lg", i + 0.12345); }
            }
        },
        { MONEYOID, {
            true,
            [] (auto i) { return Sprintf("%lg", i + i / 100.); },
            [] (auto i) { return Sprintf("$%.2lf", i + i / 100.); }
            }
        },
        { DATEOID, {
            true,
            [] (auto i) { return Sprintf("1970-01-%02u", i + 1); },
            [] (auto i) { return Sprintf("1970-01-%02u", i + 1); }
            }
        },
        { TIMEOID, {
            true,
            [] (auto i) { return Sprintf("%02u:01:02.345", i); },
            [] (auto i) { return Sprintf("%02u:01:02.345", i); }
            }
        },
        { TIMESTAMPOID, {
            true,
            [] (auto i) { return Sprintf("1970-01-01 %02u:01:02.345", i); },
            [] (auto i) { return Sprintf("1970-01-01 %02u:01:02.345", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { TIMETZOID, {
            true,
            [] (auto i) { return Sprintf("%02u:01:02.345-03", i); },
            [] (auto i) { return Sprintf("%02u:01:02.345-03", i); }
            }
        },
        { TIMESTAMPTZOID, {
            true,
            [] (auto i) { return Sprintf("1970-01-01 %02u:01:02.345 -3:00", i); },
            [] (auto i) { return Sprintf("1970-01-01 %02u:01:02.345+00", i + 3); }, // TODO: investigate
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { INTERVALOID, {
            true,
            [] (auto i) { return Sprintf("P01-02-03T04:05:%02u", i); },
            [] (auto i) { return Sprintf("1 year 2 mons 3 days 04:05:%02u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { BITOID, {
            true,
            [] (auto i) { return Sprintf("%c%c%c%c", (i&8)?'1':'0', (i&4)?'1':'0', (i&2)?'1':'0', (i&1)?'1':'0'); },
            [] (auto i) { return Sprintf("%c%c%c%c", (i&8)?'1':'0', (i&4)?'1':'0', (i&2)?'1':'0', (i&1)?'1':'0'); }
            }
        },
        { VARBITOID, {
            true,
            [] (auto i) { return Sprintf("%c%c%c%c", (i&8)?'1':'0', (i&4)?'1':'0', (i&2)?'1':'0', (i&1)?'1':'0'); },
            [] (auto i) { return Sprintf("%c%c%c%c", (i&8)?'1':'0', (i&4)?'1':'0', (i&2)?'1':'0', (i&1)?'1':'0'); }
            }
        },
        { POINTOID, {
            false,
            [] (auto i) { return Sprintf("(10, %u)", i); },
            [] (auto i) { return Sprintf("(10,%u)", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { LINEOID, {
            false,
            [] (auto i) { return Sprintf("{1, 2, %u}", i); },
            [] (auto i) { return Sprintf("{1,2,%u}", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { LSEGOID, {
            false,
            [] (auto i) { return Sprintf("[(0, 0), (1, %u)]", i); },
            [] (auto i) { return Sprintf("[(0,0),(1,%u)]", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { BOXOID, {
            false,
            [] (auto i) { return Sprintf("(1, %u), (0, 0)", i + 1); },
            [] (auto i) { return Sprintf("(1,%u),(0,0)", i + 1); },
            [] (auto s) { return Sprintf("{%s;%s}", s.c_str(), s.c_str()); }
            }
        },
        { PATHOID, {
            false,
            [] (auto i) { return Sprintf("((0, 1), (2, 3), (4, %u))", i); },
            [] (auto i) { return Sprintf("((0,1),(2,3),(4,%u))", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { POLYGONOID, {
            false,
            [] (auto i) { return Sprintf("((0, 1), (2, 3), (4, %u))", i); },
            [] (auto i) { return Sprintf("((0,1),(2,3),(4,%u))", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { CIRCLEOID, {
            false,
            [] (auto i) { return Sprintf("<(0, 1), %u>", i); },
            [] (auto i) { return Sprintf("<(0,1),%u>", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { INETOID, {
            false,
            [] (auto i) { return Sprintf("128.%u.0.0/16", i); },
            [] (auto i) { return Sprintf("128.%u.0.0/16", i); }
            }
        },
        { CIDROID, {
            false,
            [] (auto i) { return Sprintf("128.%u.0.0/16", i); },
            [] (auto i) { return Sprintf("128.%u.0.0/16", i); }
            }
        },
        { MACADDROID, {
            false,
            [] (auto i) { return Sprintf("08:00:2b:01:02:%02u", i); },
            [] (auto i) { return Sprintf("08:00:2b:01:02:%02u", i); }
            }
        },
        { MACADDR8OID, {
            false,
            [] (auto i) { return Sprintf("08:00:2b:01:02:03:04:%02u", i); },
            [] (auto i) { return Sprintf("08:00:2b:01:02:03:04:%02u", i); }
            }
        },
        { UUIDOID, {
            false,
            [] (auto i) { return Sprintf("00000000-0000-0000-0000-0000000000%02u", i); },
            [] (auto i) { return Sprintf("00000000-0000-0000-0000-0000000000%02u", i); }
            }
        },
        { JSONOID, {
            false,
            [] (auto i) { return Sprintf("[%u]", i); },
            [] (auto i) { return Sprintf("[%u]", i); }
            }
        },
        { JSONBOID, {
            false,
            [] (auto i) { return Sprintf("[%u]", i); },
            [] (auto i) { return Sprintf("[%u]", i); }
            }
        },
        { JSONPATHOID, {
            false,
            [] (auto i) { return Sprintf("$[%u]", i); },
            [] (auto i) { return Sprintf("$[%u]", i); }
            }
        },
        { XMLOID, {
            false,
            [] (auto i) { return Sprintf("<a>%u</a>", i); },
            [] (auto i) { return Sprintf("<a>%u</a>", i); }
            }
        },
        { TSQUERYOID, {
            false,
            [] (auto i) { return Sprintf("a&b%u", i); },
            [] (auto i) { return Sprintf("'a' & 'b%u'", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { TSVECTOROID, {
            false,
            [] (auto i) { return Sprintf("a:1 b:%u", i + 2); },
            [] (auto i) { return Sprintf("'a':1 'b':%u", i + 2); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        },
        { INT2VECTOROID, {
            false,
            [] (auto i) { return Sprintf("%u %u %u", i, i + 1, i + 2); },
            [] (auto i) { return Sprintf("%u %u %u", i, i + 1, i + 2); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
            }
        }
    };
    auto createTable = [] (
        NYdb::NTable::TTableClient& db,
        NYdb::NTable::TSession& session,
        ui32 id,
        bool isKey,
        std::function<TString(size_t)> textIn,
        TString setTableName = "",
        ui16 rowCount = 10
    ) {
        TTableBuilder builder;
        if (isKey) {
            builder.AddNullableColumn("key", makePgType(id));
        } else {
            builder.AddNullableColumn("key", makePgType(INT2OID));
        }
        builder.AddNullableColumn("value", makePgType(id));
        builder.SetPrimaryKeyColumn("key");

        auto tableName = (setTableName.empty()) ? Sprintf("/Root/Pg%u", id) : setTableName;
        auto result = session.CreateTable(tableName, builder.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < rowCount; ++i) {
            auto str = NPg::PgNativeBinaryFromNativeText(textIn(i), id);
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
        return tableName;
    };

    Y_UNIT_TEST(CreateTableBulkUpsertAndRead) {
        TKikimrRunner kikimr;

        auto testSingleType = [&kikimr] (ui32 id, bool isKey,
            std::function<TString(size_t)> textIn,
            std::function<TString(size_t)> textOut)
        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(db, session, id, isKey, textIn);

            auto readSettings = TReadTableSettings()
                .AppendColumns("key")
                .AppendColumns("value");

            auto it = session.ReadTable(tableName, readSettings).GetValueSync();
            Y_ENSURE(it.IsSuccess());

            bool eos = false;
            while (!eos) {
                auto part = it.ReadNext().ExtractValueSync();
                if (!part.IsSuccess()) {
                    eos = true;
                    Y_ENSURE(part.EOS());
                    continue;
                }
                auto resultSet = part.ExtractPart();
                TResultSetParser parser(resultSet);
                for (size_t i = 0; parser.TryNextRow(); ++i) {
                    auto check = [&parser, &id, &i] (const TString& column, const TString& expected) {
                        auto& c = parser.ColumnParser(column);
                        UNIT_ASSERT_VALUES_EQUAL(expected, NPg::PgNativeTextFromNativeBinary(c.GetPg().Content_, id));
                        Cerr << expected << Endl;
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

        auto testType = [&] (ui32 id, const TPgTypeTestSpec& typeSpec)
        {
            testSingleType(id, typeSpec.IsKey, typeSpec.TextIn, typeSpec.TextOut);

            auto arrayId = NYql::NPg::LookupType(id).ArrayTypeId;

            auto textInArray = [&typeSpec] (auto i) {
                auto str = typeSpec.TextIn(i);
                return typeSpec.ArrayPrint(str);
            };

            auto textOutArray = [&typeSpec] (auto i) {
                auto str = typeSpec.TextOut(i);
                return typeSpec.ArrayPrint(str);
            };

            testSingleType(arrayId, false, textInArray, textOutArray);
        };

        auto testByteaType = [&] () {
            testSingleType(BYTEAOID, true,
                [] (auto i) { return Sprintf("bytea %u", i); },
                [] (auto i) { return Sprintf("\\x627974656120%x", i + 48); });

            testSingleType(BYTEAARRAYOID, false,
                [] (auto i) { return Sprintf("{a%u, b%u}", i, i + 10); },
                [] (auto i) { return Sprintf("{\"\\\\x61%x\",\"\\\\x6231%x\"}", i + 48, i + 48); });
        };

        testByteaType();

        for (const auto& [oid, spec] : typeSpecs) {
            testType(oid, spec);
        }
        // TODO: varchar as a key
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

    Y_UNIT_TEST(TableSelect) {
        auto kikimr = DefaultKikimrRunner();
        auto testSingleType = [&kikimr] (ui32 id, bool isKey,
            std::function<TString(size_t)> textIn,
            std::function<TString(size_t)> textOut)
        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(db, session, id, isKey, textIn);
            session.Close().GetValueSync();
            NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());
            auto result = client.ExecuteYqlScript(
                TStringBuilder() << R"(
                --!syntax_pg
                SELECT * FROM ")"
                << tableName << "\""
            ).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSetParser(0));
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                auto check = [&parser, &id] (const TString& column, const TString& expected) {
                    auto& c = parser.ColumnParser(column);
                    UNIT_ASSERT_VALUES_EQUAL(expected, c.GetPg().Content_);
                    Cerr << expected << Endl;
                };
                auto expected = textOut(i);
                if (isKey) {
                    check("key", expected);
                }
                check("value", expected);
            }
        };


        auto testType = [&] (ui32 id, const TPgTypeTestSpec& typeSpec)
        {
            testSingleType(id, typeSpec.IsKey, typeSpec.TextIn, typeSpec.TextOut);

            //arrays do not work for now due to postgress requesting null-terminated
            //data in ReadArrayBinary
            //KIKIMR-16501

            // auto arrayId = NYql::NPg::LookupType(id).ArrayTypeId;

            // auto textInArray = [&typeSpec] (auto i) {
            //     auto str = typeSpec.TextIn(i);
            //     return typeSpec.ArrayPrint(str);
            // };

            // auto textOutArray = [&typeSpec] (auto i) {
            //     auto str = typeSpec.TextOut(i);
            //     return typeSpec.ArrayPrint(str);
            // };

            // testSingleType(arrayId, false, textInArray, textOutArray);
        };

        auto testByteaType = [&] () {
            testSingleType(BYTEAOID, true,
                [] (auto i) { return Sprintf("bytea %u", i); },
                [] (auto i) { return Sprintf("\\x627974656120%x", i + 48); });

            testSingleType(BYTEAARRAYOID, false,
                [] (auto i) { return Sprintf("{a%u, b%u}", i, i + 10); },
                [] (auto i) { return Sprintf("{\"\\\\x61%x\",\"\\\\x6231%x\"}", i + 48, i + 48); });
        };
        testByteaType();
        for (const auto& [oid, spec] : typeSpecs) {
            Cerr << oid << Endl;
            testType(oid, spec);
        }
    }

    Y_UNIT_TEST(ReadPgArray) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        auto binaryStr = NPg::PgNativeBinaryFromNativeText("{1,1}", INT2ARRAYOID);
        Y_ENSURE(binaryStr.Size() == 32);
        auto value = NYql::NCommon::PgValueFromNativeBinary(binaryStr, INT2ARRAYOID);
    }

    Y_UNIT_TEST(CreateNotNullPgColumn) {
        auto kikimr = DefaultKikimrRunner();

        TTableBuilder builder;
        UNIT_ASSERT_EXCEPTION(builder.AddNonNullableColumn("key", makePgType(INT2OID)), yexception);
        //add create table check here once create table YQL is supported
    }
}

} // namespace NKqp
} // namespace NKikimr
