#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>
#include <ydb/library/yql/utils/log/log.h>
#include <util/system/env.h>


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

NYdb::NScripting::TExecuteYqlResult
ExecutePgSelect(NKikimr::NKqp::TKikimrRunner& kikimr, const TString& tableName) {
    NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());
    auto result = client.ExecuteYqlScript(
        TStringBuilder() << R"(
        --!syntax_pg
        SELECT * FROM ")"
        << tableName << "\""
    ).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return result;
}

void ExecutePgInsert(
    NKikimr::NKqp::TKikimrRunner& kikimr,
    const TString& tableName,
    ui32 id,
    const TPgTypeTestSpec& spec
) {
    NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());
    auto valType = NYql::NPg::LookupType(id).Name;
    auto keyType = (spec.IsKey) ? valType : "int2";
    if (id == BITOID) {
        valType.append("(4)");
    }
    for (size_t i = 0; i < ((id == BOOLOID) ? 2 : 3); i++) {
        auto keyIn = (spec.IsKey) ? spec.TextIn(i) : ToString(i);
        TString req = Sprintf("\
        --!syntax_pg\n\
        INSERT INTO \"%s\" (key, value) VALUES (\n\
            '%s'::%s, '%s'::%s\n\
        )", tableName.Data(), keyIn.Data(), keyType.Data(), spec.TextIn(i).Data(), valType.Data());
        Cerr << req << Endl;
        auto result = client.ExecuteYqlScript(req).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

void ExecutePgArrayInsert(
    NKikimr::NKqp::TKikimrRunner& kikimr,
    const TString& tableName,
    ui32 id,
    const TPgTypeTestSpec& spec
) {
    NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());
    auto valType = NYql::NPg::LookupType(id).Name;
    if (id == BITOID) {
        valType.append("(4)");
    }
    for (size_t i = 0; i < ((id == BOOLOID) ? 2 : 3); i++) {
        auto keyEntry = Sprintf("'%d'::int2", i);
        auto valueEntry = Sprintf(
            "ARRAY ['%s'::%s, '%s'::%s]",
            spec.TextIn(i).Data(),
            valType.Data(),
            spec.TextIn(i).Data(),
            valType.Data()
        );
        TString req = Sprintf("\
        --!syntax_pg\n\
        INSERT INTO \"%s\" (key, value) VALUES (\n\
            %s, %s\n\
        );", tableName.Data(), keyEntry.Data(), valueEntry.Data());
        Cerr << req << Endl;
        auto result = client.ExecuteYqlScript(req).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

void ValidatePgYqlResult(const NYdb::NScripting::TExecuteYqlResult& result, const TPgTypeTestSpec& spec) {
    TResultSetParser parser(result.GetResultSetParser(0));
    bool gotRows = false;
    for (size_t i = 0; parser.TryNextRow(); ++i) {
        gotRows = true;
        auto check = [&parser] (const TString& column, const TString& expected) {
            auto& c = parser.ColumnParser(column);
            UNIT_ASSERT_VALUES_EQUAL(expected, c.GetPg().Content_);
            Cerr << expected << Endl;
        };
        auto expected = spec.TextOut(i);
        if (spec.IsKey) {
            check("key", expected);
        }
        check("value", expected);
    }
    Y_ENSURE(gotRows, "empty select result");
}

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
        bool isText,
        std::function<TString(size_t)> textIn,
        TString setTableName = "",
        ui16 rowCount = 10,
        TVector <TString> colNames = {"key", "value"}
    ) {
        TTableBuilder builder;
        if (isKey) {
            builder.AddNullableColumn(colNames[0], makePgType(id));
        } else {
            builder.AddNullableColumn(colNames[0], makePgType(INT2OID));
        }
        builder.AddNullableColumn(colNames[1], makePgType(id));
        builder.SetPrimaryKeyColumn(colNames[0]);

        auto tableName = (setTableName.empty()) ?
            Sprintf("/Root/Pg%u_%s", id, isText ? "t" : "b") : setTableName;

        auto result = session.CreateTable(tableName, builder.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < rowCount; ++i) {
            auto str = isText ? textIn(i) : NPg::PgNativeBinaryFromNativeText(textIn(i), id).Str;
            auto mode = isText ? TPgValue::VK_TEXT : TPgValue::VK_BINARY;
            if (isKey) {
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember(colNames[0]).Pg(TPgValue(mode, str, makePgType(id)))
                    .AddMember(colNames[1]).Pg(TPgValue(mode, str, makePgType(id)))
                    .EndStruct();
            } else {
                auto int2Str = NPg::PgNativeBinaryFromNativeText(Sprintf("%u", i), INT2OID).Str;
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember(colNames[0]).Pg(TPgValue(TPgValue::VK_BINARY, int2Str, makePgType(INT2OID)))
                    .AddMember(colNames[1]).Pg(TPgValue(mode, str, makePgType(id)))
                    .EndStruct();
            }
        }
        rows.EndList();

        result = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto readSettings = TReadTableSettings()
            .AppendColumns(colNames[0])
            .AppendColumns(colNames[1]);

        auto it = session.ReadTable(tableName, readSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), result.GetIssues().ToString());
        return tableName;
    };

    Y_UNIT_TEST(CreateTableBulkUpsertAndRead) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (ui32 id, bool isKey, bool isText,
            std::function<TString(size_t)> textIn,
            std::function<TString(size_t)> textOut)
        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(db, session, id, isKey, isText, textIn);

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
                        auto result = NPg::PgNativeTextFromNativeBinary(c.GetPg().Content_, id);
                        UNIT_ASSERT_C(result.Error.empty(), result.Error);
                        UNIT_ASSERT_VALUES_EQUAL(expected, result.Str);
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
            testSingleType(id, typeSpec.IsKey, false, typeSpec.TextIn, typeSpec.TextOut);
            testSingleType(id, typeSpec.IsKey, true, typeSpec.TextIn, typeSpec.TextOut);

            auto arrayId = NYql::NPg::LookupType(id).ArrayTypeId;

            auto textInArray = [&typeSpec] (auto i) {
                auto str = typeSpec.TextIn(i);
                return typeSpec.ArrayPrint(str);
            };

            auto textOutArray = [&typeSpec] (auto i) {
                auto str = typeSpec.TextOut(i);
                return typeSpec.ArrayPrint(str);
            };

            testSingleType(arrayId, typeSpec.IsKey, false, textInArray, textOutArray);
            testSingleType(arrayId, typeSpec.IsKey, true, textInArray, textOutArray);
        };

        auto testByteaType = [&] () {
            testSingleType(BYTEAOID, true, false,
                [] (auto i) { return Sprintf("bytea %u", i); },
                [] (auto i) { return Sprintf("\\x627974656120%x", i + 48); });

            testSingleType(BYTEAOID, true, true,
                [] (auto i) { return Sprintf("bytea %u", i); },
                [] (auto i) { return Sprintf("\\x627974656120%x", i + 48); });

            testSingleType(BYTEAARRAYOID, true, false,
                [] (auto i) { return Sprintf("{a%u, b%u}", i, i + 10); },
                [] (auto i) { return Sprintf("{\"\\\\x61%x\",\"\\\\x6231%x\"}", i + 48, i + 48); });

            testSingleType(BYTEAARRAYOID, true, true,
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
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());

        auto result = client.ExecuteYqlScript(R"(
            --!syntax_pg
        )").GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        Y_ENSURE(result.GetResultSets().empty());
    }

    Y_UNIT_TEST(NoTableQuery) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
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
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto testSingleType = [&kikimr] (ui32 id, const TPgTypeTestSpec& spec)
        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(db, session, id, spec.IsKey, false, spec.TextIn);
            session.Close().GetValueSync();
            NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());
            auto result = ExecutePgSelect(kikimr, tableName);
            ValidatePgYqlResult(result, spec);
        };


        auto testType = [&] (ui32 id, const TPgTypeTestSpec& typeSpec)
        {
            testSingleType(id, typeSpec);

            auto arrayId = NYql::NPg::LookupType(id).ArrayTypeId;
            auto textInArray = [&typeSpec] (auto i) {
                auto str = typeSpec.TextIn(i);
                return typeSpec.ArrayPrint(str);
            };
            auto textOutArray = [&typeSpec] (auto i) {
                auto str = typeSpec.TextOut(i);
                return typeSpec.ArrayPrint(str);
            };
            TPgTypeTestSpec arraySpec(typeSpec.IsKey, textInArray, textOutArray);

            testSingleType(arrayId, arraySpec);
        };

        auto testByteaType = [&] () {
            TPgTypeTestSpec byteaSpec(
                true,
                [] (auto i) { return Sprintf("bytea %u", i); },
                [] (auto i) { return Sprintf("\\x627974656120%x", i + 48); }
            );

            TPgTypeTestSpec bytearrSpec(
                false,
                [] (auto i) { return Sprintf("{a%u, b%u}", i, i + 10); },
                [] (auto i) { return Sprintf("{\"\\\\x61%x\",\"\\\\x6231%x\"}", i + 48, i + 48); }
            );
            testSingleType(BYTEAOID, byteaSpec);
            testSingleType(BYTEAARRAYOID, bytearrSpec);
        };
        testByteaType();
        for (const auto& [oid, spec] : typeSpecs) {
            Cerr << oid << Endl;
            testType(oid, spec);
        }
    }

    Y_UNIT_TEST(ReadPgArray) {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        auto binaryStr = NPg::PgNativeBinaryFromNativeText("{1,1}", INT2ARRAYOID).Str;
        Y_ENSURE(binaryStr.Size() == 32);
        auto value = NYql::NCommon::PgValueFromNativeBinary(binaryStr, INT2ARRAYOID);
    }

    Y_UNIT_TEST(TableInsert) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (ui32 id, const TPgTypeTestSpec& spec) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(
                db,
                session,
                id,
                spec.IsKey,
                false,
                spec.TextIn,
                "",
                0
            );
            session.Close().GetValueSync();

            ExecutePgInsert(kikimr, tableName, id, spec);
            auto result = ExecutePgSelect(kikimr, tableName);
            ValidatePgYqlResult(result, spec);
        };

        auto testType = [&] (ui32 id, const TPgTypeTestSpec& typeSpec)
        {
            testSingleType(id, typeSpec);
        };
        auto testByteaType = [&] () {
            TPgTypeTestSpec byteaSpec(
                true,
                [] (auto i) { return Sprintf("bytea %u", i); },
                [] (auto i) { return Sprintf("\\x627974656120%x", i + 48); }
            );

            TPgTypeTestSpec bytearrSpec(
                false,
                [] (auto i) { return Sprintf("{a%u, b%u}", i, i + 10); },
                [] (auto i) { return Sprintf("{\"\\\\x61%x\",\"\\\\x6231%x\"}", i + 48, i + 48); }
            );
            testSingleType(BYTEAOID, byteaSpec);
            testSingleType(BYTEAARRAYOID, bytearrSpec);
        };
        testByteaType();
        for (auto [oid, spec] : typeSpecs) {
            Cerr << oid << Endl;
            if (oid == CHAROID) {
                continue;
                // I cant come up with a query with explicit char conversion.
                // ::char, ::character casts to pg_bpchar
            }
            if (oid == MONEYOID || oid == BITOID || oid == VARBITOID) {
                spec.IsKey = false;
                // Those types do not have HashProcId, so are not hashable,
                // And we can not validate their uniqueness as keys in INSERT.
            }
            testType(oid, spec);
        }
    }

    Y_UNIT_TEST(TableArrayInsert) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (ui32 id, const TPgTypeTestSpec& spec) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto arrayId = NYql::NPg::LookupType(id).ArrayTypeId;
            auto tableName = createTable(
                db,
                session,
                arrayId,
                spec.IsKey/*false*/,
                false,
                spec.TextIn,
                "",
                0
            );
            session.Close().GetValueSync();

            ExecutePgArrayInsert(kikimr, tableName, id, spec);

            auto result = ExecutePgSelect(kikimr, tableName);

            TResultSetParser parser(result.GetResultSetParser(0));
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                auto check = [&parser, &id, &spec] (const TString& column, const TString& expected) {
                    auto& c = parser.ColumnParser(column);
                    UNIT_ASSERT_VALUES_EQUAL(expected, c.GetPg().Content_);
                };
                TString expected = spec.TextOut(i);
                check("value", expected);
            }
        };


        auto testType = [&] (ui32 id, const TPgTypeTestSpec& typeSpec) {
            auto textOutArray = [&typeSpec] (auto i) {
                auto str = typeSpec.TextOut(i);
                return typeSpec.ArrayPrint(str);
            };
            TPgTypeTestSpec arraySpec(false, typeSpec.TextIn, textOutArray);
            testSingleType(id, arraySpec);
        };

        for (auto [oid, spec] : typeSpecs) {
            Cerr << oid << Endl;
            if (oid == CHAROID) {
                continue;
                // I cant come up with a query with explicit char conversion.
                // ::char, ::character casts to pg_bpchar
            }
            if (oid == MONEYOID || oid == BITOID || oid == VARBITOID) {
                spec.IsKey = false;
                // Those types do not have HashProcId, so are not hashable,
                // And we can not validate their uniqueness as keys in INSERT.
            }
            testType(oid, spec);
        }
    }

    Y_UNIT_TEST(InsertFromSelect) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (ui32 id, bool isKey,
            std::function<TString(size_t)> textIn,
            std::function<TString(size_t)> textOut)
        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(db, session, id, isKey, false, textIn, "", 10, {"key1", "value1"});
            TString emptyTableName = "/Root/PgEmpty" + ToString(id);
            createTable(db, session, id, isKey, false, textIn, emptyTableName, 0);
            session.Close().GetValueSync();
            NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());
            auto result = client.ExecuteYqlScript(
                TStringBuilder() << R"(
                --!syntax_pg
                INSERT INTO ")" << emptyTableName << "\" (key, value) SELECT * FROM \"" << tableName << "\";"
            ).GetValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::INTERNAL_ERROR);
        };

        auto testType = [&] (ui32 id, const TPgTypeTestSpec& typeSpec)
        {
            testSingleType(id, typeSpec.IsKey, typeSpec.TextIn, typeSpec.TextOut);
        };

        testType(INT2OID, typeSpecs[INT2OID]);
    }

    Y_UNIT_TEST(CreateTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto testSingleType = [&kikimr] (ui32 id, const TPgTypeTestSpec& spec, bool isArray)
        {
            NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());

            auto tableName = "/Root/Pg" + ToString(id) + (isArray ? "array" : "");
            auto typeName = ((isArray) ? "_pg" : "pg") + NYql::NPg::LookupType(id).Name;
            auto keyEntry = spec.IsKey ? ("key "+ typeName) : "key pgint2";
            auto valueEntry = "value " + typeName;
            auto req = Sprintf("\
            CREATE TABLE `%s` (\n\
                %s,\n\
                %s,\n\
                PRIMARY KEY (key)\n\
            );", tableName.Data(), keyEntry.Data(), valueEntry.Data());
            Cerr << req << Endl;
            auto result = client.ExecuteYqlScript(req).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            if (!isArray) {
                ExecutePgInsert(kikimr, tableName, id, spec);
                result = ExecutePgSelect(kikimr, tableName);
                ValidatePgYqlResult(result, spec);
            } else {
                ExecutePgArrayInsert(kikimr, tableName, id, spec);
                result = ExecutePgSelect(kikimr, tableName);
                TResultSetParser parser(result.GetResultSetParser(0));
                for (size_t i = 0; parser.TryNextRow(); ++i) {
                    auto check = [&parser, &id, &spec] (const TString& column, const TString& expected) {
                        auto& c = parser.ColumnParser(column);
                        UNIT_ASSERT_VALUES_EQUAL(expected, c.GetPg().Content_);
                    };
                    TString expected = spec.TextOut(i);
                    check("value", expected);
                }
            }
        };

        auto testType = [&] (ui32 id, const TPgTypeTestSpec& typeSpec)
        {
            testSingleType(id, typeSpec, false);

            auto textOutArray = [&typeSpec] (auto i) {
                auto str = typeSpec.TextOut(i);
                return typeSpec.ArrayPrint(str);
            };
            TPgTypeTestSpec arraySpec(false, typeSpec.TextIn, textOutArray);

            testSingleType(id, arraySpec, true);

        };
        for (auto [oid, spec] : typeSpecs) {
            if (oid == CHAROID) {
                continue;
            }
            if (oid == CHAROID) {
                continue;
                // I cant come up with a query with explicit char conversion.
                // ::char, ::character casts to pg_bpchar
            }
            if (oid == MONEYOID || oid == BITOID || oid == VARBITOID) {
                spec.IsKey = false;
                // Those types do not have HashProcId, so are not hashable,
                // And we can not validate their uniqueness as keys in INSERT.
            }
            testType(oid, spec);
        }
    }

    Y_UNIT_TEST(CreateNotNullPgColumn) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        TTableBuilder builder;
        UNIT_ASSERT_EXCEPTION(builder.AddNonNullableColumn("key", makePgType(INT2OID)), yexception);

        NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());
        auto req = TStringBuilder() << R"(
        --!syntax_v1
        CREATE TABLE `/Root/Pg` (
        key pg_int2,
        value pg_int2 NOT NULL,
        PRIMARY KEY (key)
        );)";
        Cerr << req << Endl;
        auto result = client.ExecuteYqlScript(req).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::INTERNAL_ERROR);
        UNIT_ASSERT(result.GetIssues().begin()->GetMessage().EndsWith("notNull is forbidden for pg types"));
    }
}

} // namespace NKqp
} // namespace NKikimr
