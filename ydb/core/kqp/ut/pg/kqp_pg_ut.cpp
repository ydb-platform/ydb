#include "ydb/public/sdk/cpp/client/ydb_proto/accessor.h"
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
        ui32 TypeId;
        bool IsKey;
        std::function<TString(size_t)> TextIn, TextOut;
        std::function<TString(TString)> ArrayPrint = [] (auto s) { return Sprintf("{%s,%s}", s.c_str(), s.c_str()); };
    };

    struct TPgTypeCoercionTestSpec {
        ui32 TypeId;
        TString TypeMod;
        bool ShouldPass;
        std::function<TString()> TextIn, TextOut;
        std::function<TString(TString)> ArrayPrint = [] (auto s) { return Sprintf("{%s,%s}", s.c_str(), s.c_str()); };
    };
}

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

NYdb::NTable::TDataQueryResult ExecutePgSelect(
    NKikimr::NKqp::TKikimrRunner& kikimr, const TString& tableName)
{
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    auto result = session.ExecuteDataQuery(
        TStringBuilder() << R"(
        --!syntax_pg
        SELECT * FROM ")"
        << tableName << "\""
    , TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result;
}

void ExecutePgInsert(
    NKikimr::NKqp::TKikimrRunner& kikimr,
    const TString& tableName,
    const TPgTypeTestSpec& spec)
{
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    auto valType = NYql::NPg::LookupType(spec.TypeId).Name;
    if (spec.TypeId == CHAROID) {
        valType = "\"char\"";
    }
    if (spec.TypeId == BITOID) {
        valType.append("(4)");
    }
    auto keyType = (spec.IsKey) ? valType : "int2";
    for (size_t i = 0; i < ((spec.TypeId == BOOLOID) ? 2 : 3); i++) {
        auto keyIn = (spec.IsKey) ? spec.TextIn(i) : ToString(i);
        TString req = Sprintf("\
        --!syntax_pg\n\
        INSERT INTO %s (key, value) VALUES (\n\
            '%s'::%s, '%s'::%s\n\
        )", tableName.Data(), keyIn.Data(), keyType.Data(), spec.TextIn(i).Data(), valType.Data());
        Cerr << req << Endl;
        auto result = session.ExecuteDataQuery(req, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
}

void ExecutePgArrayInsert(
    NKikimr::NKqp::TKikimrRunner& kikimr,
    const TString& tableName,
    const TPgTypeTestSpec& spec)
{
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    auto valType = NYql::NPg::LookupType(spec.TypeId).Name;
    if (spec.TypeId == CHAROID) {
        valType = "\"char\"";
    }
    if (spec.TypeId == BITOID) {
        valType.append("(4)");
    }
    for (size_t i = 0; i < ((spec.TypeId == BOOLOID) ? 2 : 3); i++) {
        auto keyEntry = Sprintf("'%u'::int2", i);
        auto valueEntry = Sprintf(
            "ARRAY ['%s'::%s, '%s'::%s]",
            spec.TextIn(i).Data(),
            valType.Data(),
            spec.TextIn(i).Data(),
            valType.Data()
        );
        TString req = Sprintf("\
        --!syntax_pg\n\
        INSERT INTO %s (key, value) VALUES (\n\
            %s, %s\n\
        );", tableName.Data(), keyEntry.Data(), valueEntry.Data());
        Cerr << req << Endl;
        auto result = session.ExecuteDataQuery(req, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
}

bool ExecutePgInsertForCoercion(
    NKikimr::NKqp::TKikimrRunner& kikimr,
    const TString& tableName,
    const TPgTypeCoercionTestSpec& spec)
{
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    auto valType = NYql::NPg::LookupType(spec.TypeId).Name;
    if (spec.TypeId == CHAROID) {
        valType = "\"char\"";
    }
    if (spec.TypeId == BITOID) {
        valType.append("(4)");
    }

    TString req = Sprintf("\
    --!syntax_pg\n\
    INSERT INTO %s (key, value) VALUES (\n\
        '0'::int2, '%s'::%s\n\
    )", tableName.Data(), spec.TextIn().Data(), valType.Data());
    Cerr << req << Endl;

    auto result = session.ExecuteDataQuery(req, TTxControl::BeginTx().CommitTx()).GetValueSync();
    if (!result.IsSuccess()) {
        Cerr << result.GetIssues().ToString() << Endl;
    }
    return result.IsSuccess();
}

template <typename T>
ui32 ValidatePgYqlResult(const T& result, const TPgTypeTestSpec& spec, bool check = true) {
    TResultSetParser parser(result.GetResultSetParser(0));
    ui32 rows = 0;
    for (size_t i = 0; parser.TryNextRow(); ++i) {
        rows++;
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
    if (check) {
        Y_ENSURE(rows, "empty select result");
    }
    return rows;
}

void ValidateTypeCoercionResult(
    NYdb::NTable::TSession& session,
    const TString& tableName,
    const TPgTypeCoercionTestSpec& spec)
{
    auto it = session.ReadTable("/Root/" + tableName).GetValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

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
            auto expected = spec.TextOut();
            auto& c = parser.ColumnParser("value");
            UNIT_ASSERT_VALUES_EQUAL(expected, c.GetPg().Content_);
            Cerr << expected << Endl;
        }
    }
}

Y_UNIT_TEST_SUITE(KqpPg) {

    TVector<TPgTypeTestSpec> typeSpecs = {
        {
            BOOLOID,
            true,
            [] (auto i) { return TString(i ? "true" : "false"); },
            [] (auto i) { return TString(i ? "t" : "f"); }
        },
        {
            CHAROID,
            true,
            [] (auto i) { return Sprintf("%c", (char)(i + '0')); },
            [] (auto i) { return Sprintf("%c", (char)(i + '0')); }
        },
        {
            INT2OID,
            true,
            [] (auto i) { return Sprintf("%u", i); },
            [] (auto i) { return Sprintf("%u", i); }
        },
        {
            INT4OID,
            true,
            [] (auto i) { return Sprintf("%u", i); },
            [] (auto i) { return Sprintf("%u", i); }
        },
        {
            INT8OID,
            true,
            [] (auto i) { return Sprintf("%u", i); },
            [] (auto i) { return Sprintf("%u", i); }
        },
        {
            FLOAT4OID,
            true,
            [] (auto i) { return Sprintf("%g", i + 0.5f); },
            [] (auto i) { return Sprintf("%g", i + 0.5f); }
        },
        {
            FLOAT8OID,
            true,
            [] (auto i) { return Sprintf("%lg", i + 0.5); },
            [] (auto i) { return Sprintf("%lg", i + 0.5); }
        },
        {
            TEXTOID,
            true,
            [] (auto i) { return Sprintf("text %u", i); },
            [] (auto i) { return Sprintf("text %u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            BPCHAROID,
            true,
            [] (auto i) { return Sprintf("bpchar %u", i); },
            [] (auto i) { return Sprintf("bpchar %u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            VARCHAROID,
            false,
            [] (auto i) { return Sprintf("varchar %u", i); },
            [] (auto i) { return Sprintf("varchar %u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            NAMEOID,
            true,
            [] (auto i) { return Sprintf("name %u", i); },
            [] (auto i) { return Sprintf("name %u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            NUMERICOID,
            true,
            [] (auto i) { return Sprintf("%lg", i + 0.12345); },
            [] (auto i) { return Sprintf("%lg", i + 0.12345); }
        },
        {
            MONEYOID,
            false, // no HashProcId
            [] (auto i) { return Sprintf("%lg", i + i / 100.); },
            [] (auto i) { return Sprintf("$%.2lf", i + i / 100.); }
        },
        {
            DATEOID,
            true,
            [] (auto i) { return Sprintf("1970-01-%02u", i + 1); },
            [] (auto i) { return Sprintf("1970-01-%02u", i + 1); }
        },
        {
            TIMEOID,
            true,
            [] (auto i) { return Sprintf("%02u:01:02.345", i); },
            [] (auto i) { return Sprintf("%02u:01:02.345", i); }
        },
        {
            TIMESTAMPOID,
            true,
            [] (auto i) { return Sprintf("1970-01-01 %02u:01:02.345", i); },
            [] (auto i) { return Sprintf("1970-01-01 %02u:01:02.345", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            TIMETZOID,
            true,
            [] (auto i) { return Sprintf("%02u:01:02.345-03", i); },
            [] (auto i) { return Sprintf("%02u:01:02.345-03", i); }
        },
        {
            TIMESTAMPTZOID,
            true,
            [] (auto i) { return Sprintf("1970-01-01 %02u:01:02.345 -3:00", i); },
            [] (auto i) { return Sprintf("1970-01-01 %02u:01:02.345+00", i + 3); }, // TODO: investigate
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            INTERVALOID,
            true,
            [] (auto i) { return Sprintf("P01-02-03T04:05:%02u", i); },
            [] (auto i) { return Sprintf("1 year 2 mons 3 days 04:05:%02u", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            BITOID,
            false, // no HashProcId
            [] (auto i) { return Sprintf("%c%c%c%c", (i&8)?'1':'0', (i&4)?'1':'0', (i&2)?'1':'0', (i&1)?'1':'0'); },
            [] (auto i) { return Sprintf("%c%c%c%c", (i&8)?'1':'0', (i&4)?'1':'0', (i&2)?'1':'0', (i&1)?'1':'0'); }
        },
        {
            VARBITOID,
            false, // no HashProcId
            [] (auto i) { return Sprintf("%c%c%c%c", (i&8)?'1':'0', (i&4)?'1':'0', (i&2)?'1':'0', (i&1)?'1':'0'); },
            [] (auto i) { return Sprintf("%c%c%c%c", (i&8)?'1':'0', (i&4)?'1':'0', (i&2)?'1':'0', (i&1)?'1':'0'); }
        },
        {
            POINTOID,
            false,
            [] (auto i) { return Sprintf("(10, %u)", i); },
            [] (auto i) { return Sprintf("(10,%u)", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            LINEOID,
            false,
            [] (auto i) { return Sprintf("{1, 2, %u}", i); },
            [] (auto i) { return Sprintf("{1,2,%u}", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            LSEGOID,
            false,
            [] (auto i) { return Sprintf("[(0, 0), (1, %u)]", i); },
            [] (auto i) { return Sprintf("[(0,0),(1,%u)]", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            BOXOID,
            false,
            [] (auto i) { return Sprintf("(1, %u), (0, 0)", i + 1); },
            [] (auto i) { return Sprintf("(1,%u),(0,0)", i + 1); },
            [] (auto s) { return Sprintf("{%s;%s}", s.c_str(), s.c_str()); }
        },
        {
            PATHOID,
            false,
            [] (auto i) { return Sprintf("((0, 1), (2, 3), (4, %u))", i); },
            [] (auto i) { return Sprintf("((0,1),(2,3),(4,%u))", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            POLYGONOID,
            false,
            [] (auto i) { return Sprintf("((0, 1), (2, 3), (4, %u))", i); },
            [] (auto i) { return Sprintf("((0,1),(2,3),(4,%u))", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            CIRCLEOID,
            false,
            [] (auto i) { return Sprintf("<(0, 1), %u>", i); },
            [] (auto i) { return Sprintf("<(0,1),%u>", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            INETOID,
            false,
            [] (auto i) { return Sprintf("128.%u.0.0/16", i); },
            [] (auto i) { return Sprintf("128.%u.0.0/16", i); }
        },
        {
            CIDROID,
            false,
            [] (auto i) { return Sprintf("128.%u.0.0/16", i); },
            [] (auto i) { return Sprintf("128.%u.0.0/16", i); }
        },
        {
            MACADDROID,
            false,
            [] (auto i) { return Sprintf("08:00:2b:01:02:%02u", i); },
            [] (auto i) { return Sprintf("08:00:2b:01:02:%02u", i); }
        },
        {
            MACADDR8OID,
            false,
            [] (auto i) { return Sprintf("08:00:2b:01:02:03:04:%02u", i); },
            [] (auto i) { return Sprintf("08:00:2b:01:02:03:04:%02u", i); }
        },
        {
            UUIDOID,
            false,
            [] (auto i) { return Sprintf("00000000-0000-0000-0000-0000000000%02u", i); },
            [] (auto i) { return Sprintf("00000000-0000-0000-0000-0000000000%02u", i); }
        },
        {
            JSONOID,
            false,
            [] (auto i) { return Sprintf("[%u]", i); },
            [] (auto i) { return Sprintf("[%u]", i); }
        },
        {
            JSONBOID,
            false,
            [] (auto i) { return Sprintf("[%u]", i); },
            [] (auto i) { return Sprintf("[%u]", i); }
        },
        {
            JSONPATHOID,
            false,
            [] (auto i) { return Sprintf("$[%u]", i); },
            [] (auto i) { return Sprintf("$[%u]", i); }
        },
        {
            XMLOID,
            false,
            [] (auto i) { return Sprintf("<a>%u</a>", i); },
            [] (auto i) { return Sprintf("<a>%u</a>", i); }
        },
        {
            TSQUERYOID,
            false,
            [] (auto i) { return Sprintf("a&b%u", i); },
            [] (auto i) { return Sprintf("'a' & 'b%u'", i); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            TSVECTOROID,
            false,
            [] (auto i) { return Sprintf("a:1 b:%u", i + 2); },
            [] (auto i) { return Sprintf("'a':1 'b':%u", i + 2); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            INT2VECTOROID,
            false,
            [] (auto i) { return Sprintf("%u %u %u", i, i + 1, i + 2); },
            [] (auto i) { return Sprintf("%u %u %u", i, i + 1, i + 2); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
    };

    TPgTypeTestSpec typeByteaSpec{
        BYTEAOID,
        true,
        [] (auto i) { return Sprintf("bytea %u", i); },
        [] (auto i) { return Sprintf("\\x627974656120%x", i + 48); }
    };

    TPgTypeTestSpec typeByteaArraySpec{
        BYTEAARRAYOID,
        true,
        [] (auto i) { return Sprintf("{a%u, b%u}", i, i + 10); },
        [] (auto i) { return Sprintf("{\"\\\\x61%x\",\"\\\\x6231%x\"}", i + 48, i + 48); }
    };


#define SUCCESS true
#define FAIL false

    TVector<TPgTypeCoercionTestSpec> typeCoercionSpecs = {
        {
            BPCHAROID, "2",
            FAIL,
            [] () { return TString("abcd"); },
            [] () { return TString(""); }
        },
        {
            BPCHAROID, "4",
            SUCCESS,
            [] () { return TString("abcd"); },
            [] () { return TString("abcd"); }
        },
        {
            BPCHAROID, "6",
            SUCCESS,
            [] () { return TString("abcd"); },
            [] () { return TString("abcd  "); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },

        {
            VARCHAROID, "2",
            FAIL,
            [] () { return TString("abcd"); },
            [] () { return TString(""); }
        },
        {
            VARCHAROID, "4",
            SUCCESS,
            [] () { return TString("abcd"); },
            [] () { return TString("abcd"); }
        },
        {
            VARCHAROID, "6",
            SUCCESS,
            [] () { return TString("abcd"); },
            [] () { return TString("abcd"); }
        },

        {
            BITOID, "2",
            FAIL,
            [] () { return TString("1111"); },
            [] () { return TString(""); }
        },
        {
            BITOID, "4",
            SUCCESS,
            [] () { return TString("1111"); },
            [] () { return TString("1111"); }
        },
        {
            BITOID, "6",
            FAIL,
            [] () { return TString("1111"); },
            [] () { return TString(""); }
        },

        {
            VARBITOID, "2",
            FAIL,
            [] () { return TString("1111"); },
            [] () { return TString(""); }
        },
        {
            VARBITOID, "4",
            SUCCESS,
            [] () { return TString("1111"); },
            [] () { return TString("1111"); }
        },
        {
            VARBITOID, "6",
            SUCCESS,
            [] () { return TString("1111"); },
            [] () { return TString("1111"); }
        },

        {
            NUMERICOID, "2",
            FAIL,
            [] () { return TString("9999"); },
            [] () { return TString(""); }
        },
        {
            NUMERICOID, "4",
            SUCCESS,
            [] () { return TString("9999.1234"); },
            [] () { return TString("9999"); }
        },
        {
            NUMERICOID, "4",
            SUCCESS,
            [] () { return TString("9999"); },
            [] () { return TString("9999"); }
        },
        {
            NUMERICOID, "6",
            SUCCESS,
            [] () { return TString("9999"); },
            [] () { return TString("9999"); }
        },
        {
            NUMERICOID, "20,2",
            SUCCESS,
            [] () { return TString("99.1234"); },
            [] () { return TString("99.12"); }
        },
        {
            NUMERICOID, "20,4",
            SUCCESS,
            [] () { return TString("99.1234"); },
            [] () { return TString("99.1234"); }
        },
        {
            NUMERICOID, "20,6",
            SUCCESS,
            [] () { return TString("99.1234"); },
            [] () { return TString("99.123400"); }
        },

        {
            TIMEOID, "2",
            SUCCESS,
            [] () { return TString("01:02:03.1234"); },
            [] () { return TString("01:02:03.12"); }
        },
        {
            TIMEOID, "4",
            SUCCESS,
            [] () { return TString("01:02:03.1234"); },
            [] () { return TString("01:02:03.1234"); }
        },
        {
            TIMEOID, "6",
            SUCCESS,
            [] () { return TString("01:02:03.1234"); },
            [] () { return TString("01:02:03.1234"); }
        },

        {
            TIMETZOID, "2",
            SUCCESS,
            [] () { return TString("01:02:03.1234+00"); },
            [] () { return TString("01:02:03.12+00"); }
        },
        {
            TIMETZOID, "4",
            SUCCESS,
            [] () { return TString("01:02:03.1234+00"); },
            [] () { return TString("01:02:03.1234+00"); }
        },
        {
            TIMETZOID, "6",
            SUCCESS,
            [] () { return TString("01:02:03.1234+00"); },
            [] () { return TString("01:02:03.1234+00"); }
        },

        {
            TIMESTAMPOID, "2",
            SUCCESS,
            [] () { return TString("2001-01-01 01:02:03.1234"); },
            [] () { return TString("2001-01-01 01:02:03.12"); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            TIMESTAMPOID, "4",
            SUCCESS,
            [] () { return TString("2001-01-01 01:02:03.1234"); },
            [] () { return TString("2001-01-01 01:02:03.1234"); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            TIMESTAMPOID, "6",
            SUCCESS,
            [] () { return TString("2001-01-01 01:02:03.1234"); },
            [] () { return TString("2001-01-01 01:02:03.1234"); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },

        {
            TIMESTAMPTZOID, "2",
            SUCCESS,
            [] () { return TString("2001-01-01 01:02:03.1234+00"); },
            [] () { return TString("2001-01-01 01:02:03.12+00"); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            TIMESTAMPTZOID, "4",
            SUCCESS,
            [] () { return TString("2001-01-01 01:02:03.1234+00"); },
            [] () { return TString("2001-01-01 01:02:03.1234+00"); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            TIMESTAMPTZOID, "6",
            SUCCESS,
            [] () { return TString("2001-01-01 01:02:03.1234+00"); },
            [] () { return TString("2001-01-01 01:02:03.1234+00"); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },

        {
            INTERVALOID, "day",
            SUCCESS,
            [] () { return TString("100 01:02:03.1234"); },
            [] () { return TString("100 days"); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            INTERVALOID, "day to minute",
            SUCCESS,
            [] () { return TString("100 01:02:03.1234"); },
            [] () { return TString("100 days 01:02:00"); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            INTERVALOID, "day to second,2",
            SUCCESS,
            [] () { return TString("100 01:02:03.1234"); },
            [] () { return TString("100 days 01:02:03.12"); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            INTERVALOID, "day to second,4",
            SUCCESS,
            [] () { return TString("100 01:02:03.1234"); },
            [] () { return TString("100 days 01:02:03.1234"); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
        {
            INTERVALOID, "day to second,6",
            SUCCESS,
            [] () { return TString("100 01:02:03.1234"); },
            [] () { return TString("100 days 01:02:03.1234"); },
            [] (auto s) { return Sprintf("{\"%s\",\"%s\"}", s.c_str(), s.c_str()); }
        },
    };

#undef SUCCESS
#undef FAIL

    auto createTable = [] (
        NYdb::NTable::TTableClient& db,
        NYdb::NTable::TSession& session,
        ui32 typeId,
        bool isKey,
        bool isText,
        std::function<TString(size_t)> textIn,
        TString setTableName = "",
        ui16 rowCount = 10,
        TVector<TString> colNames = {"key", "value"}
    ) {
        auto* typeDesc = NPg::TypeDescFromPgTypeId(typeId);
        auto typeName = NPg::PgTypeNameFromTypeDesc(typeDesc);

        TTableBuilder builder;
        if (isKey) {
            builder.AddNullableColumn(colNames[0], TPgType(typeName));
        } else {
            builder.AddNullableColumn(colNames[0], TPgType("pgint2"));
        }
        builder.AddNullableColumn(colNames[1], TPgType(typeName));
        builder.SetPrimaryKeyColumn(colNames[0]);


        auto tableName = (setTableName.empty()) ?
            Sprintf("Pg%u_%s", typeId, isText ? "t" : "b") : setTableName;

        auto fullTableName = "/Root/" + tableName;
        auto result = session.CreateTable(fullTableName, builder.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < rowCount; ++i) {
            auto str = isText ? textIn(i) : NPg::PgNativeBinaryFromNativeText(textIn(i), typeId).Str;
            auto mode = isText ? TPgValue::VK_TEXT : TPgValue::VK_BINARY;
            if (isKey) {
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember(colNames[0]).Pg(TPgValue(mode, str, TPgType(typeName)))
                    .AddMember(colNames[1]).Pg(TPgValue(mode, str, TPgType(typeName)))
                    .EndStruct();
            } else {
                auto int2Str = NPg::PgNativeBinaryFromNativeText(Sprintf("%u", i), INT2OID).Str;
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember(colNames[0]).Pg(TPgValue(TPgValue::VK_BINARY, int2Str,  TPgType("pgint2")))
                    .AddMember(colNames[1]).Pg(TPgValue(mode, str, TPgType(typeName)))
                    .EndStruct();
            }
        }
        rows.EndList();

        result = db.BulkUpsert(fullTableName, rows.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto readSettings = TReadTableSettings()
            .AppendColumns(colNames[0])
            .AppendColumns(colNames[1]);

        auto it = session.ReadTable(fullTableName, readSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), result.GetIssues().ToString());
        return tableName;
    };

    auto createCoercionTable = [] (
        NYdb::NTable::TTableClient& db,
        NYdb::NTable::TSession& session,
        ui32 typeId,
        const TString& typeMod,
        std::function<TString()> textIn,
        size_t rowCount = 1
    ) {
        auto* typeDesc = NPg::TypeDescFromPgTypeId(typeId);
        auto typeName = NPg::PgTypeNameFromTypeDesc(typeDesc);

        auto paramsHash = THash<TString>()(typeMod);
        auto textHash = THash<TString>()(textIn());
        auto tableName = Sprintf("Coerce_%s_%" PRIu64 "_%" PRIu64,
            typeName.c_str(), paramsHash, textHash);
        auto fullTableName = "/Root/" + tableName;
        TTableBuilder builder;
        builder.AddNullableColumn("key", TPgType("pgint2"));
        builder.AddNullableColumn("value", TPgType(typeName, typeMod));
        builder.SetPrimaryKeyColumn("key");

        auto createResult = session.CreateTable(fullTableName, builder.Build()).GetValueSync();
        UNIT_ASSERT_C(createResult.IsSuccess(), createResult.GetIssues().ToString());

        auto describeResult = session.DescribeTable(fullTableName).GetValueSync();
        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());
        auto tableColumns = describeResult.GetTableDescription().GetTableColumns();
        for (const auto& column : tableColumns) {
            const auto& name = column.Name;
            const auto& type = column.Type;
            if (name == "value") {
                TTypeParser parser(type);
                auto pgType = parser.GetPg();
                UNIT_ASSERT(pgType.TypeName == typeName);
                UNIT_ASSERT(pgType.TypeModifier == typeMod);
            }
        }

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < rowCount; ++i) {
            auto str = NPg::PgNativeBinaryFromNativeText(textIn(), typeId).Str;
            auto int2Str = NPg::PgNativeBinaryFromNativeText(Sprintf("%u", i), INT2OID).Str;
            rows.AddListItem()
                .BeginStruct()
                .AddMember("key").Pg(TPgValue(TPgValue::VK_BINARY, int2Str, TPgType("pgint2")))
                .AddMember("value").Pg(TPgValue(TPgValue::VK_BINARY, str, TPgType(typeName, typeMod)))
                .EndStruct();
        }
        rows.EndList();

        auto upsertResult = db.BulkUpsert(fullTableName, rows.Build()).GetValueSync();
        if (!upsertResult.IsSuccess()) {
            Cerr << upsertResult.GetIssues().ToString() << Endl;
            return std::make_pair(tableName, false);
        }

        return std::make_pair(tableName, true);
    };


    Y_UNIT_TEST(CreateTableBulkUpsertAndRead) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (const TPgTypeTestSpec& spec, bool isText) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(db, session, spec.TypeId, spec.IsKey, isText, spec.TextIn);

            auto readSettings = TReadTableSettings()
                .AppendColumns("key")
                .AppendColumns("value");

            auto it = session.ReadTable("/Root/" + tableName, readSettings).GetValueSync();
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
                    auto check = [&parser, &spec, &i] (const TString& column, const TString& expected) {
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
            }

            session.Close().GetValueSync();
        };

        auto testType = [&] (const TPgTypeTestSpec& spec) {
            auto textInArray = [&spec] (auto i) {
                auto str = spec.TextIn(i);
                return spec.ArrayPrint(str);
            };

            auto textOutArray = [&spec] (auto i) {
                auto str = spec.TextOut(i);
                return spec.ArrayPrint(str);
            };

            auto arrayTypeId = NYql::NPg::LookupType(spec.TypeId).ArrayTypeId;
            TPgTypeTestSpec arraySpec{arrayTypeId, spec.IsKey, textInArray, textOutArray};

            testSingleType(spec, false);
            testSingleType(spec, true);
            testSingleType(arraySpec, false);
            testSingleType(arraySpec, true);
        };

        auto testByteaType = [&] () {
            testSingleType(typeByteaSpec, false);
            testSingleType(typeByteaSpec, true);
            testSingleType(typeByteaArraySpec, false);
            testSingleType(typeByteaArraySpec, true);
        };

        testByteaType();

        for (const auto& spec : typeSpecs) {
            testType(spec);
        }

        // TODO: varchar as a key
        // TODO: native range/multirange types (use get_range_io_data())
    }

    Y_UNIT_TEST(TypeCoercionBulkUpsert) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (const TPgTypeCoercionTestSpec& spec) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            TString tableName;
            bool success;
            std::tie(tableName, success) = createCoercionTable(db, session, spec.TypeId, spec.TypeMod, spec.TextIn);

            UNIT_ASSERT_VALUES_EQUAL(success, spec.ShouldPass);
            if (!success) {
                return;
            }

            ValidateTypeCoercionResult(session, tableName, spec);
            session.Close().GetValueSync();
        };

        auto testType = [&] (const TPgTypeCoercionTestSpec& spec) {
            auto textInArray = [&spec] () {
                auto str = spec.TextIn();
                return spec.ArrayPrint(str);
            };

            auto textOutArray = [&spec] () {
                auto str = spec.TextOut();
                return spec.ArrayPrint(str);
            };

            auto arrayTypeId = NYql::NPg::LookupType(spec.TypeId).ArrayTypeId;
            TPgTypeCoercionTestSpec arraySpec{arrayTypeId, spec.TypeMod, spec.ShouldPass, textInArray, textOutArray};

            testSingleType(spec);
            testSingleType(arraySpec);
        };

        for (const auto& spec : typeCoercionSpecs) {
            testType(spec);
        }

        TPgTypeCoercionTestSpec partialArrayCoerce{
            NUMERICARRAYOID, "2",
            false,
            [] () { return TString("{99,99,9999,99}"); },
            [] () { return TString(""); }
        };
        testSingleType(partialArrayCoerce);
    }

    Y_UNIT_TEST(TypeCoercionInsert) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (const TPgTypeCoercionTestSpec& spec) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            TString tableName;
            bool success;
            std::tie(tableName, success) = createCoercionTable(db, session, spec.TypeId, spec.TypeMod, spec.TextIn, 0);
            session.Close().GetValueSync();

            success = ExecutePgInsertForCoercion(kikimr, tableName, spec);

            UNIT_ASSERT_VALUES_EQUAL(success, spec.ShouldPass);
            if (!success) {
                return;
            }

            auto sessionValidate = db.CreateSession().GetValueSync().GetSession();
            ValidateTypeCoercionResult(sessionValidate, tableName, spec);
            sessionValidate.Close().GetValueSync();
        };

        auto testType = [&] (const TPgTypeCoercionTestSpec& spec) {
            auto textInArray = [&spec] () {
                auto str = spec.TextIn();
                return spec.ArrayPrint(str);
            };

            auto textOutArray = [&spec] () {
                auto str = spec.TextOut();
                return spec.ArrayPrint(str);
            };

            auto arrayTypeId = NYql::NPg::LookupType(spec.TypeId).ArrayTypeId;
            TPgTypeCoercionTestSpec arraySpec{arrayTypeId, spec.TypeMod, spec.ShouldPass, textInArray, textOutArray};

            testSingleType(spec);
            testSingleType(arraySpec);
        };

        for (const auto& spec : typeCoercionSpecs) {
            Cerr << spec.TypeId << Endl;
            testType(spec);
        }
    }

    Y_UNIT_TEST(EmptyQuery) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_pg
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        Y_ENSURE(result.GetResultSets().empty());
    }

    Y_UNIT_TEST(NoTableQuery) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_pg
            SELECT * FROM (VALUES
                (1, 'one'),
                (2, 'two'),
                (3, 'three')
            ) AS t (int8, varchar);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        CompareYson(R"([
            ["1";"one"];
            ["2";"two"];
            ["3";"three"]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(TableSelect) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (const TPgTypeTestSpec& spec) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(db, session, spec.TypeId, spec.IsKey, false, spec.TextIn);
            session.Close().GetValueSync();

            auto result = ExecutePgSelect(kikimr, tableName);
            ValidatePgYqlResult(result, spec);
        };

        auto testType = [&] (const TPgTypeTestSpec& spec) {
            auto textInArray = [&spec] (auto i) {
                auto str = spec.TextIn(i);
                return spec.ArrayPrint(str);
            };

            auto textOutArray = [&spec] (auto i) {
                auto str = spec.TextOut(i);
                return spec.ArrayPrint(str);
            };

            auto arrayTypeId = NYql::NPg::LookupType(spec.TypeId).ArrayTypeId;
            TPgTypeTestSpec arraySpec{arrayTypeId, spec.IsKey, textInArray, textOutArray};

            testSingleType(spec);
            testSingleType(arraySpec);
        };

        auto testByteaType = [&] () {
            testSingleType(typeByteaSpec);
            testSingleType(typeByteaArraySpec);
        };

        testByteaType();

        for (const auto& spec : typeSpecs) {
            Cerr << spec.TypeId << Endl;
            testType(spec);
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

        auto testSingleType = [&kikimr] (const TPgTypeTestSpec& spec) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(db, session, spec.TypeId, spec.IsKey, false, spec.TextIn, "", 0);
            session.Close().GetValueSync();

            ExecutePgInsert(kikimr, tableName, spec);
            auto result = ExecutePgSelect(kikimr, tableName);
            ValidatePgYqlResult(result, spec);
        };

        auto testByteaType = [&] () {
            testSingleType(typeByteaSpec);

            TPgTypeTestSpec typeByteaArraySpecForInsert{
                BYTEAARRAYOID, false, typeByteaArraySpec.TextIn, typeByteaArraySpec.TextOut};

            testSingleType(typeByteaArraySpecForInsert);
        };

        testByteaType();

        for (const auto& spec : typeSpecs) {
            Cerr << spec.TypeId << Endl;
            testSingleType(spec);
        }
    }

    Y_UNIT_TEST(TableArrayInsert) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (const TPgTypeTestSpec& spec) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto arrayId = NYql::NPg::LookupType(spec.TypeId).ArrayTypeId;
            auto tableName = createTable(db, session, arrayId, spec.IsKey, false, spec.TextIn, "", 0);
            session.Close().GetValueSync();

            ExecutePgArrayInsert(kikimr, tableName, spec);

            auto result = ExecutePgSelect(kikimr, tableName);

            TResultSetParser parser(result.GetResultSetParser(0));
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                auto check = [&parser] (const TString& column, const TString& expected) {
                    auto& c = parser.ColumnParser(column);
                    UNIT_ASSERT_VALUES_EQUAL(expected, c.GetPg().Content_);
                };
                TString expected = spec.TextOut(i);
                check("value", expected);
            }
        };

        auto testType = [&] (const TPgTypeTestSpec& spec) {
            auto textOutArray = [&spec] (auto i) {
                auto str = spec.TextOut(i);
                return spec.ArrayPrint(str);
            };
            TPgTypeTestSpec arraySpec{spec.TypeId, false, spec.TextIn, textOutArray};
            testSingleType(arraySpec);
        };

        for (const auto& spec : typeSpecs) {
            Cerr << spec.TypeId << Endl;
            testType(spec);
        }
    }

    Y_UNIT_TEST(InsertFromSelect) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (const TPgTypeTestSpec& spec) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(db, session, spec.TypeId, spec.IsKey, false, spec.TextIn, "", 10, {"key1", "value1"});
            TString emptyTableName = "PgEmpty" + ToString(spec.TypeId);
            createTable(db, session, spec.TypeId, spec.IsKey, false, spec.TextIn, emptyTableName, 0);
            session.Close().GetValueSync();

            db = kikimr.GetTableClient();
            session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(
                TStringBuilder() << R"(
                --!syntax_pg
                INSERT INTO )" << emptyTableName << " (key, value) SELECT * FROM \"" << tableName << "\";"
            , TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::INTERNAL_ERROR);
        };

        for (const auto& spec : typeSpecs) {
            Cerr << spec.TypeId << Endl;
            testSingleType(spec);
        }
    }

    Y_UNIT_TEST(V1CreateTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (const TPgTypeTestSpec& spec, bool isArray) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = "Pg" + ToString(spec.TypeId) + (isArray ? "array" : "");
            auto typeName = ((isArray) ? "_pg" : "pg") + NYql::NPg::LookupType(spec.TypeId).Name;
            auto keyEntry = spec.IsKey ? ("key "+ typeName) : "key pgint2";
            auto valueEntry = "value " + typeName;
            auto req = Sprintf("\
            CREATE TABLE `%s` (\n\
                %s,\n\
                %s,\n\
                PRIMARY KEY (key)\n\
            );", tableName.Data(), keyEntry.Data(), valueEntry.Data());
            Cerr << req << Endl;
            auto result = session.ExecuteSchemeQuery(req).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            if (!isArray) {
                ExecutePgInsert(kikimr, tableName, spec);
                auto result = ExecutePgSelect(kikimr, tableName);
                ValidatePgYqlResult(result, spec);
            } else {
                ExecutePgArrayInsert(kikimr, tableName, spec);
                auto result = ExecutePgSelect(kikimr, tableName);
                TResultSetParser parser(result.GetResultSetParser(0));
                for (size_t i = 0; parser.TryNextRow(); ++i) {
                    auto check = [&parser, &spec] (const TString& column, const TString& expected) {
                        auto& c = parser.ColumnParser(column);
                        UNIT_ASSERT_VALUES_EQUAL(expected, c.GetPg().Content_);
                    };
                    TString expected = spec.TextOut(i);
                    check("value", expected);
                }
            }
        };

        auto testType = [&] (const TPgTypeTestSpec& spec) {
            auto textOutArray = [&spec] (auto i) {
                auto str = spec.TextOut(i);
                return spec.ArrayPrint(str);
            };

            TPgTypeTestSpec arraySpec{spec.TypeId, false, spec.TextIn, textOutArray};

            testSingleType(spec, false);
            testSingleType(arraySpec, true);
        };

        for (const auto& spec : typeSpecs) {
            Cerr << spec.TypeId << Endl;
            if (spec.TypeId == CHAROID) {
                continue;
                // I cant come up with a query with explicit char conversion.
                // ::char, ::character casts to pg_bpchar
            }
            testType(spec);
        }
    }

    Y_UNIT_TEST(PgCreateTable) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (const TPgTypeTestSpec& spec, bool isArray) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = "Pg" + ToString(spec.TypeId) + (isArray ? "array" : "");
            auto typeName = ((isArray) ? "_" : "") + NYql::NPg::LookupType(spec.TypeId).Name;
            auto keyEntry = spec.IsKey ? ("key "+ typeName) : "key int2";
            auto valueEntry = "value " + typeName;
            auto req = Sprintf("\
            --!syntax_pg\n\
            CREATE TABLE %s (\n\
                %s PRIMARY KEY,\n\
                %s\n\
            );", tableName.Data(), keyEntry.Data(), valueEntry.Data());
            Cerr << req << Endl;
            auto result = session.ExecuteSchemeQuery(req).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            if (!isArray) {
                ExecutePgInsert(kikimr, tableName, spec);
                auto result = ExecutePgSelect(kikimr, tableName);
                ValidatePgYqlResult(result, spec);
            } else {
                ExecutePgArrayInsert(kikimr, tableName, spec);
                auto result = ExecutePgSelect(kikimr, tableName);
                TResultSetParser parser(result.GetResultSetParser(0));
                for (size_t i = 0; parser.TryNextRow(); ++i) {
                    auto check = [&parser, &spec] (const TString& column, const TString& expected) {
                        auto& c = parser.ColumnParser(column);
                        UNIT_ASSERT_VALUES_EQUAL(expected, c.GetPg().Content_);
                    };
                    TString expected = spec.TextOut(i);
                    check("value", expected);
                }
            }
        };

        auto testType = [&] (const TPgTypeTestSpec& spec) {
            auto textOutArray = [&spec] (auto i) {
                auto str = spec.TextOut(i);
                return spec.ArrayPrint(str);
            };

            TPgTypeTestSpec arraySpec{spec.TypeId, false, spec.TextIn, textOutArray};

            testSingleType(spec, false);
            testSingleType(arraySpec, true);
        };

        for (const auto& spec : typeSpecs) {
            Cerr << spec.TypeId << Endl;
            if (spec.TypeId == CHAROID) {
                continue;
                // I cant come up with a query with explicit char conversion.
                // ::char, ::character casts to pg_bpchar
            }
            testType(spec);
        }
    }

    Y_UNIT_TEST(DropTablePg) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            const auto query = Q_(R"(
                --!syntax_pg
                DROP TABLE Test;
                )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateTableSerialColumns) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false).SetEnableNotNullDataColumns(true));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE PgSerial (
                key serial PRIMARY KEY,
                value int2
                ))");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO PgSerial (value) values (101::int2);
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgSerial;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSetParser(0));
            ui32 rows = 0;
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                auto& c = parser.ColumnParser("key");
                Cerr << c.GetPg().Content_ << Endl;
                rows++;
            }

            UNIT_ASSERT_EQUAL(rows, static_cast<ui32>(1));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgSerial WHERE key = 1;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSetParser(0));
            ui32 rows = 0;
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                auto& c = parser.ColumnParser("key");
                Cerr << c.GetPg().Content_ << Endl;
                rows++;
            }

            UNIT_ASSERT_EQUAL(rows, static_cast<ui32>(1));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgSerial WHERE value = 101;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSetParser(0));
            ui32 rows = 0;
            for (size_t i = 0; parser.TryNextRow(); ++i) {
                auto& c = parser.ColumnParser("key");
                Cerr << c.GetPg().Content_ << Endl;
                rows++;
            }

            UNIT_ASSERT_EQUAL(rows, static_cast<ui32>(1));
        }
    }

    Y_UNIT_TEST(CreateNotNullPgColumn) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false).SetEnableNotNullDataColumns(true));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE Pg (
                key int2 PRIMARY KEY,
                value int2 NOT NULL
                ))");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_v1
                CREATE TABLE `PgV1` (
                key pg_int2,
                value pg_int2 NOT NULL,
                PRIMARY KEY (key)
                ))");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
     }

    Y_UNIT_TEST(ValuesInsert) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto testSingleType = [&kikimr] (const TPgTypeTestSpec& spec) {
            auto tableClient = kikimr.GetTableClient();
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(tableClient, session, spec.TypeId, spec.IsKey, true, spec.TextIn, "", 0);
            auto* typeDesc = NPg::TypeDescFromPgTypeId(spec.TypeId);
            auto typeName = NPg::PgTypeNameFromTypeDesc(typeDesc);
            auto keyType = spec.IsKey ? typeName : "pgint2";
            auto req = Sprintf("\
            --!syntax_v1\n\
            DECLARE $key0 as %s;\n\
            DECLARE $key1 as %s;\n\
            DECLARE $value0 as %s;\n\
            DECLARE $value1 as %s;\n\
            INSERT INTO `%s` (key, value) VALUES ($key0, $value0), ($key1, $value1);\n\
            ", keyType.c_str(), keyType.c_str(), typeName.c_str(), typeName.c_str(), tableName.c_str());
            Cerr << req << Endl;
            auto key0Value = TPgValue(TPgValue::VK_TEXT, spec.IsKey ? spec.TextIn(0) : "0", TPgType(keyType));
            auto key1Value = TPgValue(TPgValue::VK_TEXT, spec.IsKey ? spec.TextIn(1) : "1", TPgType(keyType));
            auto params = tableClient.GetParamsBuilder()
                .AddParam("$key0")
                    .Pg(key0Value)
                    .Build()
                .AddParam("$value0")
                    .Pg(TPgValue(TPgValue::VK_TEXT, spec.TextIn(0), TPgType(typeName)))
                    .Build()
                .AddParam("$key1")
                    .Pg(key1Value)
                    .Build()
                .AddParam("$value1")
                    .Pg(TPgValue(TPgValue::VK_TEXT, spec.TextIn(1), TPgType(typeName)))
                    .Build()
                .Build();
            auto result = session.ExecuteDataQuery(req, TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto selectResult = ExecutePgSelect(kikimr, tableName);
            ValidatePgYqlResult(selectResult, spec);
        };
        auto testType = [&] (const TPgTypeTestSpec& spec) {
            auto textInArray = [&spec] (auto i) {
                auto str = spec.TextIn(i);
                return spec.ArrayPrint(str);
            };

            auto textOutArray = [&spec] (auto i) {
                auto str = spec.TextOut(i);
                return spec.ArrayPrint(str);
            };

            auto arrayTypeId = NYql::NPg::LookupType(spec.TypeId).ArrayTypeId;
            TPgTypeTestSpec arraySpec{arrayTypeId, false, textInArray, textOutArray};
            testSingleType(spec);
            testSingleType(arraySpec);
        };

        for (const auto& spec : typeSpecs) {
            Cerr << spec.TypeId << Endl;
            testType(spec);
        }
    }

    Y_UNIT_TEST(TableDeleteAllData) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));

        auto testSingleType = [&kikimr] (const TPgTypeTestSpec& spec) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(db, session, spec.TypeId, spec.IsKey, false, spec.TextIn);
            session.Close().GetValueSync();

            {
                session = db.CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery(
                    TStringBuilder() << R"(
                    --!syntax_pg
                    DELETE FROM )" << tableName << ';'
                , TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            {
                auto result = ExecutePgSelect(kikimr, tableName);
                ui32 rows = ValidatePgYqlResult(result, spec, false);
                UNIT_ASSERT_C(!rows, "table is not empty");
            }
        };

        auto testType = [&] (const TPgTypeTestSpec& spec) {
            auto textInArray = [&spec] (auto i) {
                auto str = spec.TextIn(i);
                return spec.ArrayPrint(str);
            };

            auto textOutArray = [&spec] (auto i) {
                auto str = spec.TextOut(i);
                return spec.ArrayPrint(str);
            };

            auto arrayTypeId = NYql::NPg::LookupType(spec.TypeId).ArrayTypeId;
            TPgTypeTestSpec arraySpec{arrayTypeId, spec.IsKey, textInArray, textOutArray};

            testSingleType(spec);
            testSingleType(arraySpec);
        };

        auto testByteaType = [&] () {
            testSingleType(typeByteaSpec);
            testSingleType(typeByteaArraySpec);
        };

        testByteaType();

        for (const auto& spec : typeSpecs) {
            Cerr << spec.TypeId << Endl;
            testType(spec);
        }
    }

    Y_UNIT_TEST(TableDeleteWhere) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        int cnt = 0;
        auto testSingleType = [&kikimr, &cnt] (TPgTypeTestSpec spec) {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto tableName = createTable(db, session, spec.TypeId, spec.IsKey, /*isText=*/false, spec.TextIn, "", 2);
            if (!spec.IsKey && cnt) {
                return;
            }
            cnt += !spec.IsKey;
            {
                auto valType = NYql::NPg::LookupType(spec.TypeId).Name;
                if (spec.TypeId == CHAROID) {
                    valType = "\"char\"";
                }
                if (spec.TypeId == BITOID) {
                    valType.append("(4)");
                }
                TString keyType = (spec.IsKey) ? valType : "int2";
                TString keyIn = (spec.IsKey) ? spec.TextIn(1) : "1";

                session = db.CreateSession().GetValueSync().GetSession();
                TString req = Sprintf("\
                    --!syntax_pg\n\
                    DELETE FROM %s WHERE key = '%s'::%s", tableName.Data(), keyIn.Data(), keyType.Data());
                Cerr << req << Endl;
                auto result = session.ExecuteDataQuery(req, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
            {
                auto result = ExecutePgSelect(kikimr, tableName);
                ui32 rows = ValidatePgYqlResult(result, spec);
                Y_ENSURE(rows == 1, "incorrect rows size");
            }
        };

        auto testType = [&] (const TPgTypeTestSpec& spec) {
            auto textInArray = [&spec] (auto i) {
                auto str = spec.TextIn(i);
                return spec.ArrayPrint(str);
            };

            auto textOutArray = [&spec] (auto i) {
                auto str = spec.TextOut(i);
                return spec.ArrayPrint(str);
            };

            auto arrayTypeId = NYql::NPg::LookupType(spec.TypeId).ArrayTypeId;
            TPgTypeTestSpec arraySpec{arrayTypeId, spec.IsKey, textInArray, textOutArray};

            testSingleType(spec);
            testSingleType(arraySpec);
        };

        auto testByteaType = [&] () {
            testSingleType(typeByteaSpec);
            testSingleType(typeByteaArraySpec);
        };

        testByteaType();

        for (const auto& spec : typeSpecs) {
            Cerr << spec.TypeId << Endl;
            testType(spec);
        }
    }

    Y_UNIT_TEST(DeleteWithQueryService) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto client = kikimr.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE test (
                key int4 PRIMARY KEY,
                value int4
                ))");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                DELETE FROM test;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetResultSets().empty(), "results are not empty");
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM test;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(PgUpdate) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto client = kikimr.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE test (
                key int4 PRIMARY KEY,
                value int4
                ))");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO test (key, value) VALUES (120, 120), (121, 121), (122, 122), (123, 123);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                UPDATE test SET value = 122 WHERE key = 123;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM test;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"(
                [["120";"120"];["121";"121"];["122";"122"];["123";"122"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = db.ExecuteQuery(R"(
                UPDATE test SET key = key, value = 121 WHERE key = 123;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT(result.GetIssues().ToString().Contains("Cannot update primary key column: key"));
        }
        {
            auto result = db.ExecuteQuery(R"(
                UPDATE test SET key = 12 WHERE key = 123;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT(result.GetIssues().ToString().Contains("Cannot update primary key column: key"));
        }
        {
            auto result = db.ExecuteQuery(R"(
                UPDATE test SET value = key + 10;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {   
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM test;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"(
                [["120";"130"];["121";"131"];["122";"132"];["123";"133"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(PgUpdateCompoundKey) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto client = kikimr.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE test (
                key1 int4,
                key2 int4,
                value int4,
                primary key (key1, key2)
                ))");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO test (key1, key2, value) VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                UPDATE test SET key1 = 1, key2 = 2, value = 2 WHERE key1 = 1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT(result.GetIssues().ToString().Contains("Cannot update primary key column: key1"));
            UNIT_ASSERT(result.GetIssues().ToString().Contains("Cannot update primary key column: key2"));
        }
        {
            kikimr.GetTestClient().CreateTable("/Root", R"(
                Name: "PgTwoShard"
                Columns { Name: "key", Type: "pgint4", NotNull: true }
                Columns { Name: "value", Type: "pgint4" }
                KeyColumnNames: ["key"],
                SplitBoundary { KeyPrefix { Tuple { Optional { Text: "100" } } } }
            )");
            auto db = kikimr.GetTableClient();  
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto describeResult = session.DescribeTable(
                "/Root/PgTwoShard",
                TDescribeTableSettings().WithTableStatistics(true).WithKeyShardBoundary(true)
            ).GetValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());   
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetTableDescription().GetPartitionsCount(), 2);
        }  
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO PgTwoShard (key, value) VALUES (10, 10), (110, 110);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                UPDATE PgTwoShard SET value = key + 1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {   
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM PgTwoShard ORDER BY key;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"(
                [["10";"11"];["110";"111"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
