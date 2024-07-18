#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <util/system/env.h>


extern "C" {
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
            true,
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
            UNIT_ASSERT(!str.empty());
            auto mode = isText ? TPgValue::VK_TEXT : TPgValue::VK_BINARY;
            if (isKey) {
                rows.AddListItem()
                    .BeginStruct()
                    .AddMember(colNames[0]).Pg(TPgValue(mode, str, TPgType(typeName)))
                    .AddMember(colNames[1]).Pg(TPgValue(mode, str, TPgType(typeName)))
                    .EndStruct();
            } else {
                auto int2Str = NPg::PgNativeBinaryFromNativeText(Sprintf("%u", i), INT2OID).Str;
                UNIT_ASSERT(!int2Str.empty());
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
            UNIT_ASSERT(!str.empty());
            UNIT_ASSERT(!int2Str.empty());
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

    Y_UNIT_TEST(InsertFromSelect_Simple) {
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
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            result = ExecutePgSelect(kikimr, emptyTableName);
            ValidatePgYqlResult(result, spec);
        };

        for (const auto& spec : typeSpecs) {
            Cerr << spec.TypeId << Endl;
            testSingleType(spec);
        }
    }

    Y_UNIT_TEST(InsertFromSelect_NoReorder) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
         {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t (
                    columnA      int4 PRIMARY KEY,
                    columnB      int4 NOT NULL
            ))", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t SELECT 1 as columnC, 2 as columnA;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t (columnA, columnB) SELECT 2 as columnB, 4 as columnA;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            Cerr << "Result:\n" << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            CompareYson(R"(
                [["1";"2"];["2";"4"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InsertFromSelect_Serial) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
         {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE movies (
                    id          serial PRIMARY KEY,
                    title       text NOT NULL
            ))", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE movies_2 (
                id          serial PRIMARY KEY,
                title       text NOT NULL
            ))", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO movies (id, title) VALUES (1, 'movie1'), (4, 'movie4');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM movies;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"movie1"];["4";"movie4"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO movies_2 (title)
                SELECT title
                FROM movies;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM movies_2;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"movie1"];["2";"movie4"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
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

    Y_UNIT_TEST(Returning) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(serverSettings);

        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        const auto queryCreate = Q_(R"(
            --!syntax_pg
            CREATE TABLE ReturningTable (
            key serial PRIMARY KEY,
            value int4);

            CREATE TABLE ReturningTableExtraValue (
            key serial PRIMARY KEY,
            value int4,
            value2 int4 default 2);
            )");

        auto resultCreate = session.ExecuteSchemeQuery(queryCreate).GetValueSync();
        UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO ReturningTable (value) VALUES(2) RETURNING key;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());

            CompareYson(R"([["1"]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO ReturningTable (value) VALUES(2) RETURNING key, value;
                INSERT INTO ReturningTableExtraValue (value) VALUES(3) RETURNING key, value;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([["2";"2"]])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([["1";"3"]])", FormatResultSetYson(result.GetResultSet(1)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO ReturningTable (value) VALUES(2) RETURNING *;
                INSERT INTO ReturningTableExtraValue (value) VALUES(4) RETURNING *;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([["3";"2"]])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([["2";"4";"2"]])", FormatResultSetYson(result.GetResultSet(1)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO ReturningTable (value) VALUES(2) RETURNING fake;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            Cerr << result.GetIssues().ToString(true) << Endl;
            UNIT_ASSERT(result.GetIssues().ToString(true) == "{ <main>: Error: Type annotation, code: 1030 subissue: { <main>:1:1: Error: At function: DataQueryBlocks, At function: TKiDataQueryBlock, At function: KiReturningList! subissue: { <main>:1:1: Error: Column not found: fake } } }");
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                UPDATE ReturningTable SET  value = 3 where key = 1 RETURNING *;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([["1";"3"]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                UPDATE ReturningTableExtraValue SET  value2 = 3 where key = 2 RETURNING *;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([["2";"4";"3"]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                DELETE FROM ReturningTableExtraValue WHERE key = 2 RETURNING key, value, value2;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([["2";"4";"3"]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                DELETE FROM ReturningTable WHERE key <= 3 RETURNING key, value;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());
            CompareYson(R"([["2";"2"];["3";"2"];["1";"3"]])", FormatResultSetYson(result.GetResultSet(0)));
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

    Y_UNIT_TEST(DropTablePgMultiple) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE FirstTable (
                    key int4 PRIMARY KEY,
                    val int4
                );
                )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE SecondTable (
                    key int4 PRIMARY KEY,
                    val int4
                );
                )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            const auto query = Q_(R"(
                --!syntax_pg
                DROP TABLE FirstTable, SecondTable;
                )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateTableSerialColumns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);
        TKikimrRunner kikimr(serverSettings);
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

    Y_UNIT_TEST(CopyTableSerialColumns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false)
            .SetEnableNotNullDataColumns(true);
        TKikimrRunner kikimr(serverSettings);
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
                INSERT INTO PgSerial (value) values (1);
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto result = session.CopyTable("/Root/PgSerial", "/Root/copy").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto desc = session.DescribeTable("/Root/copy").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT * FROM copy;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"1"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO copy (value) values (1);
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT * FROM copy;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"1"];["2";"1"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgSerial;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"1"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO PgSerial (value) values (1);
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT * FROM copy;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"1"];["2";"1"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgSerial;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"1"];["2";"1"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(CreateIndex) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false));

        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        const auto txCtrl = NYdb::NQuery::TTxControl::NoTx();
        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE test(
                    id int8,
                    fk int8,
                    value char,
                    primary key(id)
                );
                CREATE INDEX "test_fk_idx" ON test (fk);
                CREATE INDEX "test_fk_idx_cover" ON test (fk) INCLUDE(value);
                )");

            auto result = session.ExecuteQuery(query, txCtrl).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE test2(
                    id int8,
                    fk int8,
                    primary key(id)
                );
                CREATE INDEX "test_fk_idx" ON test2 (fk);
                )");

            auto result = session.ExecuteQuery(query, txCtrl).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE INDEX "test_fk_idx" ON test (fk);
                )");

            auto result = session.ExecuteQuery(query, txCtrl).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE INDEX "test" ON test (fk);
                )");

            auto result = session.ExecuteQuery(query, txCtrl).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE INDEX IF NOT EXISTS "test_fk_idx" ON test (fk);
                )");

            auto result = session.ExecuteQuery(query, txCtrl).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE INDEX IF NOT EXISTS "test" ON test (fk);
                )");

            auto result = session.ExecuteQuery(query, txCtrl).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(SelectIndex) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false));

        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE test(
                    id int4,
                    fk int4,
                    value int4,
                    primary key(id)
                );
                CREATE INDEX "test_fk_idx" ON test (fk);
                CREATE INDEX "test_fk_idx_cover" ON test (fk) INCLUDE(value);
                )");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO test (id, fk, value) VALUES (1, 2, 5), (2, 3, 6);
                )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT id, fk, value FROM test where fk = 2;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).GetValueSync();
            UNIT_ASSERT(result.IsSuccess());

            CompareYson(R"([["1";"2";"5"]])", FormatResultSetYson(result.GetResultSet(0)));
            AssertTableStats(result, "/Root/test/test_fk_idx_cover/indexImplTable", {
                .ExpectedReads = 1,
            });
        }
    }

    Y_UNIT_TEST(DropIndex) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));

        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        const auto txCtrl = NYdb::NQuery::TTxControl::NoTx();

        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE test2(
                    id int8,
                    fk int8,
                    value char,
                    primary key(id)
                );
                CREATE INDEX "test2_fk_idx" ON test2 (fk);
                CREATE INDEX "test2_fk.idx_cover" ON test2 (fk) INCLUDE(value);
                )");

            auto result = session.ExecuteQuery(query, txCtrl).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            const auto query = Q_(R"(
                --!syntax_pg
                DROP INDEX "test2_fk_idx";
                )");

            auto result = session.ExecuteQuery(query, txCtrl).ExtractValueSync();
            // TODO: KIKIMR-19695
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        }
        // TODO: test with <schema>.<name>: "DROP INDEX test2_fk.idx_cover;"
    }

    Y_UNIT_TEST(CreateUniqPgColumn) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE Pg (
                key int4 PRIMARY KEY,
                value int4 UNIQUE
                ))");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE Pg1 (
                key int4 PRIMARY KEY,
                value int4,
                UNIQUE(value, key)
                ))");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO Pg (key, value) VALUES (120, 120), (121, 120);
                )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO Pg (key, value) VALUES (120, 120), (121, 121);
                )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                UPDATE Pg SET value = 120 WHERE key = 121;
                )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_pg
                SELECT * FROM Pg;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([["120";"120"];["121";"121"]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO Pg1 (key, value) VALUES (120, 120), (121, 120);
                )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_pg
                SELECT * FROM Pg1;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([["120";"120"];["121";"120"]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                UPDATE Pg1 SET value = 121 WHERE key = 121;
                )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_pg
                SELECT * FROM Pg1;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([["120";"120"];["121";"121"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(CreateUniqComplexPgColumn) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE Pg (
                key int4 PRIMARY KEY,
                value1 int4,
                value2 int4,
                UNIQUE(value1, value2)
                ))");

            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO Pg (key, value1) VALUES (120, 120), (121, 120);
                )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_pg
                SELECT * FROM Pg;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([["120";"120";#];["121";"120";#]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                UPDATE Pg SET value2 = 111 WHERE key = 120;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_pg
                SELECT * FROM Pg;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([["120";"120";"111"];["121";"120";#]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                UPDATE Pg SET value2 = 111 WHERE key = 121;
            )");

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_pg
                SELECT * FROM Pg;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([["120";"120";"111"];["121";"120";#]])", FormatResultSetYson(result.GetResultSet(0)));
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

    Y_UNIT_TEST(CreateTempTable) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false).SetEnableTempTables(true));
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        auto client = kikimr.GetQueryClient();
        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryCreate = Q_(R"(
                --!syntax_pg
                CREATE TEMP TABLE PgTemp (
                key int2 PRIMARY KEY,
                value int2))");

            auto resultCreate = session.ExecuteQuery(queryCreate, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

            const auto querySelect = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgTemp;
            )");

            auto resultSelect = session.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultSelect.IsSuccess(), resultSelect.GetIssues().ToString());

            bool allDoneOk = true;
            NTestHelpers::CheckDelete(clientConfig, id, Ydb::StatusIds::SUCCESS, allDoneOk);

            UNIT_ASSERT(allDoneOk);
        }

        {
            const auto querySelect = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgTemp;
            )");

            auto resultSelect = client.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!resultSelect.IsSuccess());
        }
    }

    Y_UNIT_TEST(CreateTempTableSerial) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false).SetEnableTempTables(true));
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        auto client = kikimr.GetQueryClient();
        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryCreate = Q_(R"(
                --!syntax_pg
                CREATE TEMP TABLE PgTemp (
                key serial PRIMARY KEY,
                value int2))");

            auto resultCreate = session.ExecuteQuery(queryCreate, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

            const auto querySelect = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgTemp;
            )");

            auto resultSelect = session.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultSelect.IsSuccess(), resultSelect.GetIssues().ToString());

            bool allDoneOk = true;
            NTestHelpers::CheckDelete(clientConfig, id, Ydb::StatusIds::SUCCESS, allDoneOk);

            UNIT_ASSERT(allDoneOk);
        }

        {
            const auto querySelect = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgTemp;
            )");

            auto resultSelect = client.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!resultSelect.IsSuccess());
        }
    }

    Y_UNIT_TEST(CreateSequence) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false));
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        auto client = kikimr.GetQueryClient();
        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryCreate = R"(
                --!syntax_pg
                CREATE SEQUENCE IF NOT EXISTS seq
                    AS bigint
                    START WITH 10
                    INCREMENT BY 2
                    MINVALUE 1
                    NO MAXVALUE
                    CACHE 3
                    CYCLE;
            )";

            auto resultCreate = session.ExecuteQuery(queryCreate, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());
        }

        {
            auto runtime = kikimr.GetTestServer().GetRuntime();
            TActorId sender = runtime->AllocateEdgeActor();
            auto describeResult = DescribeTable(&kikimr.GetTestServer(), sender, "/Root/seq");
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetStatus(), NKikimrScheme::StatusSuccess);
            auto& sequenceDescription = describeResult.GetPathDescription().GetSequenceDescription();
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetName(), "seq");
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetMinValue(), 1);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetMaxValue(), Max<i64>());
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetStartValue(), 10);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetCache(), 3);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetIncrement(), 2);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetCycle(), true);
        }
    }

    Y_UNIT_TEST(DropSequence) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false));
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        auto client = kikimr.GetQueryClient();
        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryCreate = R"(
                --!syntax_pg
                CREATE SEQUENCE IF NOT EXISTS seq
                    AS bigint
                    START WITH 10
                    INCREMENT BY 2
                    MINVALUE 1
                    NO MAXVALUE
                    CACHE 3
                    CYCLE;
            )";

            auto resultCreate = session.ExecuteQuery(queryCreate, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());
        }

        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            TDescribeTableResult describe = session.DescribeTable("/Root/seq").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryDrop = R"(
                --!syntax_pg
                DROP SEQUENCE seq;
            )";

            auto resultDrop = session.ExecuteQuery(queryDrop, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultDrop.IsSuccess(), resultDrop.GetIssues().ToString());
        }

        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            TDescribeTableResult describe = session.DescribeTable("/Root/seq").GetValueSync();
            UNIT_ASSERT(!describe.IsSuccess());
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryDrop = R"(
                --!syntax_pg
                DROP SEQUENCE seq;
            )";

            auto resultDrop = session.ExecuteQuery(queryDrop, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!resultDrop.IsSuccess());
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryDrop = R"(
                --!syntax_pg
                DROP SEQUENCE IF EXISTS seq;
            )";

            auto resultDrop = session.ExecuteQuery(queryDrop, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultDrop.IsSuccess(), resultDrop.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterSequence) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false));
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        auto client = kikimr.GetQueryClient();
        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryCreate = R"(
                --!syntax_pg
                CREATE SEQUENCE IF NOT EXISTS seq
                    as integer
                    START WITH 10
                    INCREMENT BY 2
                    MINVALUE 1
                    CACHE 3
                    CYCLE;
            )";

            auto resultCreate = session.ExecuteQuery(queryCreate, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());
        }

        {
            auto runtime = kikimr.GetTestServer().GetRuntime();
            TActorId sender = runtime->AllocateEdgeActor();
            auto describeResult = DescribeTable(&kikimr.GetTestServer(), sender, "/Root/seq");
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetStatus(), NKikimrScheme::StatusSuccess);
            auto& sequenceDescription = describeResult.GetPathDescription().GetSequenceDescription();
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetName(), "seq");
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetMinValue(), 1);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetMaxValue(), Max<i32>());
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetStartValue(), 10);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetCache(), 3);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetIncrement(), 2);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetCycle(), true);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetDataType(), "pgint4");
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryAlter = R"(
                --!syntax_pg
                ALTER SEQUENCE IF EXISTS seq
                    as smallint
                    START WITH 20
                    INCREMENT BY 5
                    MAXVALUE 30
                    NO CYCLE;
            )";

            auto resultAlter = session.ExecuteQuery(queryAlter, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultAlter.IsSuccess(), resultAlter.GetIssues().ToString());
        }

        {
            auto runtime = kikimr.GetTestServer().GetRuntime();
            TActorId sender = runtime->AllocateEdgeActor();
            auto describeResult = DescribeTable(&kikimr.GetTestServer(), sender, "/Root/seq");
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetStatus(), NKikimrScheme::StatusSuccess);
            auto& sequenceDescription = describeResult.GetPathDescription().GetSequenceDescription();
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetName(), "seq");
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetMinValue(), 1);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetMaxValue(), 30);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetStartValue(), 20);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetCache(), 3);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetIncrement(), 5);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetCycle(), false);
            UNIT_ASSERT_VALUES_EQUAL(sequenceDescription.GetDataType(), "pgint2");
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryAlter = R"(
                --!syntax_pg
                ALTER SEQUENCE IF EXISTS seq
                    MAXVALUE 65000;
            )";

            auto resultAlter = session.ExecuteQuery(queryAlter, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!resultAlter.IsSuccess());
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryAlter = R"(
                --!syntax_pg
                ALTER SEQUENCE IF EXISTS seq
                    START WITH 31;
            )";

            auto resultAlter = session.ExecuteQuery(queryAlter, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!resultAlter.IsSuccess());
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryAlter = R"(
                --!syntax_pg
                ALTER SEQUENCE IF EXISTS seq
                    MAXVALUE 32000;
            )";

            auto resultAlter = session.ExecuteQuery(queryAlter, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultAlter.IsSuccess(), resultAlter.GetIssues().ToString());
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryAlter = R"(
                --!syntax_pg
                ALTER SEQUENCE IF EXISTS seq
                    as bigint;
            )";

            auto resultAlter = session.ExecuteQuery(queryAlter, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultAlter.IsSuccess(), resultAlter.GetIssues().ToString());
        }

        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryAlter = R"(
                --!syntax_pg
                ALTER SEQUENCE IF EXISTS seq
                    as integer;
                    MAXVALUE 2147483647;
            )";

            auto resultAlter = session.ExecuteQuery(queryAlter, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!resultAlter.IsSuccess());
        }

         {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryAlter = R"(
                --!syntax_pg
                ALTER SEQUENCE IF EXISTS seq
                    MAXVALUE 2147483647;
            )";

            auto resultAlter = session.ExecuteQuery(queryAlter, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultAlter.IsSuccess(), resultAlter.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterColumnSetDefaultFromSequence) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false));
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        auto client = kikimr.GetQueryClient();

        auto session = client.GetSession().GetValueSync().GetSession();

        auto tableClient = kikimr.GetTableClient();
        auto tableClientSession = tableClient.CreateSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteQuery(R"(
                --!syntax_pg
                CREATE TABLE Pg (
                    key int8 PRIMARY KEY,
                    value int8
                );
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO Pg (key, value) values (1, 1);
            )");

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        } 

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT * FROM Pg;
            )");

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"1"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = session.ExecuteQuery(R"(
                --!syntax_pg
                ALTER TABLE Pg ALTER COLUMN key SET DEFAULT nextval('seq');
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
        }

        {
            const auto queryCreate = R"(
                --!syntax_pg
                CREATE SEQUENCE IF NOT EXISTS seq1
                    START WITH 10
                    INCREMENT BY 2
                    MINVALUE 1
                    CACHE 3
                    CYCLE;
            )";

            auto resultCreate = session.ExecuteQuery(queryCreate, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(R"(
                --!syntax_pg
                ALTER TABLE Pg ALTER COLUMN key SET DEFAULT nextval('seq1');
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto runtime = kikimr.GetTestServer().GetRuntime();
            TActorId sender = runtime->AllocateEdgeActor();
            auto describeResult = DescribeTable(&kikimr.GetTestServer(), sender, "/Root/Pg");
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetStatus(), NKikimrScheme::StatusSuccess);
            const auto& tableDescription = describeResult.GetPathDescription().GetTable();

            for (const auto& column: tableDescription.GetColumns()) {
                if (column.GetName() == "key") {
                    UNIT_ASSERT(column.HasDefaultFromSequence());
                    UNIT_ASSERT(column.GetDefaultFromSequence() == "/Root/seq1");
                    break;
                }
            }
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO Pg (value) values (2), (3);
            )");

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        } 

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT * FROM Pg;
            )");

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"1"];["10";"2"];["12";"3"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto queryCreate = R"(
                --!syntax_pg
                CREATE SEQUENCE IF NOT EXISTS seq2
                    START WITH 5
                    INCREMENT BY 3
                    MINVALUE 1
                    CACHE 3
                    CYCLE;
            )";

            auto resultCreate = session.ExecuteQuery(queryCreate, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(R"(
                --!syntax_pg
                ALTER TABLE Pg ALTER COLUMN key SET DEFAULT nextval('seq2');
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO Pg (value) values (4), (5);
            )");

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        } 

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT * FROM Pg;
            )");

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"1"];["5";"4"];["8";"5"];["10";"2"];["12";"3"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = session.ExecuteQuery(R"(
                --!syntax_pg
                ALTER TABLE Pg ALTER COLUMN key DROP DEFAULT;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(R"(
                --!syntax_pg
                ALTER TABLE Pg ALTER COLUMN value SET DEFAULT nextval('seq1');
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = Q_(R"(
                --!syntax_pg
                INSERT INTO Pg (key) values (13), (14);
            )");

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        } 

        {
            const auto query = Q_(R"(
                --!syntax_pg
                SELECT * FROM Pg;
            )");

            auto result = tableClientSession.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"1"];["5";"4"];["8";"5"];["10";"2"];["12";"3"];["13";"14"];["14";"16"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(TempTablesSessionsIsolation) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false).SetEnableTempTables(true));
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        auto client = kikimr.GetQueryClient();

        auto session = client.GetSession().GetValueSync().GetSession();
        auto id = session.GetId();

        const auto queryCreate = Q_(R"(
            --!syntax_pg
            CREATE TEMP TABLE PgTemp (
            key int2 PRIMARY KEY,
            value int2))");

        auto resultCreate = session.ExecuteQuery(queryCreate, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

        const auto querySelect = Q_(R"(
            --!syntax_pg
            SELECT * FROM PgTemp;
        )");

        auto resultSelect = session.ExecuteQuery(
            querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(resultSelect.IsSuccess(), resultSelect.GetIssues().ToString());

        bool allDoneOk = true;
        NTestHelpers::CheckDelete(clientConfig, id, Ydb::StatusIds::SUCCESS, allDoneOk);

        UNIT_ASSERT(allDoneOk);

        auto sessionAnother = client.GetSession().GetValueSync().GetSession();
        auto idAnother = sessionAnother.GetId();
        UNIT_ASSERT(id != idAnother);

        const auto querySelectAnother = Q_(R"(
            --!syntax_pg
            SELECT * FROM PgTemp;
        )");

        auto resultSelectAnother = sessionAnother.ExecuteQuery(
            querySelectAnother, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(!resultSelectAnother.IsSuccess());
    }

    Y_UNIT_TEST(TempTablesDrop) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false).SetEnableTempTables(true));
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        auto client = kikimr.GetQueryClient();

        auto session = client.GetSession().GetValueSync().GetSession();
        auto id = session.GetId();

        const auto queryCreate = Q_(R"(
            --!syntax_pg
            CREATE TEMP TABLE PgTemp (
            key int2 PRIMARY KEY,
            value int2))");

        auto resultCreate = session.ExecuteQuery(queryCreate, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

        {
            const auto querySelect = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgTemp;
            )");

            auto resultSelect = session.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultSelect.IsSuccess(), resultSelect.GetIssues().ToString());
        }

        const auto queryDrop = Q_(R"(
            --!syntax_pg
            DROP TABLE PgTemp;
        )");

        auto resultDrop = session.ExecuteQuery(
            queryDrop, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(resultDrop.IsSuccess(), resultDrop.GetIssues().ToString());

        {
            const auto querySelect = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgTemp;
            )");

            auto resultSelect = session.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!resultSelect.IsSuccess());
        }

        bool allDoneOk = true;
        NTestHelpers::CheckDelete(clientConfig, id, Ydb::StatusIds::SUCCESS, allDoneOk);

        UNIT_ASSERT(allDoneOk);

        auto sessionAnother = client.GetSession().GetValueSync().GetSession();
        auto idAnother = sessionAnother.GetId();
        UNIT_ASSERT(id != idAnother);

        {
            const auto querySelect = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgTemp;
            )");

            auto resultSelect = sessionAnother.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!resultSelect.IsSuccess());
        }
    }

    Y_UNIT_TEST(TempTablesWithCache) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false).SetEnableTempTables(true));
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        auto client = kikimr.GetQueryClient();

        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::Pg)
            .StatsMode(NYdb::NQuery::EStatsMode::Basic);
        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();
            {
                const auto query = Q_(R"(
                    --!syntax_pg
                    CREATE TABLE PgTemp (
                    key int2 PRIMARY KEY,
                    value int2))");

                auto result =
                    session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    --!syntax_pg
                    DROP TABLE PgTemp;
                )");

                auto result =
                    session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    --!syntax_pg
                    CREATE TABLE PgTemp (
                    key int2 PRIMARY KEY,
                    value int2))");

                auto result =
                    session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

                auto resultInsert = session.ExecuteQuery(R"(
                    INSERT INTO PgTemp VALUES(1, 1);
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    resultInsert.GetStatus(), EStatus::SUCCESS, resultInsert.GetIssues().ToString());
            }

            {
                const auto query = Q_(R"(
                    --!syntax_pg
                    CREATE TABLE SimpleTable (
                    key int2 PRIMARY KEY,
                    value int2))");

                auto result =
                    session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    --!syntax_pg
                    SELECT * FROM PgTemp;
                )");

                auto result = session.ExecuteQuery(
                    query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    --!syntax_pg
                    SELECT * FROM SimpleTable;
                )");

                auto result = session.ExecuteQuery(
                    query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    --!syntax_pg
                    CREATE TEMP TABLE PgTemp (
                    key int2 PRIMARY KEY,
                    value int2))");

                auto result =
                    session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

                auto resultInsert = session.ExecuteQuery(R"(
                    INSERT INTO PgTemp VALUES(2, 2);
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    resultInsert.GetStatus(), EStatus::SUCCESS, resultInsert.GetIssues().ToString());
            }

            {
                const auto query = Q_(R"(
                    --!syntax_pg
                    SELECT * FROM PgTemp;
                )");

                auto result = session.ExecuteQuery(
                    query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

                UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
                CompareYson(R"(
                    [["2";"2"]]
                )", FormatResultSetYson(result.GetResultSet(0)));
            }

            {
                const auto query = Q_(R"(
                    --!syntax_pg
                    SELECT * FROM SimpleTable;
                )");

                auto result = session.ExecuteQuery(
                    query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
            }

            {
                const auto query = Q_(R"(
                    --!syntax_pg
                    DROP TABLE PgTemp;
                )");

                auto result =
                    session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    --!syntax_pg
                    SELECT * FROM PgTemp;
                )");

                auto result = session.ExecuteQuery(
                    query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);

                UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
                CompareYson(R"(
                    [["1";"1"]]
                )", FormatResultSetYson(result.GetResultSet(0)));
            }

            {
                const auto query = Q_(R"(
                    --!syntax_pg
                    DROP TABLE PgTemp;
                )");

                auto result =
                    session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            bool allDoneOk = true;
            NTestHelpers::CheckDelete(clientConfig, id, Ydb::StatusIds::SUCCESS, allDoneOk);

            UNIT_ASSERT(allDoneOk);
        }

        {
            const auto querySelect = Q_(R"(
                --!syntax_pg
                SELECT * FROM PgTemp;
            )");

            auto resultSelect = client.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!resultSelect.IsSuccess());
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

    Y_UNIT_TEST(CreateTableIfNotExists_GenericQuery) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting})
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE test (
                    id int2 primary key
                );
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE test (
                    id int4 primary key
                );
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
        }
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE IF NOT EXISTS test (
                    id int4 primary key
                );
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM test;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                DROP TABLE IF EXISTS test;
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM test;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToString().Contains("Cannot find table 'db.[/Root/test]'"));
        }
    }

    Y_UNIT_TEST(DropTableIfExists) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(R"(
                --!syntax_pg
                DROP TABLE IF EXISTS test;
            )").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(R"(
                --!syntax_pg
                CREATE TABLE test (
                    id int4 primary key
                );
            )").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto db = kikimr.GetQueryClient();
            auto settings = NYdb::NQuery::TExecuteQuerySettings()
                .Syntax(NYdb::NQuery::ESyntax::Pg);
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM test;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(R"(
                --!syntax_pg
                DROP TABLE IF EXISTS test;
            )").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto db = kikimr.GetQueryClient();
            auto settings = NYdb::NQuery::TExecuteQuerySettings()
                .Syntax(NYdb::NQuery::ESyntax::Pg);
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM test;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToString().Contains("Cannot find table 'db.[/Root/test]'"));
        }
    }

    Y_UNIT_TEST(DropTableIfExists_GenericQuery) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting})
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                DROP TABLE IF EXISTS test;
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE test (
                    id int4 primary key
                );
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM test;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                DROP TABLE IF EXISTS test;
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM test;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToString().Contains("Cannot find table 'db.[/Root/test]'"));
        }
    }

    Y_UNIT_TEST_TWIN(JoinWithQueryService, StreamLookup) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamIdxLookupJoin(StreamLookup);
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(serverSettings);
        auto client = kikimr.GetTableClient();
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE t1(
                id1 int4 PRIMARY KEY,
                val1 text
                ))");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto query = Q_(R"(
                --!syntax_pg
                CREATE TABLE t2(
                id2 int4 PRIMARY KEY NOT NULL,
                val2 text NOT NULL
                ))");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t1(id1, val1) VALUES (1, 'val1');
                INSERT INTO t2(id2, val2) VALUES (1, 'val2');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t1 JOIN t2 ON t1.id1 = t2.id2;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([["1";"val1";"1";"val2"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(EquiJoin) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto client = kikimr.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            const auto query = Q_(R"_(
                --!syntax_pg
                    CREATE TABLE left_table(id int4, val text, primary key(id));

                    CREATE TABLE right_table(id int4, val2 text, primary key(id));
                )_");
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO left_table (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c');
                INSERT INTO right_table (id, val2) VALUES (1, 'd'), (2, 'e'), (3, 'f');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = db.ExecuteQuery(R"(
                SELECT left_table.*, right_table.val2 FROM left_table, right_table
                WHERE left_table.id=right_table.id
                ORDER BY left_table.id
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"a";"d"];["2";"b";"e"];["3";"c";"f"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(PgAggregate) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto tc = kikimr.GetTableClient();
            auto session = tc.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(R"(
                --!syntax_pg
                CREATE TABLE t(id int8 not null, primary key(id));
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t(id) VALUES(1::int8), (2::int8), (3::int8);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT sum(id) FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([["6"]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InsertNoTargetColumns_Simple) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t(columnA int4, columnB text, primary key(columnA));
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"a"];["2";"b"];["3";"c"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InsertNoTargetColumns_ColumnOrder) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
            auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t(columnC int4, columnA text, columnB text, primary key(columnC));
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES (1, 'a', 'b'), (2, 'c', 'd'), (3, 'e', 'f');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"a";"b"];["2";"c";"d"];["3";"e";"f"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InsertNoTargetColumns_NotOneSize) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
                auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t(id int4, value text, primary key(id));
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = db.ExecuteQuery(R"(
                CREATE TABLE nopg(id uint64, primary key(id));
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES (1);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";#]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES (1, 'a', 'a');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToString().Contains("values have 3 columns, INSERT INTO expects: 2"));
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES ('a', 1);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToString().Contains("invalid input syntax for type integer: \"a\""));
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO nopg VALUES ('a');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToString().Contains("Failed to convert 'id': pgunknown to Optional<Uint64>"));
        }
    }

    Y_UNIT_TEST(InsertNoTargetColumns_Alter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto tableClient = kikimr.GetTableClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t(columnB int4, columnA int4, columnC text, primary key(columnA));
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(R"(
                ALTER TABLE t DROP COLUMN columnB;
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES (3, 'c');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"a"];["2";"b"];["3";"c"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(Insert_Serial) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
         {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE movies (
                    id          serial PRIMARY KEY,
                    title       text NOT NULL
            ))", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO movies VALUES (1, 'movie1'), (4, 'movie4');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM movies;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"movie1"];["4";"movie4"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InsertNoTargetColumns_Serial) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t (a INT, b SERIAL PRIMARY KEY);
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES(1);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"1"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InsertValuesFromTableWithDefault) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t (a INT, b int DEFAULT 5, PRIMARY KEY(a));
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES(1);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"5"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InsertValuesFromTableWithDefaultAndCast) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t (
                    a INT,
                    b int DEFAULT 5::int4,
                    c int DEFAULT '7'::int4,
                    d varchar(20) DEFAULT 'foo'::varchar(2),
                    e int DEFAULT NULL,
                    f bit varying(5) DEFAULT '1001',
                    g bigint DEFAULT 0 NOT NULL,
                    PRIMARY KEY(a)
                );
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES(1);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"5";"7";"fo";#;"1001";"0"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InsertValuesFromTableWithDefaultBool) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t (a INT, b bool DEFAULT false, PRIMARY KEY(a));
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES(1);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"f"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InsertValuesFromTableWithDefaultText) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t (a INT, b text DEFAULT 'empty', PRIMARY KEY(a));
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES(1);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"empty"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InsertValuesFromTableWithDefaultTextNotNull) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t (a INT, b text NOT NULL DEFAULT 'empty', PRIMARY KEY(a));
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t VALUES(1);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"empty"]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InsertValuesFromTableWithDefaultTextNotNullButNull) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t (a INT, b text NOT NULL DEFAULT NULL, PRIMARY KEY(a));
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            Cerr << result.GetIssues().ToString() << Endl;
            UNIT_ASSERT(!result.IsSuccess());
        }
    }

    Y_UNIT_TEST(InsertValuesFromTableWithDefaultNegativeCase) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t (a INT, b int DEFAULT 'text', PRIMARY KEY(a));
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            Cerr << result.GetIssues().ToString() << Endl;
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(InsertNoTargetColumns_SerialNotNull) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
         {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t1 (a INT, b SERIAL PRIMARY KEY, c INT);
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t2 (a INT, b SERIAL PRIMARY KEY, c INT NOT NULL);
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t2 VALUES(1, 1);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO t1 VALUES(1, 1);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM t1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
            CompareYson(R"(
                [["1";"1";#]]
            )", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(CheckPgAutoParams) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::Pg)
            .StatsMode(NYdb::NQuery::EStatsMode::Basic);

        {
            // Check disable setting
            appConfig.MutableTableServiceConfig()->SetEnablePgConstsToParams(false);
            auto setting = NKikimrKqp::TKqpSetting();
            auto serverSettings = TKikimrSettings()
                .SetAppConfig(appConfig)
                .SetKqpSettings({setting});
            TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
            auto db = kikimr.GetQueryClient();
            const auto query = Q_(R"(
                CREATE TABLE PgTable (
                key int4 PRIMARY KEY,
                value text
            ))");
            db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();

            {
                const auto query = Q_(R"(
                    SELECT * FROM PgTable WHERE key = 1;
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats1 = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats1.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    SELECT * FROM PgTable WHERE key = 2;
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }
        }

        appConfig.MutableTableServiceConfig()->SetEnablePgConstsToParams(true);
        appConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        const auto query = Q_(R"(
            CREATE TABLE PgTable (
            key int4 PRIMARY KEY,
            value text
        ))");
        db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();

        {
            // Check the same queries and differend values
            {
                const auto query = Q_(R"(
                    SELECT * FROM PgTable WHERE key = 3;
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    SELECT * FROM PgTable WHERE key = 4;
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
            }
        }

        {
            // Check NoTx
            {
                const auto query = Q_(R"(
                    CREATE TABLE PgTable3 (
                        key int4 PRIMARY KEY,
                        value text
                    );
                    SELECT * FROM PgTable3 WHERE key = 3;
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            {
                const auto query = Q_(R"(
                    SELECT * FROM PgTable3 WHERE key = 4;
                    SELECT * FROM PgTable3 WHERE key = 5;
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
            }
        }

        {
            // Check values without table
            {
                const auto query = Q_(R"(
                    SELECT 1;
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    SELECT 'a';
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    SELECT true;
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    SELECT 2;
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
            }

            {
                const auto query = Q_(R"(
                    SELECT (1, 2);
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT(result.GetIssues().ToString().Contains("alternative is not implemented yet : 34"));
            }
        }

        {
            // Check wrong values type for table
            {
                const auto query = Q_(R"(
                    SELECT * FROM PgTable WHERE key = '3';
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            {
                const auto query = Q_(R"(
                    SELECT * FROM PgTable WHERE value = 4;
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT(result.GetIssues().ToString().Contains("Unable to find an overload for operator = with given argument type(s): (text,int4)"));
            }

            {
                const auto query = Q_(R"(
                    SELECT * FROM PgTable WHERE key = 3 and value = 4;
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT(result.GetIssues().ToString().Contains("Unable to find an overload for operator = with given argument type(s): (text,int4)"));
            }

            {
                const auto query = Q_(R"(
                    SELECT * FROM PgTable WHERE key = 'a';
                )");
                auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
                UNIT_ASSERT(result.GetIssues().ToString().Contains("invalid input syntax for type integer: \"a\""));
            }
        }

        {
            // Check insert
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE PgTable1(id int4, value text, primary key(id));
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            result = db.ExecuteQuery(R"(
                CREATE TABLE PgTable2(id uint64, primary key(id));
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            {
                auto result = db.ExecuteQuery(R"(
                    INSERT INTO PgTable1 VALUES (1, 'a', 'a');
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT(result.GetIssues().ToString().Contains("values have 3 columns, INSERT INTO expects: 2"));
            }
            {
                auto result = db.ExecuteQuery(R"(
                    INSERT INTO PgTable2 VALUES ('a');
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT(result.GetIssues().ToString().Contains("Failed to convert 'id': pgunknown to Optional<Uint64>"));
            }
            {
                auto result = db.ExecuteQuery(R"(
                    INSERT INTO PgTable1 VALUES ('a', 1);
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
                UNIT_ASSERT(result.GetIssues().ToString().Contains("invalid input syntax for type integer: \"a\""));
            }
        }

        {
            // Check recompile
            {
                auto result = db.ExecuteQuery(R"(
                    CREATE TABLE RecompileTable (id int primary key);
                )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

                result = db.ExecuteQuery(R"(
                    INSERT INTO RecompileTable (id) VALUES (1);
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

                result = db.ExecuteQuery(R"(
                    DROP TABLE RecompileTable;
                )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

                result = db.ExecuteQuery(R"(
                    CREATE TABLE RecompileTable (id int primary key);
                )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

                result = db.ExecuteQuery(R"(
                    INSERT INTO RecompileTable (id) VALUES (1);
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }
        }
    }

    Y_UNIT_TEST(MkqlTerminate) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
         {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE t (id INT PRIMARY KEY, data1 UUID[], data2 UUID[][]);
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            TString binval;
            for (int i = 0; i < 4; i++) {
                binval.push_back(0);
            }
            auto p1Value = TPgValue(TPgValue::VK_TEXT, "1", TPgType("pgunknown"));
            auto p2Value = TPgValue(TPgValue::VK_BINARY, binval, TPgType("pgunknown"));
            auto params = TParamsBuilder()
                .AddParam("$p1")
                    .Pg(p1Value)
                    .Build()
                .AddParam("$p2")
                    .Pg(p2Value)
                    .Build()
                .Build();

            auto result = db.ExecuteQuery(R"(
                INSERT INTO t (id, data2) VALUES ($1, $2);
            )", NYdb::NQuery::TTxControl::NoTx(), params, settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToString().Contains("invalid byte sequence for encoding \"UTF8\": 0x00"));
        }
    }

    Y_UNIT_TEST(NoSelectFullScan) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(serverSettings.SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE pgbench_accounts(aid    int not null,bid int,abalance int,filler char(84), primary key (aid))
            )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = db.ExecuteQuery(R"(
                INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES
                    (1, 1, 10, '                                                                                    '::char),
                    (2, 1, 20, '                                                                                    '::char),
                    (3, 1, 30, '                                                                                    '::char),
                    (4, 1, 40, '
                                               '::char),
                    (5, 1, 50, '                                                                                    '::char),
                    (6, 1, 60, '                                                                                    '::char),
                    (7, 1, 70, '                                                                                    '::char),
                    (8, 1, 80, '                                                                                    '::char),
                    (9, 1, 90, '                                                                                    '::char),
                    (10, 1, 100, '                                                                                    '::char)
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto tc = kikimr.GetTableClient();
            TStreamExecScanQuerySettings settings;
            settings.Explain(true);
            auto it = tc.StreamExecuteScanQuery(R"(
                --!syntax_pg
                SELECT abalance FROM pgbench_accounts WHERE aid = 7 OR aid = 3 ORDER BY abalance;
            )", settings).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto res = CollectStreamResult(it);
            UNIT_ASSERT(res.PlanJson);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(*res.PlanJson, &plan, true);
            UNIT_ASSERT(ValidatePlanNodeIds(plan));

            auto fullScan = FindPlanNodeByKv(plan, "Node Type", "Filter-TableFullScan");
            UNIT_ASSERT_C(!fullScan.IsDefined(), "got fullscan, expected lookup");
            auto lookup = FindPlanNodeByKv(plan, "Node Type", "TableLookup");
            if (!lookup.IsDefined()) {
                lookup = FindPlanNodeByKv(plan, "Node Type", "TableRangeScan");
            }
            UNIT_ASSERT_C(lookup.IsDefined(), "no Table Lookup in plan");
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT abalance FROM pgbench_accounts WHERE aid = 7 OR aid = 3 ORDER BY abalance;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                ["30"];["70"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto tc = kikimr.GetTableClient();
            TStreamExecScanQuerySettings settings;
            settings.Explain(true);
            auto it = tc.StreamExecuteScanQuery(R"(
                --!syntax_pg
                SELECT abalance FROM pgbench_accounts WHERE aid = 7 OR aid < 3 ORDER BY abalance;
            )", settings).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto res = CollectStreamResult(it);
            UNIT_ASSERT(res.PlanJson);
            Cerr << res.PlanJson << Endl;

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(*res.PlanJson, &plan, true);
            UNIT_ASSERT(ValidatePlanNodeIds(plan));

            auto fullScan = FindPlanNodeByKv(plan, "Node Type", "Filter-TableFullScan");
            UNIT_ASSERT_C(!fullScan.IsDefined(), "got fullscan, expected lookup");
            auto lookup = FindPlanNodeByKv(plan, "Node Type", "TableRangeScan");
            UNIT_ASSERT_C(lookup.IsDefined(), "no Table Range Scan in plan");
        }
        {
            auto tc = kikimr.GetTableClient();
            TStreamExecScanQuerySettings settings;
            settings.Explain(true);
            auto it = tc.StreamExecuteScanQuery(R"(
                --!syntax_pg
                SELECT abalance FROM pgbench_accounts WHERE aid > 4 AND aid < 3;
            )", settings).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto res = CollectStreamResult(it);
            UNIT_ASSERT(res.PlanJson);
            Cerr << res.PlanJson << Endl;
            NJson::TJsonValue plan;
            NJson::ReadJsonTree(*res.PlanJson, &plan, true);
            UNIT_ASSERT(ValidatePlanNodeIds(plan));

            auto fullScan = FindPlanNodeByKv(plan, "Node Type", "Filter-TableFullScan");
            UNIT_ASSERT_C(!fullScan.IsDefined(), "got fullscan, expected lookup");
            auto lookup = FindPlanNodeByKv(plan, "Node Type", "TableRangeScan");
            UNIT_ASSERT_C(lookup.IsDefined(), "no Table Range Scan in plan");
        }
        {
            auto result = db.ExecuteQuery(R"(
                SELECT abalance FROM pgbench_accounts WHERE aid > 4 AND aid < 3;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ExplainColumnsReorder) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();
        auto kqpProxy = MakeKqpProxyID(runtime->GetNodeId(0));

        InitRoot(server, sender);

        auto createSession = [&]() {
            runtime->Send(new IEventHandle(kqpProxy, sender, new TEvKqp::TEvCreateSessionRequest()));
            auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvCreateSessionResponse>(sender);
            auto record = reply->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
            return record.GetResponse().GetSessionId();
        };

        auto sendQuery = [&](const TString& queryText) {
            auto ev = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
            ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXPLAIN);
            ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
            ev->Record.MutableRequest()->SetQuery(queryText);
            ev->Record.MutableRequest()->SetSyntax(::Ydb::Query::Syntax::SYNTAX_PG);
            ev->Record.MutableRequest()->SetKeepSession(false);
            ActorIdToProto(sender, ev->Record.MutableRequestActorId());

            runtime->Send(new IEventHandle(kqpProxy, sender, ev.release()));
            return runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
        };

        createSession();

        auto reply = sendQuery(R"(
            SELECT 2 y, 1 x;
        )");

        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRef().GetResponse().GetYdbResults().size(), 1);
        Cerr << reply->Get()->Record.GetRef().GetResponse().DebugString() << Endl;
        auto ydbResults = reply->Get()->Record.GetRef().GetResponse().GetYdbResults();
        TVector <TString> colNames = {"y", "x"};
        UNIT_ASSERT_VALUES_EQUAL(colNames.size(), ydbResults.begin()->Getcolumns().size());
        for (size_t i = 0; i < colNames.size(); i++) {
            UNIT_ASSERT_VALUES_EQUAL(ydbResults.begin()->Getcolumns().at(i).Getname(), colNames[i]);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
