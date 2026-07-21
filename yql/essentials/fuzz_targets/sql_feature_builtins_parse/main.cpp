#include <yql/essentials/fuzz_targets/sql_feature_parse_common.h>

using namespace NYql::NFuzzSql;

namespace {

TString BuildExpression(FuzzedDataProvider& provider) {
    const TString lit = QuoteSqlString(provider.ConsumeBytesAsString(provider.ConsumeIntegralInRange<size_t>(0, 64)));
    const TString n = SmallInt(provider, 0, 100);

    switch (provider.ConsumeIntegralInRange<ui8>(0, 34)) {
        case 0: return "CAST(" + lit + " AS Utf8)";
        case 1: return "COALESCE(NULLIF(" + lit + ", ''), 'fallback')";
        case 2: return "IF(" + n + " > 10, 'big', 'small')";
        case 3: return "AsList(1, 2, " + n + ")";
        case 4: return "ListMap(AsList(1, 2, 3), ($x)->($x + " + n + "))";
        case 5: return "ListFilter(AsList(1, 2, 3), ($x)->($x > " + n + "))";
        case 6: return "ListFlatMap(AsList(1, 2), ($x)->(AsList($x, $x + 1)))";
        case 7: return "ListFold(AsList(1, 2, 3), 0, ($item, $state)->($item + $state))";
        case 8: return "ListSort(AsList(3, 1, 2))";
        case 9: return "ListZip(AsList(1, 2), AsList('a', 'b'))";
        case 10: return "AsDict(AsTuple('a', 1), AsTuple('b', 2))";
        case 11: return "DictLookup(AsDict(AsTuple('a', 1)), 'a')";
        case 12: return "DictContains(AsDict(AsTuple('a', 1)), 'a')";
        case 13: return "DictKeys(AsDict(AsTuple('a', 1), AsTuple('b', 2)))";
        case 14: return "DictPayloads(AsDict(AsTuple('a', 1), AsTuple('b', 2)))";
        case 15: return "AsStruct(" + lit + " AS name, " + n + " AS value)";
        case 16: return "AddMember(AsStruct(1 AS a), 'b', 2)";
        case 17: return "RemoveMember(AsStruct(1 AS a, 2 AS b), 'b')";
        case 18: return "ChooseMembers(AsStruct(1 AS a, 2 AS b), AsList('a'))";
        case 19: return "CombineMembers(AsStruct(1 AS a), AsStruct(2 AS b))";
        case 20: return "JSON_VALUE(CAST(@@{\"a\":1,\"b\":[2]}@@ AS Json), 'strict $.a')";
        case 21: return "JSON_QUERY(CAST(@@{\"a\":1,\"b\":[2]}@@ AS Json), 'strict $.b')";
        case 22: return "JSON_EXISTS(CAST(@@{\"a\":1}@@ AS Json), 'strict $.a')";
        case 23: return "Yson::Parse(Yson(@@{a=1;b=\"x\"}@@))";
        case 24: return "Yson::SerializeText(Yson::Parse(Yson(@@{a=1}@@)))";
        case 25: return "Yson::Lookup(Yson::Parse(Yson(@@{a=1;b={c=2}}@@)), 'b.c')";
        case 26: return "Yson::ConvertTo(Yson::Parse(Yson(@@1@@)), Int32)";
        case 27: return "FormatType(ParseType('List<Int32?>'))";
        case 28: return "TypeOf(" + n + ")";
        case 29: return "InstanceOf(ParseType('Int32'))";
        case 30: return "EvaluateCode(FuncCode('Int32', AtomCode('1')))";
        case 31: return "FormatCode(FuncCode('Int32', AtomCode('1')))";
        case 32: return "QuoteCode(AtomCode(" + lit + "))";
        case 33: return "String::Length(" + lit + ")";
        case 34: return "Unicode::ToUpper(CAST(" + lit + " AS Utf8))";
    }
    return lit;
}

TString BuildBuiltinQuery(FuzzedDataProvider& provider) {
    TStringBuilder query;
    query << "SELECT\n";
    const size_t count = provider.ConsumeIntegralInRange<size_t>(1, 8);
    for (size_t i = 0; i < count; ++i) {
        if (i) {
            query << ",\n";
        }
        query << "  " << BuildExpression(provider) << " AS value" << i;
    }
    query << ";\n";

    if (provider.ConsumeBool()) {
        query << "SELECT grp, COUNT(*) AS c, SUM(amount) AS s, COUNT_IF(flag) AS cf "
              << "FROM plato.Input GROUP BY grp;\n";
    }
    return Limit(query);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    if (size > MaxInputSize) {
        return 0;
    }

    const TString raw(reinterpret_cast<const char*>(data), size);
    ExerciseSqlWithFreshSettings(raw, data, size);

    FuzzedDataProvider provider(data, size);
    ExerciseSqlWithFreshSettings(BuildBuiltinQuery(provider), data, size);
    return 0;
}
