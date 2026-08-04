#include <yql/essentials/fuzz_targets/sql_feature_parse_common.h>

using namespace NYql::NFuzzSql;

namespace {

TString BuildSelectQuery(FuzzedDataProvider& provider) {
    const TString lit = QuoteSqlString(provider.ConsumeBytesAsString(provider.ConsumeIntegralInRange<size_t>(0, 64)));
    const TString ident = Identifier(provider.ConsumeBytesAsString(provider.ConsumeIntegralInRange<size_t>(0, 24)), "col");
    const TString n = SmallInt(provider);

    TStringBuilder query;
    switch (provider.ConsumeIntegralInRange<ui8>(0, 15)) {
        case 0:
            query << "SELECT key, subkey, value FROM plato.Input WHERE amount BETWEEN " << n
                  << " AND " << SmallInt(provider, 0, 200) << " ORDER BY key LIMIT "
                  << provider.ConsumeIntegralInRange<ui32>(0, 64) << " OFFSET "
                  << provider.ConsumeIntegralInRange<ui32>(0, 16) << ";";
            break;
        case 1:
            query << "SELECT a.key, b.value FROM plato.Input AS a "
                  << "INNER JOIN plato.Input2 AS b ON a.key = b.key WHERE a.value != " << lit << ";";
            break;
        case 2:
            query << "SELECT a.key FROM plato.Input AS a LEFT SEMI JOIN plato.Input2 AS b ON a.key = b.key;";
            break;
        case 3:
            query << "SELECT grp, COUNT(*) AS cnt, SUM(amount) AS total, COUNT_IF(flag) AS flags "
                  << "FROM plato.Input GROUP BY grp HAVING cnt >= " << provider.ConsumeIntegralInRange<ui32>(0, 8) << ";";
            break;
        case 4:
            query << "SELECT key, grp, amount, ROW_NUMBER() OVER w AS rn, "
                  << "SUM(amount) OVER w AS running FROM plato.Input "
                  << "WINDOW w AS (PARTITION BY grp ORDER BY key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
            break;
        case 5:
            query << "SELECT * FROM (SELECT key, value FROM plato.Input) AS s "
                  << "UNION ALL SELECT key, value FROM plato.Input2;";
            break;
        case 6:
            query << "SELECT key FROM plato.Input INTERSECT SELECT key FROM plato.Input2;";
            break;
        case 7:
            query << "SELECT key FROM plato.Input EXCEPT SELECT key FROM plato.Input2;";
            break;
        case 8:
            query << "$rows = AsList(AsStruct(" << lit << " AS key, " << n << " AS amount));\n"
                  << "SELECT key, amount FROM AS_TABLE($rows) WHERE key IS NOT NULL;";
            break;
        case 9:
            query << "SELECT * WITHOUT " << ident << " FROM plato.Input SAMPLE "
                  << provider.ConsumeIntegralInRange<ui32>(1, 99) << " PERCENT;";
            break;
        case 10:
            query << "SELECT key, subkey, value FROM plato.Input FLATTEN LIST BY tags;";
            break;
        case 11:
            query << "SELECT key, value FROM plato.Input ORDER BY key ASSUME ORDER BY key;";
            break;
        case 12:
            query << "SELECT key, CASE WHEN amount < 0 THEN 'neg' WHEN amount = 0 THEN 'zero' ELSE 'pos' END AS bucket "
                  << "FROM plato.Input WHERE key IN (SELECT key FROM plato.Input2);";
            break;
        case 13:
            query << "SELECT key, value FROM plato.Input WHERE value LIKE " << lit
                  << " OR value REGEXP " << QuoteSqlString("[a-z]+") << ";";
            break;
        case 14:
            query << "SELECT key, amount FROM plato.Input MATCH_RECOGNIZE ("
                  << "PARTITION BY key ORDER BY subkey MEASURES A.amount AS first_amount "
                  << "PATTERN (A B*) DEFINE A AS A.amount >= 0, B AS B.amount >= A.amount);";
            break;
        case 15:
            query << "PROCESS plato.Input USING ($row)->{ RETURN AsStruct($row.key AS key, "
                  << "String::Length($row.value) AS len); };";
            break;
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
    ExerciseSqlWithFreshSettings(BuildSelectQuery(provider), data, size);
    return 0;
}
