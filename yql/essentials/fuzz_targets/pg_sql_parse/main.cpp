#include <yql/essentials/parser/pg_wrapper/arena_ctx.h>
#include <yql/essentials/parser/pg_wrapper/interface/raw_parser.h>
#include <yql/essentials/parser/pg_wrapper/interface/utils.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace {

constexpr size_t MaxInputSize = 8 * 1024;

class TEvents final: public NYql::IPGParseEvents {
public:
    void OnResult(const List* raw) override {
        if (raw) {
            (void)NYql::PrintPGTree(raw);
        }
    }

    void OnError(const NYql::TIssue& issue) override {
        (void)issue.ToString();
    }
};

TString BuildPgQuery(FuzzedDataProvider& provider) {
    const int value = provider.ConsumeIntegralInRange<int>(-100, 100);
    switch (provider.ConsumeIntegralInRange<ui8>(0, 9)) {
        case 0:
            return TStringBuilder() << "SELECT " << value << " AS value";
        case 1:
            return "SELECT a, count(*) FROM (VALUES (1), (2), (3)) AS t(a) GROUP BY a";
        case 2:
            return "SELECT * FROM generate_series(1, 5) AS g(x) WHERE x BETWEEN 1 AND 4";
        case 3:
            return "SELECT jsonb_extract_path_text('{\"a\":{\"b\":1}}'::jsonb, 'a', 'b')";
        case 4:
            return "SELECT ARRAY[1,2,3] @> ARRAY[2]";
        case 5:
            return "SELECT CASE WHEN 1 IS DISTINCT FROM 2 THEN 'yes' ELSE 'no' END";
        case 6:
            return "WITH q AS (SELECT 1 AS x) SELECT * FROM q UNION ALL SELECT 2";
        case 7:
            return "SELECT row_number() OVER (PARTITION BY 1 ORDER BY 1)";
        case 8:
            return "CREATE TABLE pg_fuzz(id int primary key, payload text)";
        case 9:
            return "SELECT interval '1 day' + interval '2 hours'";
    }
    return "SELECT 1";
}

void ExercisePg(TStringBuf query) {
    if (query.empty() || query.size() > MaxInputSize) {
        return;
    }

    try {
        TEvents events;
        NYql::PGParse(TString(query), events);

        NYql::TPGParseResult result;
        NYql::PGParse(TString(query), result);
        result.Visit(events);
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    if (size > MaxInputSize) {
        return 0;
    }

    const TString raw(reinterpret_cast<const char*>(data), size);
    ExercisePg(raw);

    FuzzedDataProvider provider(data, size);
    ExercisePg(BuildPgQuery(provider));

    i32 modifier = 0;
    try {
        (void)NYql::ParsePgIntervalModifier(raw, modifier);
    } catch (...) {
    }
    return 0;
}
