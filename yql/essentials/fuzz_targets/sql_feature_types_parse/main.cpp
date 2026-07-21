#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/fuzz_targets/sql_feature_parse_common.h>

#include <util/memory/pool.h>

using namespace NYql;
using namespace NYql::NFuzzSql;

namespace {

TString BuildTypeString(FuzzedDataProvider& provider, ui32 depth = 0) {
    if (depth >= 4 || provider.remaining_bytes() == 0) {
        return PickType(provider);
    }

    switch (provider.ConsumeIntegralInRange<ui8>(0, 12)) {
        case 0:
            return PickType(provider);
        case 1:
            return "Optional<" + BuildTypeString(provider, depth + 1) + ">";
        case 2:
            return BuildTypeString(provider, depth + 1) + "?";
        case 3:
            return "List<" + BuildTypeString(provider, depth + 1) + ">";
        case 4:
            return "Stream<" + BuildTypeString(provider, depth + 1) + ">";
        case 5:
            return "Dict<" + PickType(provider) + "," + BuildTypeString(provider, depth + 1) + ">";
        case 6:
            return "Tuple<" + BuildTypeString(provider, depth + 1) + "," + BuildTypeString(provider, depth + 1) + ">";
        case 7:
            return "Struct<a:" + BuildTypeString(provider, depth + 1) + ",b:" + BuildTypeString(provider, depth + 1) + ">";
        case 8:
            return "Variant<" + BuildTypeString(provider, depth + 1) + "," + BuildTypeString(provider, depth + 1) + ">";
        case 9:
            return "Variant<a:" + BuildTypeString(provider, depth + 1) + ",b:" + BuildTypeString(provider, depth + 1) + ">";
        case 10:
            return "Callable<(" + BuildTypeString(provider, depth + 1) + ")->" + BuildTypeString(provider, depth + 1) + ">";
        case 11:
            return "Tagged<" + BuildTypeString(provider, depth + 1) + ",tag" + SmallInt(provider, 0, 16) + ">";
        case 12:
            return "Decimal(" + ToString(provider.ConsumeIntegralInRange<ui8>(1, 35)) + "," +
                ToString(provider.ConsumeIntegralInRange<ui8>(0, 9)) + ")";
    }
    return PickType(provider);
}

void ExerciseTypeString(TStringBuf typeText) {
    if (typeText.size() > 2048) {
        return;
    }

    try {
        TMemoryPool pool(4096);
        TIssues issues;
        if (auto* ast = ParseType(typeText, pool, issues)) {
            (void)ast->ToString();
        }
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    if (size > MaxInputSize) {
        return 0;
    }

    const TString raw(reinterpret_cast<const char*>(data), size);
    ExerciseTypeString(raw);

    FuzzedDataProvider provider(data, size);
    const TString generated = BuildTypeString(provider);
    ExerciseTypeString(generated);

    TStringBuilder query;
    query << "SELECT FormatType(ParseType(" << QuoteSqlString(generated) << ")) AS type_text, "
          << "Nothing(ParseType(" << QuoteSqlString(generated) << ")) AS typed_null;";
    ExerciseSqlWithFreshSettings(Limit(query), data, size);
    return 0;
}
