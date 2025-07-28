#include "settings.h"

namespace NYql::NFastCheck {

    THashSet<TString> TranslationFlags() {
        return {
            "AnsiOrderByLimitInUnionAll",
            "DisableCoalesceJoinKeysOnQualifiedAll",
            "AnsiRankForNullableKeys",
            "DisableUnorderedSubqueries",
            "DisableAnsiOptionalAs",
            "FlexibleTypes",
            "CompactNamedExprs",
            "DistinctOverWindow",
        };
    }

} // namespace NYql::NFastCheck
