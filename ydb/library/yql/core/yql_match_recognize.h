#pragma once
#include <ydb/library/yql/core/sql_types/match_recognize.h>
#include "ydb/library/yql/ast/yql_expr.h"

namespace NYql::NMatchRecognize {

inline TRowPattern ConvertPattern(const TExprNode::TPtr& pattern, TExprContext &ctx, size_t nestingLevel = 0) {
    YQL_ENSURE(nestingLevel <= MaxPatternNesting, "To big nesting level in the pattern");
    TRowPattern result;
    for (const auto& term: pattern->Children()) {
        result.push_back(TRowPatternTerm{});
        for (const auto& factor: term->Children()) {
            YQL_ENSURE(factor->ChildrenSize() == 6, "Expect 6 args");
            result.back().push_back(TRowPatternFactor{
                factor->Child(0)->IsAtom() ?
                    TRowPatternPrimary(TString(factor->Child(0)->Content())) :
                    ConvertPattern(factor->Child(0), ctx, nestingLevel + 1),
                FromString<ui64>(factor->Child(1)->Content()),
                FromString<ui64>(factor->Child(2)->Content()),
                FromString<bool>(factor->Child(3)->Content()),
                FromString<bool>(factor->Child(4)->Content()),
                FromString<bool>(factor->Child(5)->Content())
            });
        }
    }
    return result;
}

} //namespace NYql::NMatchRecognize
