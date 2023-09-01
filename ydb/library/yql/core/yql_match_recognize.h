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
            YQL_ENSURE(factor->ChildrenSize() == 5, "Expect 5 args");
            result.back().push_back(TRowPatternFactor{
                factor->ChildRef(0)->IsAtom() ?
                TRowPatternPrimary(TString(factor->ChildRef(0)->Content())) :
                ConvertPattern(factor->ChildRef(0), ctx, ++nestingLevel),
                FromString<ui64>(factor->ChildRef(1)->Content()),
                FromString<ui64>(factor->ChildRef(2)->Content()),
                FromString<bool>(factor->ChildRef(3)->Content()),
                FromString<bool>(factor->ChildRef(4)->Content())
            });
        }
    }
    return result;
}

} //namespace NYql::NMatchRecognize
