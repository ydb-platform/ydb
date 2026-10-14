#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <optional>

namespace NKikimr::NKqp::NOpt {

// A positive LIKE/ILIKE whose whole result is the match. `Column` is the Member node inside the predicate.
struct TLikeHint {
    const NYql::TExprNode* Column = nullptr;
    TString Pattern;
    TString Escape;
    bool CaseSensitive = true;
};

// Accepts the predicate itself or Coalesce(predicate, false). Nothing for NOT, OR, a parameter, or a non-literal pattern.
std::optional<TLikeHint> TryParseLikeHint(const NYql::TExprNode& predicate);

}   // namespace NKikimr::NKqp::NOpt
