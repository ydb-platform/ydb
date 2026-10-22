#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <optional>

namespace NKikimr::NKqp::NOpt {

// A LIKE/ILIKE apply whose result is the match itself. `Column` is the Member node inside the predicate.
struct TLikeHint {
    const NYql::TExprNode* Column = nullptr;
    TString Pattern;
    TString Escape;
    bool CaseSensitive = true;
};

// Accepts a LIKE/ILIKE apply, Coalesce of that match with false, or Map/FlatMap produced for an
// optional column. Nothing for NOT, a parameter, or a non-literal pattern. AND is split before this
// runs, so each conjunct can carry its own hint. OR stays one apply and gets no hint.
std::optional<TLikeHint> TryParseLikeHint(const NYql::TExprNode& predicate);

}   // namespace NKikimr::NKqp::NOpt
