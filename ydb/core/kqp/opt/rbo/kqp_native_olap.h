#pragma once

#include "frontend/expression.h"
#include <util/generic/strbuf.h>

namespace NKikimr::NKqp {

// Storage capabilities consume native nodes and types only. The optimizer and
// emitter share this contract without constructing backend expressions.
bool CanLowerNativeOlapPredicate(const NFrontend::TExpressionArena& arena,
    NFrontend::TExprId root, bool allowScalarApply = false);
// Preserve the existing collector's predicate/operand admission rules in
// addition to checking whether native emission can execute the expression.
bool CanPushNativeOlapPredicate(const NFrontend::TExpressionArena& arena,
    NFrontend::TExprId root, bool allowScalarApply = false, bool allowRegexp = false);
struct TNativeOlapPredicateParts {
    std::vector<NFrontend::TExprId> Pushed;
    std::vector<NFrontend::TExprId> Remaining;
};

// Preserve ordered partial pushdown and the separate scalar-apply retry.
// IDs belong to the input arena; no backend expressions are constructed.
TNativeOlapPredicateParts SplitNativeOlapPredicate(const NFrontend::TExpressionArena& arena,
    NFrontend::TExprId root, bool allowScalarApply = false, bool allowRegexp = false);

bool CanLowerNativeOlapProjection(const NFrontend::TExpressionArena& arena, NFrontend::TExprId root);

namespace NDetail {
TStringBuf OlapBinaryName(NFrontend::EOpcode op);
bool OlapIdentityConversion(const NFrontend::TExpressionArena& arena, const NFrontend::TExprNode& node);
}

} // namespace NKikimr::NKqp
