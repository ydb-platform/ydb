#pragma once

#include "expression.h"

namespace NKikimr::NKqp::NFrontend {

// A filter observes TRUE versus rejected rows, so FALSE and NULL are equivalent
// at this boundary. Expose native Boolean structure without changing which
// scalar expressions are evaluated. Preserve IN's native null-result operation
// and general IF: their optimizer admission differs from conjunctions.
// Scalar result expressions stay untouched.
TExprId PrepareFilterPredicate(TExpressionArena& arena, TExprId root);

} // namespace NKikimr::NKqp::NFrontend
