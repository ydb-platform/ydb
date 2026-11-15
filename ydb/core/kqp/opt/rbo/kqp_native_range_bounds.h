#pragma once

#include "frontend/expression.h"

#include <optional>

namespace NKikimr::NKqp {

class TExpression;

// A closed scalar bound in the storage key's exact type. Physical range and
// lookup builders evaluate Value only when the optional validity predicate is
// true. Admission, key-prefix selection and index ranking remain in the rule.
struct TNativeRangeBound {
    NFrontend::TExprId Value;
    std::optional<NFrontend::TExprId> Validity;
    bool Inclusive;
};

NFrontend::TExprId StripNativeOptionalLift(const NFrontend::TExpressionArena& arena, NFrontend::TExprId id);
NFrontend::TExprId StripNativeRangeKeyPromotion(const NFrontend::TExpressionArena& arena,
    NFrontend::TExprId id, NFrontend::EOpcode comparison);
bool HasUnorderedNativeKeys(NFrontend::TType type);
// A range-extraction view of filter truth, never a scalar replacement. Legacy
// membership keeps its lookup guard. Ordered scalar comparisons expose their
// complements; other NOT operands and ordinary IF remain opaque.
NFrontend::TExprId PrepareNativeRangePredicate(NFrontend::TExpressionArena& arena, NFrontend::TExprId root);
std::optional<TNativeRangeBound> BuildNativeRangeBound(const TExpression& expression,
    NFrontend::TType keyType, NFrontend::EOpcode comparison);

} // namespace NKikimr::NKqp
