#pragma once

#include "kqp_operator.h"

namespace NKikimr::NKqp {

// Checks whether this operator can be copied and evaluated separately.
// Does not check inputs or referenced subplans.
bool CanDuplicateOperator(const IOperator& op);
// Checks the entire graph that Copy(props) clones, including called subplans.
bool CanDuplicateSubtree(const IOperator& root, const TSubplans& subplans, size_t maxOperators = 1000);

} // namespace NKikimr::NKqp
