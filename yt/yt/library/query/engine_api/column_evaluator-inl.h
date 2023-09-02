#ifndef COLUMN_EVALUATOR_INL_H_
#error "Direct inclusion of this file is not allowed, include column_evaluator.h"
// For the sake of sane code completion.
#include "column_evaluator.h"
#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE bool TColumnEvaluator::IsAggregate(int index) const
{
    return IsAggregate_[index];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
