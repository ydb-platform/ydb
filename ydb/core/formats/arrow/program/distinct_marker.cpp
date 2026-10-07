#include "distinct_marker.h"

namespace NKikimr::NArrow::NSSA {

TConclusion<TExecutionResult> TDistinctMarkerProcessor::DoExecute(
    const TProcessorContext& /*context*/, const TExecutionNodeContext& /*nodeContext*/) const {
    // Stateless marker: drives graph optimizations only; reader sync points apply DISTINCT.
    return TExecutionResult::Done();
}

} // namespace NKikimr::NArrow::NSSA
