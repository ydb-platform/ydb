#include "distinct_marker.h"

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TDistinctMarkerProcessor::DoExecute(
    const TProcessorContext& /*context*/, const TExecutionNodeContext& /*nodeContext*/) const {
    // Stateless marker: drives graph optimizations only; reader sync points apply DISTINCT.
    return IResourceProcessor::EExecutionResult::Success;
}

} // namespace NKikimr::NArrow::NSSA
