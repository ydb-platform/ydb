#include "mkql_external_node_invalidator.h"

#include <yql/essentials/public/udf/udf_value.h>

#include <set>

namespace NKikimr::NMiniKQL {

TComputationExternalNodeInvalidator::TComputationExternalNodeInvalidator(std::span<const IComputationExternalNode* const> nodes) {
    std::set<ui32> merged;
    for (const IComputationExternalNode* const node : nodes) {
        node->CollectInvalidationIndexes(merged);
    }
    MergedIndexes_.assign(merged.begin(), merged.end());
}

TComputationExternalNodeInvalidator::TComputationExternalNodeInvalidator() = default;

void TComputationExternalNodeInvalidator::InvalidateMutables(TComputationContext& compCtx) const {
    for (ui32 index : MergedIndexes_) {
        compCtx.MutableValues[index] = NUdf::TUnboxedValuePod::Invalid();
    }
}

} // namespace NKikimr::NMiniKQL
