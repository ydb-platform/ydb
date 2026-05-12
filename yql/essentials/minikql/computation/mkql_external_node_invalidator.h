#pragma once

#include "mkql_computation_node.h"

#include <span>

namespace NKikimr::NMiniKQL {

class TComputationExternalNodeInvalidator {
public:
    explicit TComputationExternalNodeInvalidator(std::span<const IComputationExternalNode* const> nodes);
    TComputationExternalNodeInvalidator();

    void InvalidateMutables(TComputationContext& compCtx) const;

private:
    std::vector<ui32> MergedIndexes_;
};

} // namespace NKikimr::NMiniKQL
