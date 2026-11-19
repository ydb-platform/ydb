#pragma once

#include "kqp_rbo_test_helpers.h"

namespace NKikimr::NKqp::NTests {

inline void AssertFreshGraph(IOperator& original, IOperator& copy, TPlanProps& props) {
    THashSet<const IOperator*> originals;
    TUnorderedIUs originalOutputs;
    for (const auto& item : IterateSubtree(&original)) {
        originals.insert(item.Current);
        originalOutputs.UnionWith(item.Current->GetOutputIUs());
    }
    for (const auto& item : IterateSubtree(&copy)) {
        UNIT_ASSERT(!originals.contains(item.Current));
        UNIT_ASSERT(!originalOutputs.HasAny(item.Current->GetOutputIUs()));
        UNIT_ASSERT(!item.Current->Type);
        UNIT_ASSERT(!item.Current->Props.StageId);
        UNIT_ASSERT(item.Current->Parents.empty());
    }
    NTests::AssertIdInvariants(copy, props);
}

} // namespace NKikimr::NKqp::NTests
