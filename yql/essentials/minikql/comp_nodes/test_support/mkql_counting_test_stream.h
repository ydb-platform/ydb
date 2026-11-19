#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <util/system/types.h>

namespace NKikimr::NMiniKQL::NTest {

struct TTestStreamParams {
    ui64 MaxAllowedNumberOfFetches;
    ui64 StreamSize;
    // Counts all Fetch calls, including calls returning Finish.
    ui64 Fetches = 0;
};

IComputationNode* WrapTestStream(const TComputationNodeFactoryContext& ctx, TTestStreamParams& params);

} // namespace NKikimr::NMiniKQL::NTest
