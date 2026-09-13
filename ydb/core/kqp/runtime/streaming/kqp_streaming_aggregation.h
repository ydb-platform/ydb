#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>

namespace NKikimr::NMiniKQL {

class TKqpComputeContextBase;

IComputationNode* WrapStreamingAggregation(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    const TKqpComputeContextBase& computeCtx);

} // namespace NKikimr::NMiniKQL
