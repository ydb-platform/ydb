#pragma once

namespace NKikimr::NMiniKQL {

class IComputationNode;
class TCallable;
class TKqpComputeContextBase;
struct TComputationNodeFactoryContext;

IComputationNode* WrapKqpStreamingAggregation(TCallable& callable, const TComputationNodeFactoryContext& ctx, const TKqpComputeContextBase& computeCtx);

} // namespace NKikimr::NMiniKQL
