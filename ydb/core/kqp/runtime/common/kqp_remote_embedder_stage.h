#pragma once

namespace NKikimr::NMiniKQL {

class TCallable;
class IComputationNode;
struct TComputationNodeFactoryContext;
class TKqpComputeContextBase;

// Raised at the stage boundary after its evaluation stack has been suspended.
struct TKqpRemoteEmbedderPending {};

IComputationNode* WrapKqpRemoteEmbedderStage(TCallable& callable,
    const TComputationNodeFactoryContext& ctx, TKqpComputeContextBase& computeCtx);

} // namespace NKikimr::NMiniKQL
