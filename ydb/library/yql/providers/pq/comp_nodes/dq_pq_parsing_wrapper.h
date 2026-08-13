#pragma once

namespace NKikimr::NMiniKQL {

class IComputationNode;
class TCallable;
struct TComputationNodeFactoryContext;

IComputationNode* WrapDqPqParsingWrapper(
    TCallable& callable,
    const TComputationNodeFactoryContext& ctx
);

} // namespace NKikimr::NMiniKQL
