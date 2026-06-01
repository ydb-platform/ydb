#pragma once

namespace NKikimr::NMiniKQL {

class IComputationNode;
class TCallable;
struct TComputationNodeFactoryContext;

class TDqHashCombineTestPoints {
public:
    virtual void DisableStateDehydration(const bool disable) = 0;
    virtual void DisableKeyPassthrough(const bool disable) = 0;
};

IComputationNode* WrapDqHashCombine(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapDqHashAggregate(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL
