#pragma once

#include <ydb/library/yql/dq/runtime/dq_compute.h>

namespace NKikimr {
namespace NMiniKQL {

class TDqHashCombineTestPoints {
public:
    virtual void DisableStateDehydration(const bool disable) = 0;
};

IComputationNode* WrapDqHashCombine(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapDqHashAggregate(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NMiniKQL
} // namespace NKikimr
