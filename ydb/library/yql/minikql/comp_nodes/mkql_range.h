#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapRangeCreate(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapRangeUnion(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapRangeIntersect(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapRangeMultiply(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapRangeFinalize(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
