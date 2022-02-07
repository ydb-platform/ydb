#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapWideFilter(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideTakeWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideSkipWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideTakeWhileInclusive(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideSkipWhileInclusive(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

