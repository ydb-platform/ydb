#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapTakeWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapSkipWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapTakeWhileInclusive(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapSkipWhileInclusive(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
