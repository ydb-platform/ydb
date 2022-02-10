#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapPickle(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapStablePickle(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapUnpickle(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapAscending(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapDescending(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
