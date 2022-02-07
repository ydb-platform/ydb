#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapMakeHeap(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapPushHeap(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapPopHeap(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapSortHeap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapStableSort(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapNthElement(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapPartialSort(TCallable& callable, const TComputationNodeFactoryContext& ctx);
}
}
