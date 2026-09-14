#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapWideTopBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideTopSortBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideSortBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL
