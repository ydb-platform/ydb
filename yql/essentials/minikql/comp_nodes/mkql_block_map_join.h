#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapBlockStorage(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapBlockMapJoinIndex(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapBlockMapJoinCore(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL
