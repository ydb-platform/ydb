#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapIterator(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapEmptyIterator(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapForwardList(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
