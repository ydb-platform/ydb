#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapWideCombiner(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideLastCombiner(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideLastCombinerWithSpilling(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}

