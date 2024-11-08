#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapToSortedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapToHashedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapSqueezeToSortedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapSqueezeToHashedDict(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
