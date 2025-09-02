#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapCondense(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapSqueeze(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
