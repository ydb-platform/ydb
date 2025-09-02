#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

template <bool All>
IComputationNode* WrapZip(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
