#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapWideMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
