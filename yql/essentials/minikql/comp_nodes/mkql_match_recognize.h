#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr::NMiniKQL {
    IComputationNode* WrapMatchRecognizeCore(TCallable& callable, const TComputationNodeFactoryContext& ctx);
}
