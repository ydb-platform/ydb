#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapUdf(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapScriptUdf(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
