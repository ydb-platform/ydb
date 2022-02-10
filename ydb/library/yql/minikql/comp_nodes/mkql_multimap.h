#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapMultiMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapNarrowMultiMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
