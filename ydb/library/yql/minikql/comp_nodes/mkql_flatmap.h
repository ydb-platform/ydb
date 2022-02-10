#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapFlatMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapNarrowFlatMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);


}
}
