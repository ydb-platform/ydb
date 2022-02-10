#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapExpandMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapWideMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

IComputationNode* WrapNarrowMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
