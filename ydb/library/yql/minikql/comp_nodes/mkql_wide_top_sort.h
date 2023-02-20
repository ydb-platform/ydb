#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapWideTop(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideTopSort(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapWideSort(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}


