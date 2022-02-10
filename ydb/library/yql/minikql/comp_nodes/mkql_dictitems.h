#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapDictItems(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapDictKeys(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapDictPayloads(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
