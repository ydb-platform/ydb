#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapAggrCountInit(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapAggrCountUpdate(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
