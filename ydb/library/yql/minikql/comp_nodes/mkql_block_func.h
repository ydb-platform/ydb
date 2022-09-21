#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapBlockFunc(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapBlockBitCast(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
