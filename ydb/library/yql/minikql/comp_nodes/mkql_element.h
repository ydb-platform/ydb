#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapNth(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapMember(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
