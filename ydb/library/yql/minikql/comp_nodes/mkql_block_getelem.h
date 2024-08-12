#pragma once
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapBlockMember(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapBlockNth(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NMiniKQL
} // namespace NKikimr
