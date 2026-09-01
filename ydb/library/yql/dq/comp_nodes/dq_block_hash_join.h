#pragma once
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

#include <ydb/library/yql/dq/runtime/dq_compute.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapDqBlockHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx, NYql::NDq::TDqComputeContextBase& computeCtx);

} // namespace NMiniKQL
} // namespace NKikimr
