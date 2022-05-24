#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/kqp_compute.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NKikimr {
namespace NMiniKQL {

class TKqpScanComputeContext;

TComputationNodeFactory GetKqpActorComputeFactory(TKqpScanComputeContext* computeCtx);

} // namespace NMiniKQL

namespace NKqp {

IActor* CreateKqpComputeActor(const TActorId& executerId, ui64 txId, NYql::NDqProto::TDqTask&& task,
    NYql::NDq::IDqSourceFactory::TPtr sourceFactory, NYql::NDq::IDqSinkFactory::TPtr sinkFactory, NYql::NDq::IDqOutputTransformFactory::TPtr transformFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const NYql::NDq::TComputeRuntimeSettings& settings, const NYql::NDq::TComputeMemoryLimits& memoryLimits);

IActor* CreateKqpScanComputeActor(const NKikimrKqp::TKqpSnapshot& snapshot, const TActorId& executerId, ui64 txId,
    NYql::NDqProto::TDqTask&& task, NYql::NDq::IDqSourceFactory::TPtr sourceFactory, NYql::NDq::IDqSinkFactory::TPtr sinkFactory, NYql::NDq::IDqOutputTransformFactory::TPtr transformFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const NYql::NDq::TComputeRuntimeSettings& settings, const NYql::NDq::TComputeMemoryLimits& memoryLimits,
    TIntrusivePtr<TKqpCounters> counters);


} // namespace NKqp
} // namespace NKikimr
