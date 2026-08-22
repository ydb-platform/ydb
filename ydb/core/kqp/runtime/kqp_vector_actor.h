#pragma once

#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimrTxDataShard {
class TEvRead;
class TEvReadAck;
}

namespace NKikimr {
namespace NKqp {

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> CreateKqpVectorResolveActor(
    NKikimrTxDataShard::TKqpVectorResolveSettings&& settings,
    ui64 inputIndex,
    const NUdf::TUnboxedValue& input,
    NYql::NDq::TCollectStatsLevel statsLevel,
    NYql::NDq::TTxId txId,
    ui64 taskId,
    const NActors::TActorId& computeActorId,
    const NMiniKQL::TTypeEnvironment& typeEnv,
    const NMiniKQL::THolderFactory& holderFactory,
    std::shared_ptr<NMiniKQL::TScopedAlloc>& alloc,
    const NWilson::TTraceId& traceId,
    TIntrusivePtr<TKqpCounters> counters);

void RegisterKqpVectorResolveActor(NYql::NDq::TDqAsyncIoFactory&, TIntrusivePtr<TKqpCounters>);

} // namespace NKqp
} // namespace NKikimr
