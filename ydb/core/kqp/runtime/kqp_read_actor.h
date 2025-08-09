#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

#include <util/generic/ptr.h>

namespace NKikimrTxDataShard {
class TEvRead;
class TEvReadAck;
class TKqpReadRangesSourceSettings;
}

namespace NKikimr {
namespace NKqp {

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> CreateKqpReadActor(const NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings,
    TIntrusivePtr<NActors::TProtoArenaHolder> arena, // Arena for settings
    const NActors::TActorId& computeActorId,
    ui64 inputIndex,
    NYql::NDq::TCollectStatsLevel statsLevel,
    NYql::NDq::TTxId txId,
    ui64 taskId,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
    const NWilson::TTraceId& traceId,
    TIntrusivePtr<TKqpCounters> counters);
void RegisterKqpReadActor(NYql::NDq::TDqAsyncIoFactory&, TIntrusivePtr<TKqpCounters>);
void InterceptReadActorPipeCache(NActors::TActorId);

} // namespace NKqp
} // namespace NKikimr
