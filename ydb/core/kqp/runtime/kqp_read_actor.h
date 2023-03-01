#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimrTxDataShard {
class TEvRead;
class TEvReadAck;
}

namespace NKikimr {
namespace NKqp {

void RegisterKqpReadActor(NYql::NDq::TDqAsyncIoFactory&, TIntrusivePtr<TKqpCounters>);

void InjectRangeEvReadSettings(const NKikimrTxDataShard::TEvRead&);
void InjectRangeEvReadAckSettings(const NKikimrTxDataShard::TEvReadAck&);

void InterceptReadActorPipeCache(NActors::TActorId);

} // namespace NKqp
} // namespace NKikimr
