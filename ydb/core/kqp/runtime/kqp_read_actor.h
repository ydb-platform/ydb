#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimrTxDataShard {
class TEvRead;
class TEvReadAck;
}

namespace NKikimr {
namespace NKqp {

void RegisterKqpReadActor(NYql::NDq::TDqAsyncIoFactory& factory);

void InjectRangeEvReadSettings(const NKikimrTxDataShard::TEvRead&);
void InjectRangeEvReadAckSettings(const NKikimrTxDataShard::TEvReadAck&);

} // namespace NKqp
} // namespace NKikimr
