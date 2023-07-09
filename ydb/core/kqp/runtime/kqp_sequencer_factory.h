#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimr {
namespace NKqp {

void RegisterSequencerActorFactory(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<NKqp::TKqpCounters>);

} // namespace NKqp
} // namespace NKikimr
