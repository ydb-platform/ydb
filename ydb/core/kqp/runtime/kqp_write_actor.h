#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimr {
namespace NKqp {

void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory&, TIntrusivePtr<TKqpCounters>);

}
}
