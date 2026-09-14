#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/common/dq_common.h>

namespace NYql::NDq {

// Single actor where will be calculated final aggregate.
// Expected usage: one actor on DQ graph life time, so all registered clients are not removed
NActors::IActor* CreateDqPqInfoAggregationActor(const TTxId& txId);

void RegisterDqPqInfoAggregationActorFactory(TDqAsyncIoFactory& factory);

} // namespace NYql::NDq
