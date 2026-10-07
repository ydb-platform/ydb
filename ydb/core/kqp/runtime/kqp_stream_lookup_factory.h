#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/runtime/kqp_vector_index_levels_cache.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimr {
namespace NKqp {

void RegisterStreamLookupActorFactory(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<NKqp::TKqpCounters>,
    TIntrusivePtr<TVectorIndexLevelsCache> vectorIndexLevelsCache);

} // namespace NKqp
} // namespace NKikimr
