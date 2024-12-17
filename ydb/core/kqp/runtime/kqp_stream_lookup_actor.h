#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr {
namespace NKqp {

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateStreamLookupActor(NYql::NDq::IDqAsyncIoFactory::TInputTransformArguments&& args,
    NKikimrKqp::TKqpStreamLookupSettings&& settings, TIntrusivePtr<TKqpCounters>);

void InterceptStreamLookupActorPipeCache(NActors::TActorId);

} // namespace NKqp
} // namespace NKikimr
