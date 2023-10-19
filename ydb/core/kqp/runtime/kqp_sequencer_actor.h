#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr {
namespace NKqp {

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateSequencerActor(ui64 inputIndex,
    NYql::NDq::TCollectStatsLevel statsLevel, const NUdf::TUnboxedValue& input, const NActors::TActorId& computeActorId,
    const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
    std::shared_ptr<NMiniKQL::TScopedAlloc>& alloc, NKikimrKqp::TKqpSequencerSettings&& settings,
    TIntrusivePtr<TKqpCounters>);

} // namespace NKqp
} // namespace NKikimr
