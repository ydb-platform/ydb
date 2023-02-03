#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr {
namespace NKqp {

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateStreamLookupActor(ui64 inputIndex,
    const NUdf::TUnboxedValue& input, const NActors::TActorId& computeActorId, const NMiniKQL::TTypeEnvironment& typeEnv,
    const NMiniKQL::THolderFactory& holderFactory, std::shared_ptr<NMiniKQL::TScopedAlloc>& alloc,
    NKikimrKqp::TKqpStreamLookupSettings&& settings);

} // namespace NKqp
} // namespace NKikimr
