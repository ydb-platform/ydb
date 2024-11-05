#pragma once

#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/providers/yt/proto/source.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NYql::NDq {

std::pair<NYql::NDq::IDqAsyncLookupSource*, NActors::IActor*> CreateYtLookupActor(
    NFile::TYtFileServices::TPtr ytServices,
    NActors::TActorId parentId,
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
    std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> keyTypeHelper,
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    NYql::NYt::NSource::TLookupSource&& lookupSource,
    const NKikimr::NMiniKQL::TStructType* keyType,
    const NKikimr::NMiniKQL::TStructType* payloadType,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const size_t maxKeysInRequest
);

} // namespace NYql::NDq
