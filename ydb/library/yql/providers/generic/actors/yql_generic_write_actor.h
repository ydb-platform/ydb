#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/generic/proto/source.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>

namespace NYql::NDq {

    std::pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>
    CreateGenericWriteActor(
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
        NConnector::IClient::TPtr genericClient,
        Generic::TSink&& sink,
        ui64 outputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        const THashMap<TString, TString>& secureParams,
        NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks* callbacks,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

} // namespace NYql::NDq
