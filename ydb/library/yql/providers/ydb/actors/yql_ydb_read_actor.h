#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/ydb/proto/source.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace NYql::NDq {

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateYdbReadActor(
    NYql::NYdb::TSource&& params,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    const NActors::TActorId& computeActorId,
    ::NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

} // namespace NYql::NDq
