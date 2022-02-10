#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h> 
#include <ydb/library/yql/providers/ydb/proto/source.pb.h>

#include <library/cpp/actors/core/actor.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h> 

namespace NYql::NDq {

std::pair<NYql::NDq::IDqSourceActor*, NActors::IActor*> CreateYdbReadActor(
    NYql::NYdb::TSource&& params,
    ui64 inputIndex,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    NYql::NDq::IDqSourceActor::ICallbacks* callback,
    ::NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

} // namespace NYql::NDq
