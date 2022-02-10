#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_io_actors_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <library/cpp/actors/core/actor.h>

#include <util/generic/size_literals.h>
#include <util/system/types.h>


namespace NYql::NDq {
class TDqSourceFactory;

const i64 PQReadDefaultFreeSpace = 16_MB;

std::pair<IDqSourceActor*, NActors::IActor*> CreateDqPqReadActor(
    NPq::NProto::TDqPqTopicSource&& settings,
    ui64 inputIndex,
    TTxId txId,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IDqSourceActor::ICallbacks* callback,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    i64 bufferSize = PQReadDefaultFreeSpace
    );

void RegisterDqPqReadActorFactory(TDqSourceFactory& factory, NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

} // namespace NYql::NDq
