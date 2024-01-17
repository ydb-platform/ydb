#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/size_literals.h>
#include <util/system/types.h>

namespace NYql::NDq {

constexpr i64 DqPqDefaultFreeSpace = 16_MB;

std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateDqPqWriteActor(
    NPq::NProto::TDqPqTopicSink&& settings,
    ui64 outputIndex,
    TCollectStatsLevel statsLevel,
    TTxId txId,
    const THashMap<TString, TString>& secureParams,
    NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IDqComputeActorAsyncOutput::ICallbacks* callbacks,
    i64 freeSpace = DqPqDefaultFreeSpace);

void RegisterDqPqWriteActorFactory(TDqAsyncIoFactory& factory, NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

} // namespace NYql::NDq
