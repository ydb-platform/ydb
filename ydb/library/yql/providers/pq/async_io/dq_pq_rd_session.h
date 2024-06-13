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
#include <ydb/library/security/ydb_credentials_provider_factory.h>

namespace NYql::NDq {

std::unique_ptr<NActors::IActor> NewPqSession(
    const NPq::NProto::TDqPqTopicSource& settings,
    ui32 inputIndex,
    NActors::TActorId rowDispatcherActorId,
    const TString& token,
    bool addBearerToToken);

} // namespace NYql::NDq
