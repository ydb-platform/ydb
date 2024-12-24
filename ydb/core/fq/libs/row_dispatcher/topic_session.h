#pragma once

#include <ydb/core/fq/libs/config/protos/row_dispatcher.pb.h>
#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

#include <ydb/library/actors/core/actor.h>

#include <memory>

namespace NFq {

std::unique_ptr<NActors::IActor> NewTopicSession(
    const TString& readGroup,
    const TString& topicPath,
    const TString& endpoint,
    const TString& database,
    const NConfig::TRowDispatcherConfig& config,
    NActors::TActorId rowDispatcherActorId,
    NActors::TActorId compileServiceActorId,
    ui32 partitionId,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const ::NMonitoring::TDynamicCounterPtr& countersRoot,
    const NYql::IPqGateway::TPtr& pqGateway,
    ui64 maxBufferSize);

} // namespace NFq
