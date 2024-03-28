#pragma once

#include <ydb/core/fq/libs/config/protos/compute.pb.h>

#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/fq/libs/signer/signer.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/grpc/actor_client/grpc_service_settings.h>

#include <ydb/library/actors/core/actor.h>

namespace NFq {

NActors::TActorId ComputeDatabaseControlPlaneServiceActorId();

std::unique_ptr<NActors::IActor> CreateComputeDatabaseControlPlaneServiceActor(const NFq::NConfig::TComputeConfig& config,
                                                                               const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
                                                                               const NConfig::TCommonConfig& commonConfig,
                                                                               const TSigner::TPtr& signer,
                                                                               const TYqSharedResources::TPtr& yqSharedResources,
                                                                               const ::NMonitoring::TDynamicCounterPtr& counters);

std::unique_ptr<NActors::IActor> CreateYdbcpGrpcClientActor(const NGrpcActorClient::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider);

std::unique_ptr<NActors::IActor> CreateCmsGrpcClientActor(const NGrpcActorClient::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider);

std::unique_ptr<NActors::IActor> CreateComputeDatabasesCacheActor(const NActors::TActorId& databaseClientActorId, const TString& databasesCacheReloadPeriod, const ::NMonitoring::TDynamicCounterPtr& counters);

std::unique_ptr<NActors::IActor> CreateMonitoringGrpcClientActor(const NGrpcActorClient::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider);
std::unique_ptr<NActors::IActor> CreateMonitoringRestClientActor(const TString& endpoint, const TString& database, const NYdb::TCredentialsProviderPtr& credentialsProvider);

std::unique_ptr<NActors::IActor> CreateDatabaseMonitoringActor(const NActors::TActorId& monitoringClientActorId, NFq::NConfig::TLoadControlConfig config, const ::NMonitoring::TDynamicCounterPtr& counters);

}
