#pragma once

#include <ydb/core/fq/libs/config/protos/compute.pb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/ycloud/impl/grpc_service_settings.h>

#include <library/cpp/actors/core/actor.h>

namespace NFq {

NActors::TActorId ComputeDatabaseControlPlaneServiceActorId();

std::unique_ptr<NActors::IActor> CreateComputeDatabaseControlPlaneServiceActor(const NFq::NConfig::TComputeConfig& config, const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory);

std::unique_ptr<NActors::IActor> CreateYdbcpGrpcClientActor(const NCloud::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider);

std::unique_ptr<NActors::IActor> CreateCmsGrpcClientActor(const NCloud::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider);

}
