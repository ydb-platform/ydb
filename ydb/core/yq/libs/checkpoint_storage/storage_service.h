#pragma once

#include <ydb/core/yq/libs/config/protos/checkpoint_coordinator.pb.h>
#include <ydb/core/yq/libs/config/protos/common.pb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
 
#include <library/cpp/actors/core/actor.h>

#include <memory>

namespace NYq {

std::unique_ptr<NActors::IActor> NewCheckpointStorageService( 
    const NConfig::TCheckpointCoordinatorConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory); 

} // namespace NYq
