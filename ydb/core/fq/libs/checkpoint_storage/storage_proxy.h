#pragma once

#include <ydb/core/fq/libs/config/protos/checkpoint_coordinator.pb.h>
#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/library/actors/core/actor.h>

#include <memory>

namespace NFq {

std::unique_ptr<NActors::IActor> NewStorageProxy(
    const NConfig::TCheckpointCoordinatorConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources);

} // namespace NFq
