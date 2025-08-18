#pragma once

#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/library/actors/core/actor.h>

#include <memory>

namespace NKikimrConfig {
class TCheckpointsConfig;
} // namespace NKikimrConfig

namespace NFq {

std::unique_ptr<NActors::IActor> NewCheckpointStorageService(
    const NKikimrConfig::TCheckpointsConfig& config,
    const TString& idsPrefix,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    const ::NMonitoring::TDynamicCounterPtr& counters);

} // namespace NFq
