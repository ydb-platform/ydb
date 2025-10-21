#pragma once

#include "storage_settings.h"

#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <memory>

namespace NFq {

std::unique_ptr<NActors::IActor> NewCheckpointStorageService(
    const TCheckpointStorageSettings& config,
    const TString& idsPrefix,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYdb::TDriver driver,
    const ::NMonitoring::TDynamicCounterPtr& counters);

} // namespace NFq
