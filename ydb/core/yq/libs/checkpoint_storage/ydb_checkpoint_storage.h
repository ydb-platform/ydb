#pragma once

#include "checkpoint_storage.h"

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/core/yq/libs/common/entity_id.h>
#include <ydb/core/yq/libs/config/protos/storage.pb.h>
#include <ydb/core/yq/libs/shared_resources/shared_resources.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

TCheckpointStoragePtr NewYdbCheckpointStorage(
    const NConfig::TYdbStorageConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const IEntityIdGenerator::TPtr& entityIdGenerator,
    const TYqSharedResources::TPtr& yqSharedResources);

} // namespace NFq
