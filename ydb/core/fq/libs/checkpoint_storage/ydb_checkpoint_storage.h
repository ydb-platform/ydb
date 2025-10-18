#pragma once

#include "checkpoint_storage.h"

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/core/fq/libs/common/entity_id.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/ydb_connection.h>

namespace NKikimrConfig {
class TCheckpointsConfig;
} // namespace NKikimrConfig

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

//std::tuple<TCheckpointStoragePtr, NActors::IActor*> NewYdbCheckpointStorage(
TCheckpointStoragePtr NewYdbCheckpointStorage(
    const NKikimrConfig::TExternalStorage& config,
    const IEntityIdGenerator::TPtr& entityIdGenerator,
    const IYdbConnection::TPtr& ydbConnection);

} // namespace NFq
