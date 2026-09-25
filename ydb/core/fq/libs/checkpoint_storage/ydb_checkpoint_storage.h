#pragma once

#include "checkpoint_storage.h"

#include <ydb/core/fq/libs/checkpointing/checkpoint_provider_integration.h>
#include <ydb/core/fq/libs/common/entity_id.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

TCheckpointStoragePtr NewYdbCheckpointStorage(
    const TExternalStorageSettings& config,
    const IEntityIdGenerator::TPtr& entityIdGenerator,
    const IYdbConnection::TPtr& ydbConnection,
    TCheckpointProviderIntegrations checkpointProviderIntegrations = {});

} // namespace NFq
