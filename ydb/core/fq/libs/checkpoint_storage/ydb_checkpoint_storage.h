#pragma once

#include "checkpoint_storage.h"

#include <ydb/core/fq/libs/common/entity_id.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/ydb_connection.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

//std::tuple<TCheckpointStoragePtr, NActors::IActor*> NewYdbCheckpointStorage(
TCheckpointStoragePtr NewYdbCheckpointStorage(
    const TExternalStorageSettings& config,
    const IEntityIdGenerator::TPtr& entityIdGenerator,
    const IYdbConnection::TPtr& ydbConnection);

} // namespace NFq
