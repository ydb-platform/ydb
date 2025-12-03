#pragma once

#include "state_storage.h"
#include "storage_settings.h"

#include <ydb/core/fq/libs/ydb/ydb.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

TStateStoragePtr NewYdbStateStorage(
    const TCheckpointStorageSettings& config,
    const IYdbConnection::TPtr& ydbConnection);

} // namespace NFq
