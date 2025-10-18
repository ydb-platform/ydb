#pragma once

#include "state_storage.h"
#include "storage_settings.h"

#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

TStateStoragePtr NewYdbStateStorage(
    const TCheckpointStorageSettings& config,
    const TYdbConnectionPtr& ydbConnection);

} // namespace NFq
