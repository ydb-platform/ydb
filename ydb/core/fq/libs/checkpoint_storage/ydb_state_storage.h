#pragma once

#include "state_storage.h"

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/ydb/ydb_connection.h>

namespace NKikimrConfig {
class TCheckpointsConfig;
} // namespace NKikimrConfig

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

TStateStoragePtr NewYdbStateStorage(
    const NKikimrConfig::TCheckpointsConfig& config,
    const IYdbConnection::TPtr& ydbConnection);

} // namespace NFq
