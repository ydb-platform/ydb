#pragma once

#include "state_storage.h"

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

TStateStoragePtr NewYdbStateStorage(
    const NKikimrConfig::TCheckpointsConfig& config,
    const TYdbConnectionPtr& ydbConnection);

} // namespace NFq
