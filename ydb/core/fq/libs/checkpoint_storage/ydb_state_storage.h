#pragma once

#include "state_storage.h"

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/core/fq/libs/config/protos/checkpoint_coordinator.pb.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

TStateStoragePtr NewYdbStateStorage(
    const NConfig::TCheckpointCoordinatorConfig& config,
    const TYdbConnectionPtr& ydbConnection);

} // namespace NFq
