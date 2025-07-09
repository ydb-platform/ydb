#pragma once

#include "checkpoint_storage.h"

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/core/fq/libs/common/entity_id.h>
#include <ydb/core/fq/libs/config/protos/storage.pb.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

TCheckpointStoragePtr NewYdbCheckpointStorage(
    const NConfig::TYdbStorageConfig& config,
    const IEntityIdGenerator::TPtr& entityIdGenerator,
    const TYdbConnectionPtr& ydbConnection);

} // namespace NFq
