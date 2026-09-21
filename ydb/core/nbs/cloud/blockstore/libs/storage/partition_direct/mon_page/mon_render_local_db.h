#pragma once

#include "mon_model.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <util/stream/output.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Renders the state persisted in the partition tablet's Local DB.
void RenderLocalDb(
    IOutputStream& str,
    const TLocalDbContents& db,
    const TVChunkConfigs& vChunkConfigs);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
