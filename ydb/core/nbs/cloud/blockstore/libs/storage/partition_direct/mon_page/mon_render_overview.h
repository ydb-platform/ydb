#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <util/stream/fwd.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TMonPageData;

// Renders the Overview page.
void RenderOverview(
    IOutputStream& str,
    const TMonPageData& data,
    const TVChunkConfigs& vChunkConfigs,
    const ITouchedProvider& touchedProvider);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
