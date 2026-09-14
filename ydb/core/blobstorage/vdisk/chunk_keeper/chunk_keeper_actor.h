#pragma once

#include <ydb/core/blobstorage/defs.h>

#include "chunk_keeper_ctx.h"
#include "chunk_keeper_data.h"

namespace NKikimr {

// Contract is described in chunk_keeper_events.h

IActor* CreateChunkKeeperActor(TChunkKeeperCtx&& ctx, std::unique_ptr<TChunkKeeperData>&& data,
        bool isActive, bool dropChunks);

} // namespace NKikimr
