#pragma once

#include <ydb/core/blobstorage/defs.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_log_context.h>

#include "chunk_keeper_data.h"

namespace NKikimr {

IActor* CreateChunkKeeperActor(const TIntrusivePtr<TVDiskLogContext>& logCtx,
        std::unique_ptr<TChunkKeeperData>&& data);

} // namespace NKikimr
