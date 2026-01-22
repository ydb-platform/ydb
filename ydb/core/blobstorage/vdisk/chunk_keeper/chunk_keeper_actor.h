#pragma once

#include <ydb/core/blobstorage/defs.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_log_context.h>

namespace NKikimr {

IActor* CreateChunkKeeperActor(TIntrusivePtr<TLogContext> logCtx,
        NKikimrVDiskData::TChunkKeeperEntryPoint entryPoint);

} // namespace NKikimr
