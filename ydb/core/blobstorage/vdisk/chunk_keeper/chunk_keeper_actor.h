#pragma once

#include <ydb/core/blobstorage/defs.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>

namespace NKikimr {

IActor* CreateChunkKeeperActor(TIntrusivePtr<TVDiskContext> vctx,
        NKikimrVDiskData::TChunkKeeperEntryPoint entryPoint);

} // namespace NKikimr
