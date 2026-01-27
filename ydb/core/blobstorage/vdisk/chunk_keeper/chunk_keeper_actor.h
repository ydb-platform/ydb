#pragma once

#include <ydb/core/blobstorage/defs.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_log_context.h>

#include "chunk_keeper_data.h"

namespace NKikimr {

//////////////////////////////////////////////////////////////////////////////////////////
/// Contract
//////////////////////////////////////////////////////////////////////////////////////////
/// 1. TEvChunkKeeperAllocate(subsystem)
/// - Allocates chunk on PDisk and persistently commit it as owned by given subsystem
/// - May return ERROR on PDisk failures
///
/// 2. TEvChunkKeeperFree(chunkIdx, subsystem)
/// - Deallocates chunk with given chunkIdx if it is owned by given subsystem
/// - Returns ERROR if chunk is not owned by subsystem
/// - May return ERROR on PDisk failures
///
/// 3. TEvChunkKeeperDiscover(subsystem)
/// - Returns list of committed chunks owned by given subsystem
/// - May return chunks allocated TEvChunkKeeperAllocate before subsystem
///   receives TEvChunkKeeperAllocateResult
/// - May not return chunks deallocated by TEvChunkKeeperFree before subsystem
///   receives TEvChunkKeeperFreeResult
/// - Always succeeds
///
/// Simultaneous allocation and/or deallocation requests in the same subsystem
/// are not allowed
//////////////////////////////////////////////////////////////////////////////////////////

IActor* CreateChunkKeeperActor(const TIntrusivePtr<TVDiskLogContext>& logCtx,
        std::unique_ptr<TChunkKeeperData>&& data);

} // namespace NKikimr
