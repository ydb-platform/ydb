#include "chunk_keeper_events.h"

namespace NKikimr {

TEvChunkKeeperAllocate::TEvChunkKeeperAllocate(TSubsystem subsystem)
    : Subsystem(subsystem)
{}

TEvChunkKeeperAllocateResult::TEvChunkKeeperAllocateResult(std::optional<ui32> chunkIdx,
        NKikimrProto::EReplyStatus status, TString errorReason)
    : ChunkIdx(chunkIdx)
    , Status(status)
    , ErrorReason(errorReason)
{}

TEvChunkKeeperFree::TEvChunkKeeperFree(ui32 chunkIdx, TSubsystem subsystem)
    : ChunkIdx(chunkIdx)
    , Subsystem(subsystem)
{}


TEvChunkKeeperFreeResult::TEvChunkKeeperFreeResult(ui32 chunkIdx, NKikimrProto::EReplyStatus status,
        TString errorReason)
    : ChunkIdx(chunkIdx)
    , Status(status)
    , ErrorReason(errorReason)
{}

TEvChunkKeeperDiscover::TEvChunkKeeperDiscover(TSubsystem subsystem)
    : Subsystem(subsystem)
{}


TEvChunkKeeperDiscoverResult::TEvChunkKeeperDiscoverResult(std::vector<ui32> chunks)
    : Chunks(chunks)
{}

} // namespace NKikimr
