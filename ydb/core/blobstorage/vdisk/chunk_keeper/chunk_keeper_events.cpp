#include "chunk_keeper_events.h"

namespace NKikimr {

TEvChunkKeeperAllocate::TEvChunkKeeperAllocate(TChunkOwner chunkOwner)
    : ChunkOwner(chunkOwner)
{}

TEvChunkKeeperAllocateResult::TEvChunkKeeperAllocateResult(std::optional<ui32> chunkIdx)
    : ChunkIdx(chunkIdx)
{}

TEvChunkKeeperFree::TEvChunkKeeperFree(ui32 chunkIdx, TChunkOwner chunkOwner)
    : ChunkIdx(chunkIdx)
    , ChunkOwner(chunkOwner)
{}


TEvChunkKeeperFreeResult::TEvChunkKeeperFreeResult(ui32 chunkIdx, NKikimrProto::EReplyStatus status, TString errorReason)
    : ChunkIdx(chunkIdx),
    , Status(status)
    , ErrorReason(errorReason)
{}

} // namespace NKikimr
