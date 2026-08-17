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

TString TEvChunkKeeperAllocateResult::ToString() const {
    TStringStream str;
    str << "TEvChunkKeeperAllocateResult{";
    str << " ChunkIdx# ";
    if (ChunkIdx) {
        str << *ChunkIdx;
    } else {
        str << "<nullopt>";
    }
    str << " Status# " << NKikimrProto::EReplyStatus_Name(Status);
    if (!ErrorReason.empty()) {    
        str << " ErrorReason# " << ErrorReason;
    }
    str << " }";
    return str.Str();
}

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

TString TEvChunkKeeperFreeResult::ToString() const {
    TStringStream str;
    str << "TEvChunkKeeperFreeResult{";
    str << " ChunkIdx# " << ChunkIdx;
    str << " Status# " << NKikimrProto::EReplyStatus_Name(Status);
    if (!ErrorReason.empty()) {    
        str << " ErrorReason# " << ErrorReason;
    }
    str << " }";
    return str.Str();
}

TEvChunkKeeperDiscover::TEvChunkKeeperDiscover(TSubsystem subsystem)
    : Subsystem(subsystem)
{}


TEvChunkKeeperDiscoverResult::TEvChunkKeeperDiscoverResult(std::vector<TChunkInfo>&& chunks,
        NKikimrProto::EReplyStatus status, TString errorReason)
    : Chunks(std::move(chunks))
    , Status(status)
    , ErrorReason(errorReason)
{}

TString TEvChunkKeeperDiscoverResult::ToString() const {
    TStringStream str;
    str << "TEvChunkKeeperDiscoverResult{";
    str << " Status# " << NKikimrProto::EReplyStatus_Name(Status);
    if (!ErrorReason.empty()) {    
        str << " ErrorReason# " << ErrorReason;
    }
    str << " ChunkCount# " << Chunks.size();
    str << " Chunks# [";
    for (const TChunkInfo& chunk : Chunks) {
        str << " { ChunkIdx# " << chunk.ChunkIdx;
        str << " ShredRequested# " << chunk.ShredRequested << " }";
    }
    str << " ] }";
    return str.Str();
}

} // namespace NKikimr
