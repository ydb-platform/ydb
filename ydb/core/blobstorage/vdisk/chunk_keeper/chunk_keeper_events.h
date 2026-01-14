#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr {

struct TEvChunkKeeperAllocate : TEventLocal<TEvChunkKeeperAllocate, EvChunkKeeperAllocate> {
    using TChunkOwner = NKikimrVDiskData::TChunkKeeperEntryPoint::EChunkOwner;

    TChunkOwner ChunkOwner;

    TEvChunkKeeperAllocate(TChunkOwner chunkOwner);
};

struct TEvChunkKeeperAllocateResult : TEventLocal<TEvChunkKeeperAllocateResult, EvChunkKeeperAllocateResult> {
    std::optional<ui32> ChunkIdx;
    TString ErrorReason;

    TEvChunkKeeperAllocateResult(std::optional<ui32> chunkIdx, TString errorReason = "");
};

struct TEvChunkKeeperFree : TEventLocal<TEvChunkKeeperFree, EvChunkKeeperFree> {
    using TChunkOwner = NKikimrVDiskData::TChunkKeeperEntryPoint::EChunkOwner;

    ui32 ChunkIdx;
    TChunkOwner ChunkOwner;

    TEvChunkKeeperFree(ui32 chunkIdx, TChunkOwner chunkOwner);
};

struct TEvChunkKeeperFreeResult : TEventLocal<TEvChunkKeeperFreeResult, EvChunkKeeperFreeResult> {
    ui32 ChunkIdx;
    NKikimrProto::EReplyStatus Status;
    TString ErrorReason;

    TEvChunkKeeperFreeResult(ui32 chunkIdx, NKikimrProto::EReplyStatus status, TString errorReason = "");
};

} // namespace NKikimr
