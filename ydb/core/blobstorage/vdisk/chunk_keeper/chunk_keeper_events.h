#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr {

struct TEvChunkKeeperAllocate : TEventLocal<TEvChunkKeeperAllocate, TEvBlobStorage::EvChunkKeeperAllocate> {
    using TSubsystem = NKikimrVDiskData::TChunkKeeperEntryPoint::ESubsystem;
    TSubsystem Subsystem;

    TEvChunkKeeperAllocate(TSubsystem subsystem);
};

struct TEvChunkKeeperAllocateResult : TEventLocal<TEvChunkKeeperAllocateResult,
        TEvBlobStorage::EvChunkKeeperAllocateResult> {
    std::optional<ui32> ChunkIdx;
    NKikimrProto::EReplyStatus Status;
    TString ErrorReason;

    TEvChunkKeeperAllocateResult(std::optional<ui32> chunkIdx, NKikimrProto::EReplyStatus status,
            TString errorReason = "");
};

struct TEvChunkKeeperFree : TEventLocal<TEvChunkKeeperFree, TEvBlobStorage::EvChunkKeeperFree> {
    using TSubsystem = NKikimrVDiskData::TChunkKeeperEntryPoint::ESubsystem;

    ui32 ChunkIdx;
    TSubsystem Subsystem;

    TEvChunkKeeperFree(ui32 chunkIdx, TSubsystem subsystem);
};

struct TEvChunkKeeperFreeResult : TEventLocal<TEvChunkKeeperFreeResult, TEvBlobStorage::EvChunkKeeperFreeResult> {
    ui32 ChunkIdx;
    NKikimrProto::EReplyStatus Status;
    TString ErrorReason;

    TEvChunkKeeperFreeResult(ui32 chunkIdx, NKikimrProto::EReplyStatus status, TString errorReason = "");
};

struct TEvChunkKeeperDiscover : TEventLocal<TEvChunkKeeperDiscover, TEvBlobStorage::EvChunkKeeperDiscover> {
    using TSubsystem = NKikimrVDiskData::TChunkKeeperEntryPoint::ESubsystem;

    TSubsystem Subsystem;

    TEvChunkKeeperDiscover(TSubsystem subsystem);
};

struct TEvChunkKeeperDiscoverResult : TEventLocal<TEvChunkKeeperDiscoverResult,
        TEvBlobStorage::EvChunkKeeperDiscoverResult> {
    std::vector<ui32> Chunks;

    // Always succeeds
    // NKikimrProto::EReplyStatus Status;
    // TString ErrorReason;

    TEvChunkKeeperDiscoverResult(std::vector<ui32> chunks);
};

} // namespace NKikimr
