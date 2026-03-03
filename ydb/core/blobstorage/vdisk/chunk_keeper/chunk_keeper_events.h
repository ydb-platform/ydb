#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

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
/// - Returns list of chunks committed as owned by given subsystem
/// - May return chunks allocated by TEvChunkKeeperAllocate before subsystem receives
///   TEvChunkKeeperAllocateResult when sent in-between these events
/// - May not return chunks deallocated by TEvChunkKeeperFree before subsystem receives
///   TEvChunkKeeperFreeResult when sent in-between these events
/// - Always succeeds
///
/// Simultaneous allocation and/or deallocation requests in the same subsystem
/// are not allowed
//////////////////////////////////////////////////////////////////////////////////////////

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
