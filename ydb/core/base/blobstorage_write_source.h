#pragma once

#include <limits>
#include <type_traits>

#include <util/system/types.h>

namespace NKikimr {

enum class TWriteSource : ui16 {
    // Source is not classified; the producer did not attach an operation code.
    Unknown = 0,

    // Tablet WriteLog component writes the primary serialized tablet log entry blob.
    WriteLogEntry = 1,

    // Tablet WriteLog component writes reference blobs linked from the same log entry.
    WriteLogReference = 2,

    // Tablet block component writes generation block markers for BlobStorage.
    BlockBlobStorage = 3,

    // Tablet system GC component writes log-channel barrier advancement records.
    GcLogChannel = 4,

    // Tablet deletion component writes terminal hard barriers across tablet channels.
    DeleteHardBarrier = 5,

    // Tablet FlatExecutor compaction component writes blobs produced by page compaction.
    FlatCompactionPut = 6,

    // Tablet FlatExecutor GC component writes keep/do-not-keep sets and barrier advancement.
    FlatCollectGarbage = 7,

    // VDisk Skeleton handoff-delete component writes delete records for handoff LogoBlobs.
    SkeletonHandoffDelLogoBlob = 101,

    // VDisk Skeleton bulk-SST import component writes records for attaching prebuilt SST data.
    SkeletonAddBulkSst = 102,

    // VDisk Skeleton local-sync component writes synchronization payload records.
    SkeletonLocalSyncData = 103,

    // VDisk Skeleton repair component writes reconciliation updates from Anubis/Osiris.
    SkeletonAnubisOsirisPut = 104,

    // VDisk Skeleton phantom-blob component writes phantom detection records.
    SkeletonPhantomBlobs = 105,

    // VDisk SyncLogKeeper committer writes SyncLog data pages into chunk storage.
    SyncLogCommitterWrite = 106,

    // VDisk SyncLogKeeper committer writes SyncLog entry-point checkpoints.
    SyncLogCommitterCommit = 107,

    // VDisk HugeKeeper allocation component writes chunk allocation commits.
    HugeKeeperAllocChunk = 109,

    // VDisk HugeKeeper reclamation component writes chunk release commits.
    HugeKeeperFreeChunk = 110,

    // VDisk HugeKeeper checkpoint component writes entry-point state.
    HugeKeeperEntryPoint = 111,

    // VDisk Hull DB commit component writes new Hull entry-point and related chunk metadata.
    HullDbCommit = 112,

    // VDisk Hull compaction component writes non-inline compaction output.
    HullCompactWorkerWrite = 113,

    // VDisk Hull SST writer component writes buffered SST fragments into chunks.
    HullWriteSst = 114,

    // VDisk ChunkKeeper component writes entry-point updates for chunk ownership state.
    ChunkKeeperCommit = 115,

    // VDisk metadata component writes metadata entry-point checkpoints.
    MetadataCommit = 116,

    // VDisk scrub repair component writes corrected data for damaged parts.
    ScrubWrite = 117,

    // VDisk scrub component writes scrub-state checkpoints.
    ScrubCommit = 118,

    // VDisk log-cutter component writes FirstLsnToKeep progression.
    LogCutterCutLog = 119,

    // VDisk syncer component writes synchronization state checkpoints.
    SyncerCommit = 120,

    // VDisk recovery component writes recovered huge blobs back into normal storage.
    RecoveredHugeBlob = 121,

    // BlobStorage load-test component writes synthetic workload traffic.
    GroupWriteLoadActor = 122,
}; // Don't forget to update IsKnownWriteSource function when adding new values here.

constexpr TWriteSource UnknownWriteSource() {
    return TWriteSource::Unknown;
}

constexpr bool IsKnownWriteSource(TWriteSource op) {
    switch (op) {
        case TWriteSource::Unknown:
        case TWriteSource::WriteLogEntry:
        case TWriteSource::WriteLogReference:
        case TWriteSource::BlockBlobStorage:
        case TWriteSource::GcLogChannel:
        case TWriteSource::DeleteHardBarrier:
        case TWriteSource::FlatCompactionPut:
        case TWriteSource::FlatCollectGarbage:
        case TWriteSource::SkeletonHandoffDelLogoBlob:
        case TWriteSource::SkeletonAddBulkSst:
        case TWriteSource::SkeletonLocalSyncData:
        case TWriteSource::SkeletonAnubisOsirisPut:
        case TWriteSource::SkeletonPhantomBlobs:
        case TWriteSource::SyncLogCommitterWrite:
        case TWriteSource::SyncLogCommitterCommit:
        case TWriteSource::HugeKeeperAllocChunk:
        case TWriteSource::HugeKeeperFreeChunk:
        case TWriteSource::HugeKeeperEntryPoint:
        case TWriteSource::HullDbCommit:
        case TWriteSource::HullCompactWorkerWrite:
        case TWriteSource::HullWriteSst:
        case TWriteSource::ChunkKeeperCommit:
        case TWriteSource::MetadataCommit:
        case TWriteSource::ScrubWrite:
        case TWriteSource::ScrubCommit:
        case TWriteSource::LogCutterCutLog:
        case TWriteSource::SyncerCommit:
        case TWriteSource::RecoveredHugeBlob:
        case TWriteSource::GroupWriteLoadActor:
            return true;
    }
    return false;
}

constexpr TWriteSource WriteSourceFromProto(ui32 op) {
    using TUnderlying = std::underlying_type_t<TWriteSource>;
    if (op > static_cast<ui32>(std::numeric_limits<TUnderlying>::max())) {
        return UnknownWriteSource();
    }

    const TWriteSource value = static_cast<TWriteSource>(static_cast<TUnderlying>(op));
    return IsKnownWriteSource(value)
        ? value
        : UnknownWriteSource();
}

constexpr ui32 WriteSourceToProto(TWriteSource op) {
    return IsKnownWriteSource(op)
        ? static_cast<ui32>(op)
        : static_cast<ui32>(UnknownWriteSource());
}

} // namespace NKikimr
