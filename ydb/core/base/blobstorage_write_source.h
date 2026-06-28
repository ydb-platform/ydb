#pragma once

#include <limits>
#include <type_traits>

#include <util/system/types.h>

namespace NKikimr {

// XX(enum name, stable proto value)
#define YDB_BLOBSTORAGE_WRITE_SOURCES(XX) \
    /* Source is not classified; the producer did not attach an operation code. */ \
    XX(Unknown, 0) \
    \
    /* Tablet part: */ \
    \
    /* Tablet WriteLog component writes the primary serialized tablet log entry blob. */ \
    XX(WriteLogEntry, 1) \
    /* Tablet WriteLog component writes reference blobs linked from the same log entry. */ \
    XX(WriteLogReference, 2) \
    /* Tablet block component writes generation block markers for BlobStorage. */ \
    XX(BlockBlobStorage, 3) \
    /* Tablet system GC component writes log-channel barrier advancement records. */ \
    XX(GcLogChannel, 4) \
    /* Tablet deletion component writes terminal hard barriers across tablet channels. */ \
    XX(DeleteHardBarrier, 5) \
    /* Tablet FlatExecutor compaction component writes blobs produced by page compaction. */ \
    XX(FlatCompactionPut, 6) \
    /* Tablet FlatExecutor GC component writes keep/do-not-keep sets and barrier advancement. */ \
    XX(FlatCollectGarbage, 7) \
    \
    /* VDisk part: */ \
    \
    /* VDisk Skeleton handoff-delete component writes delete records for handoff LogoBlobs. */ \
    XX(SkeletonHandoffDelLogoBlob, 1001) \
    /* VDisk Skeleton bulk-SST import component writes records for attaching prebuilt SST data. */ \
    XX(SkeletonAddBulkSst, 1002) \
    /* VDisk Skeleton local-sync component writes synchronization payload records. */ \
    XX(SkeletonLocalSyncData, 1003) \
    /* VDisk Skeleton repair component writes reconciliation updates from Anubis/Osiris. */ \
    XX(SkeletonAnubisOsirisPut, 1004) \
    /* VDisk Skeleton phantom-blob component writes phantom detection records. */ \
    XX(SkeletonPhantomBlobs, 1005) \
    /* VDisk SyncLogKeeper committer writes SyncLog data pages into chunk storage. */ \
    XX(SyncLogCommitterWrite, 1006) \
    /* VDisk SyncLogKeeper committer writes SyncLog entry-point checkpoints. */ \
    XX(SyncLogCommitterCommit, 1007) \
    /* VDisk HugeKeeper allocation component writes chunk allocation commits. */ \
    XX(HugeKeeperAllocChunk, 1008) \
    /* VDisk HugeKeeper reclamation component writes chunk release commits. */ \
    XX(HugeKeeperFreeChunk, 1009) \
    /* VDisk HugeKeeper checkpoint component writes entry-point state. */ \
    XX(HugeKeeperEntryPoint, 1010) \
    /* IncrHuge keeper logger writes chunk queue state. */ \
    XX(HugeKeeperProcessChunkQueue, 1011) \
    /* IncrHuge keeper logger writes blob deletion queue state. */ \
    XX(HugeKeeperProcessDeleteChunkQueueItem, 1012) \
    /* VDisk Hull DB commit component writes new Hull entry-point and related chunk metadata. */ \
    XX(HullDbCommit, 1013) \
    /* VDisk Hull compaction component writes non-inline compaction output. */ \
    XX(HullCompactWorkerWrite, 1014) \
    /* VDisk Hull SST writer component writes buffered SST fragments into chunks. */ \
    XX(HullWriteSst, 1015) \
    /* VDisk ChunkKeeper component writes entry-point updates for chunk ownership state. */ \
    XX(ChunkKeeperCommit, 1016) \
    /* VDisk metadata component writes metadata entry-point checkpoints. */ \
    XX(MetadataCommit, 1017) \
    /* VDisk scrub repair component writes regenerated SST index data. */ \
    XX(ScrubWrite, 1018) \
    /* VDisk scrub component writes scrub-state checkpoints. */ \
    XX(ScrubCommit, 1019) \
    /* VDisk log-cutter component writes FirstLsnToKeep progression. */ \
    XX(LogCutterCutLog, 1020) \
    /* VDisk syncer component writes synchronization state checkpoints. */ \
    XX(SyncerCommit, 1021) \
    /* Bridge syncer writes garbage-collection barriers and flags during pile merge. */ \
    XX(SyncerMergeGC, 1022) \
    /* Bridge syncer writes tablet block records during pile merge. */ \
    XX(SyncerMergeBlock, 1023) \
    /* VDisk recovery component writes recovered huge blobs back into normal storage. */ \
    XX(RecoveredHugeBlob, 1024) \
    /* VDisk blob balancer component writes blobs as part of rebalancing operations. */ \
    XX(BlobBalancer, 1025) \
    /* VDisk scrub restore component writes reconstructed corrupted blob parts. */ \
    XX(RestoredCorruptedBlob, 1026) \
    /* DSProxy GET acceleration writes restored parts back to VDisks. */ \
    XX(DSProxyGetAccelerate, 1027) \
    /* VDisk defragmentation component rewrites blobs into new locations. */ \
    XX(DefragRewrite, 1028) \
    /* VDisk Skeleton VPatch component writes patched blob parts. */ \
    XX(SkeletonVPatch, 1029) \
    /* VDisk Skeleton force-block helper writes block records. */ \
    XX(SkeletonForceBlock, 1030) \
    /* IncrHuge keeper writes huge blob data chunks. */ \
    XX(IncrHugeWrite, 1031) \
    /* IncrHuge keeper writes index chunks. */ \
    XX(IncrHugeIndexWrite, 1032) \
    /* DSProxy PATCH fallback writes fully patched blobs through regular Put. */ \
    XX(DSProxyPatch, 1033) \
    /* Bridge syncer writes blobs while merging data between bridge piles. */ \
    XX(SyncerMergePut, 1034) \
    /* Bridge proxy restores missing blob copies detected by read/discover/range requests. */ \
    XX(BridgeProxyRestorePut, 1035) \
    /* VDisk Skeleton VMovedPatch fallback writes fully patched blobs through regular Put. */ \
    XX(SkeletonVMovedPatch, 1036) \
    \
    /* PDisk: */ \
    \
    /* PDisk writes padding data to fill up a chunk after shredding. */ \
    XX(ShredPadding, 2001) \
    \
    /* DDisk: */ \
    \
    /* DDiskActorBoot component writes initial log records during DDisk boot phase. */ \
    XX(DDiskBoot, 3001) \
    \
    /* BlobDepot */ \
    \
    /* BlobDepot component writes block records for blocked tablets. */ \
    XX(BlobDepotBlock, 4001) \
    /* BlobDepot component writes garbage-collection barriers and flags. */ \
    XX(BlobDepotGC, 4002) \
    /* BlobDepot component writes copied/restored blobs during assimilation and decommit. */ \
    XX(BlobDepotPut, 4003) \
    \
    /* KV: */ \
    \
    /* KeyValue component writes user blobs. */ \
    XX(KeyValuePut, 5001) \
    /* KeyValue component writes garbage-collection barriers and flags. */ \
    XX(KeyValueGC, 5002) \
    \
    /* ColumnShard: */ \
    \
    /* ColumnShard component writes blob batches. */ \
    XX(ColumnShardPut, 6001) \
    /* ColumnShard component writes garbage-collection barriers and flags. */ \
    XX(ColumnShardGC, 6002) \
    \
    /* Misc: */ \
    \
    /* BlobStorage load-test component writes synthetic workload traffic. */ \
    XX(GroupWriteLoadActor, 7001)

enum class TWriteSource : ui16 {
#define YDB_WRITE_SOURCE_ENUM(name, value) name = value,
    YDB_BLOBSTORAGE_WRITE_SOURCES(YDB_WRITE_SOURCE_ENUM)
#undef YDB_WRITE_SOURCE_ENUM
};

namespace NWriteSourcePrivate {

enum class EWriteSourceOrdinal : size_t {
#define YDB_WRITE_SOURCE_ORDINAL(name, value) name,
    YDB_BLOBSTORAGE_WRITE_SOURCES(YDB_WRITE_SOURCE_ORDINAL)
#undef YDB_WRITE_SOURCE_ORDINAL
    Count
};

} // namespace NWriteSourcePrivate

struct TWriteSourceInfo {
    TWriteSource Source;
    size_t Ordinal;
    const char* Name;
};

constexpr TWriteSource UnknownWriteSource() {
    return TWriteSource::Unknown;
}

constexpr size_t UnknownWriteSourceOrdinal() {
    return static_cast<size_t>(NWriteSourcePrivate::EWriteSourceOrdinal::Unknown);
}

constexpr size_t WriteSourceCount() {
    return static_cast<size_t>(NWriteSourcePrivate::EWriteSourceOrdinal::Count);
}

constexpr bool IsKnownWriteSource(TWriteSource op) {
    switch (op) {
#define YDB_WRITE_SOURCE_CASE(name, value) case TWriteSource::name:
        YDB_BLOBSTORAGE_WRITE_SOURCES(YDB_WRITE_SOURCE_CASE)
#undef YDB_WRITE_SOURCE_CASE
            return true;
    }
    return false;
}

constexpr size_t WriteSourceOrdinal(TWriteSource source) {
    switch (source) {
#define YDB_WRITE_SOURCE_ORDINAL(name, value) \
        case TWriteSource::name: \
            return static_cast<size_t>(NWriteSourcePrivate::EWriteSourceOrdinal::name);
        YDB_BLOBSTORAGE_WRITE_SOURCES(YDB_WRITE_SOURCE_ORDINAL)
#undef YDB_WRITE_SOURCE_ORDINAL
    }

    return UnknownWriteSourceOrdinal();
}

constexpr const char* WriteSourceName(TWriteSource source) {
    switch (source) {
#define YDB_WRITE_SOURCE_NAME(name, value) case TWriteSource::name: return #name;
        YDB_BLOBSTORAGE_WRITE_SOURCES(YDB_WRITE_SOURCE_NAME)
#undef YDB_WRITE_SOURCE_NAME
    }

    return "Unknown";
}

template <typename TCallback>
constexpr void ForEachWriteSourceInfo(TCallback&& callback) {
#define YDB_WRITE_SOURCE_INFO(name, value) \
    callback(TWriteSourceInfo{ \
        TWriteSource::name, \
        static_cast<size_t>(NWriteSourcePrivate::EWriteSourceOrdinal::name), \
        #name \
    });
    YDB_BLOBSTORAGE_WRITE_SOURCES(YDB_WRITE_SOURCE_INFO)
#undef YDB_WRITE_SOURCE_INFO
}

constexpr bool ValidateWriteSources() {
    if (WriteSourceCount() == 0 || UnknownWriteSourceOrdinal() != 0) {
        return false;
    }

    bool valid = true;
    bool seen[WriteSourceCount()] = {};

    ForEachWriteSourceInfo([&](const TWriteSourceInfo& info) constexpr {
        if (info.Ordinal >= WriteSourceCount() || !info.Name || !IsKnownWriteSource(info.Source)) {
            valid = false;
            return;
        }

        if (seen[info.Ordinal]) {
            valid = false;
            return;
        }
        seen[info.Ordinal] = true;
    });

    if (!valid) {
        return false;
    }

    for (size_t i = 0; i < WriteSourceCount(); ++i) {
        if (!seen[i]) {
            return false;
        }
    }

    return true;
}

static_assert(ValidateWriteSources(),
    "WriteSource table must contain unique values and dense ordinals with Unknown at ordinal 0");

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

#undef YDB_BLOBSTORAGE_WRITE_SOURCES

} // namespace NKikimr
