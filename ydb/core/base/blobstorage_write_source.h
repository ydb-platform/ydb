#pragma once

#include "defs.h"

namespace NKikimr {

struct TWriteSource {
    enum class EOp : ui16 {
        Unknown = 0,
        WriteLogEntry = 1,
        WriteLogReference = 2,
        BlockBlobStorage = 3,
        GcLogChannel = 4,
        DeleteHardBarrier = 5,
        FlatCompactionPut = 6,
        FlatCollectGarbage = 7,
        SkeletonHandoffDelLogoBlob = 101,
        SkeletonAddBulkSst = 102,
        SkeletonLocalSyncData = 103,
        SkeletonAnubisOsirisPut = 104,
        SkeletonPhantomBlobs = 105,
        SyncLogCommitterWrite = 106,
        SyncLogCommitterCommit = 107,
        HugeKeeperWriteBlob = 108,
        HugeKeeperAllocChunk = 109,
        HugeKeeperFreeChunk = 110,
        HugeKeeperEntryPoint = 111,
        HullDbCommit = 112,
        HullCompactWorkerWrite = 113,
        HullWriteSst = 114,
        ChunkKeeperCommit = 115,
        MetadataCommit = 116,
        ScrubWrite = 117,
        ScrubCommit = 118,
        LogCutterCutLog = 119,
        SyncerCommit = 120,
        RecoveredHugeBlob = 121,
    };

    EOp Op = EOp::Unknown;

    constexpr TWriteSource() = default;

    constexpr explicit TWriteSource(EOp op)
        : Op(op)
    {}

    static constexpr TWriteSource Unknown() {
        return {};
    }

    static constexpr bool IsTabletOp(EOp op) {
        return EOp::WriteLogEntry <= op && op <= EOp::FlatCollectGarbage;
    }

    static constexpr bool IsVDiskOp(EOp op) {
        return EOp::SkeletonHandoffDelLogoBlob <= op && op <= EOp::RecoveredHugeBlob;
    }

    static constexpr TWriteSource Tablet(EOp op) {
        return IsTabletOp(op) ? TWriteSource(op) : Unknown();
    }

    static constexpr TWriteSource VDisk(EOp op) {
        return IsVDiskOp(op) ? TWriteSource(op) : Unknown();
    }

    static constexpr TWriteSource FromProto(ui32 op) {
        if (op > static_cast<ui32>(EOp::RecoveredHugeBlob)) {
            return Unknown();
        }

        const EOp value = static_cast<EOp>(op);
        return IsTabletOp(value) || IsVDiskOp(value)
            ? TWriteSource(value)
            : Unknown();
    }

    constexpr ui32 ToProtoOp() const {
        if (IsTabletOp(Op) || IsVDiskOp(Op)) {
            return static_cast<ui32>(Op);
        }
        return 0;
    }

    friend constexpr bool operator==(const TWriteSource&, const TWriteSource&) = default;
};

} // namespace NKikimr
