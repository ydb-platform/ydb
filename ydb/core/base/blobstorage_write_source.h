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
        GroupWriteLoadActor = 122,
    };

    EOp Op = EOp::Unknown;

    constexpr TWriteSource() = default;

    constexpr explicit TWriteSource(EOp op)
        : Op(op)
    {}

    static constexpr TWriteSource Unknown() {
        return {};
    }

    static constexpr bool IsKnownOp(EOp op) {
        switch (op) {
            case EOp::Unknown:
            case EOp::WriteLogEntry:
            case EOp::WriteLogReference:
            case EOp::BlockBlobStorage:
            case EOp::GcLogChannel:
            case EOp::DeleteHardBarrier:
            case EOp::FlatCompactionPut:
            case EOp::FlatCollectGarbage:
            case EOp::SkeletonHandoffDelLogoBlob:
            case EOp::SkeletonAddBulkSst:
            case EOp::SkeletonLocalSyncData:
            case EOp::SkeletonAnubisOsirisPut:
            case EOp::SkeletonPhantomBlobs:
            case EOp::SyncLogCommitterWrite:
            case EOp::SyncLogCommitterCommit:
            case EOp::HugeKeeperWriteBlob:
            case EOp::HugeKeeperAllocChunk:
            case EOp::HugeKeeperFreeChunk:
            case EOp::HugeKeeperEntryPoint:
            case EOp::HullDbCommit:
            case EOp::HullCompactWorkerWrite:
            case EOp::HullWriteSst:
            case EOp::ChunkKeeperCommit:
            case EOp::MetadataCommit:
            case EOp::ScrubWrite:
            case EOp::ScrubCommit:
            case EOp::LogCutterCutLog:
            case EOp::SyncerCommit:
            case EOp::RecoveredHugeBlob:
            case EOp::GroupWriteLoadActor:
                return true;
        }
        return false;
    }

    static constexpr TWriteSource FromProto(ui32 op) {
        if (op > static_cast<ui32>(EOp::GroupWriteLoadActor)) {
            return Unknown();
        }

        const EOp value = static_cast<EOp>(op);
        return IsKnownOp(value)
            ? TWriteSource(value)
            : Unknown();
    }

    constexpr ui32 ToProtoOp() const {
        return IsKnownOp(Op)
            ? static_cast<ui32>(Op)
            : 0;
    }

    friend constexpr bool operator==(const TWriteSource&, const TWriteSource&) = default;
};

} // namespace NKikimr
