#pragma once

#include "defs.h"

namespace NKikimr::NDDisk {
    struct TPersistentBufferSectorInfo {
        ui64 ChunkIdx : 32;
        ui64 SectorIdx : 16;
        ui64 HasSignatureCorrection : 1;
        ui64 Reserved : 15;
        ui64 Checksum : 64;
    };

    struct TPersistentBufferRecordId {
        ui64 TabletId;
        ui32 Generation;
        ui64 Lsn;

        friend constexpr std::strong_ordering operator <=>(const TPersistentBufferRecordId& x, const TPersistentBufferRecordId& y) = default;
    };

    struct TPersistentBuffer {
        struct TRecord {
            ui32 OffsetInBytes;
            ui32 Size;
            std::vector<TPersistentBufferSectorInfo> Sectors;
            TRope Data;
            ui64 VChunkIndex;
            TInstant Timestamp;
            std::unordered_set<ui64> ReadInflight;
        };

        std::map<ui64, TRecord> Records;
        ui64 Size = 0;
    };
}

 namespace std {
    template <>
    struct hash<NKikimr::NDDisk::TPersistentBufferRecordId> {
        inline size_t operator()(const NKikimr::NDDisk::TPersistentBufferRecordId& r) const {
            return MultiHash(r.TabletId, r.Generation, r.Lsn);
        }
    };
 }
