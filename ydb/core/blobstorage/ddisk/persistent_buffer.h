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
