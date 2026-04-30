#pragma once

#include "persistent_buffer.h"

namespace NKikimr::NDDisk {
    struct TPersistentBufferHeader {
        static constexpr ui8 PersistentBufferHeaderSignature[16] = {249, 173, 163, 160, 196, 193, 69, 133, 83, 38, 34, 104, 170, 146, 237, 156};
        static constexpr ui32 HeaderChecksumOffset = 24;
        static constexpr ui32 HeaderChecksumSize = 8;
        static constexpr ui32 MaxBarriersPerHeader = 240;
        static constexpr ui32 MaxSectorsPerBufferRecord = 128;

        struct TRecord {
            ui64 TabletId;
            ui32 Generation;
            ui64 VChunkIndex;
            ui32 OffsetInBytes;
            ui32 Size;
            ui64 Lsn;
            TPersistentBufferSectorInfo Locations[MaxSectorsPerBufferRecord];
        };

        struct TBarrier {
            struct TBarrierRecord {
                ui64 TabletId;
                ui64 Lsn;
            };
            
            ui32 BarrierIdx;
            ui64 BarrierLsn;
            TBarrierRecord Barriers[MaxBarriersPerHeader];
        };

        enum EFlags : ui64 {
            NONE = 0,
            IS_BARRIER = 1,
        };

        ui8 Signature[16];
        ui64 HeaderChecksum;
        ui64 Flags;

        union {
            TRecord Record;
            TBarrier Barrier;
        };

        ui64 Reserved[26];
    };

    static_assert(sizeof(TPersistentBufferHeader) == 4096);
}
