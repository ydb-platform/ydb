#pragma once

#include "persistent_buffer.h"

namespace NKikimr::NDDisk {
    struct TPersistentBufferHeader {
        static constexpr ui8 PersistentBufferHeaderSignature[16] = {249, 173, 163, 160, 196, 193, 69, 133, 83, 38, 34, 104, 170, 146, 237, 156};
        static constexpr ui32 HeaderChecksumOffset = 24;
        static constexpr ui32 HeaderChecksumSize = 8;
        static constexpr ui32 MaxBarriersPerHeader = 240;
        static constexpr ui32 ErasesBufferSize = 3832;

        // Heuristic based on ErasesBufferSize, means that it's better to fallback on zeroing erases
        // If lsns count exeed this number - barrier was not moved for a long time and fast erases is not efficient in this case.
        // It is better to fall back to zeroing erases a little bit earlier,
        // than continue every erase call Compact method to check erase sector overfill.
        static constexpr ui32 ErasesBufferLsnCount = 3800;
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
            TBarrierRecord Barriers[MaxBarriersPerHeader];
        };

        struct TErase {
            ui64 TabletId;
            ui32 EraseIdx;
            char CompactLsns[ErasesBufferSize];
        };

        enum EFlags : ui64 {
            NONE = 0,
            IS_BARRIER = 1,
            IS_ERASE = 2,
            IS_ERASE_COMPACT = 4,
        };

        ui8 Signature[16];
        ui64 HeaderChecksum;
        ui64 Flags;
        ui64 RecordLsn;

        ui64 PersistentBufferUniqueId;
        ui32 NodeId;
        ui32 PDiskId;
        ui32 SlotId;

        union {
            TRecord Record;
            TBarrier Barrier;
            TErase Erase;
        };

        ui32 Reserved[45];
    };

    static_assert(sizeof(TPersistentBufferHeader) == 4096);
}
