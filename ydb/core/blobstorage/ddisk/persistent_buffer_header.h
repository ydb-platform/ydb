#pragma once

#include "ddisk.h"
#include "persistent_buffer.h"

namespace NKikimr::NDDisk {

    struct TPersistentBufferHeaderBase {
        static constexpr ui8 PersistentBufferHeaderSignature[16] = {249, 173, 163, 160, 196, 193, 69, 133, 83, 38, 34, 104, 170, 146, 237, 156};

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
        ui32 Reserved[45];
    };

    struct TPersistentBufferHeaderRecordBase {
        ui64 TabletId;
        ui32 Generation;
        ui64 VChunkIndex;
        ui32 OffsetInBytes;
        ui32 Size;
        ui64 Lsn;
    };

    struct TPersistentBufferHeaderBarrierRecord {
        ui64 TabletId;
        ui64 Lsn;
    };

    struct TPersistentBufferHeaderBarrierBase {
        ui32 BarrierIdx;
    };

    struct TPersistentBufferHeaderEraseBase {
        ui64 TabletId;
        ui32 EraseIdx;
    };

    struct TPersistentBufferHeader : public TPersistentBufferHeaderBase {
        static constexpr ui32 MaxBarriersPerHeader = (DataAlignment - sizeof(TPersistentBufferHeaderBase) - sizeof(TPersistentBufferHeaderBarrierBase)) / sizeof(TPersistentBufferHeaderBarrierRecord);
        static constexpr ui32 ErasesBufferSize = (DataAlignment - sizeof(TPersistentBufferHeaderBase) - sizeof(TPersistentBufferHeaderEraseBase)) / sizeof(char);

        // Heuristic based on ErasesBufferSize, means that it's better to fallback on zeroing erases
        // If lsns count exeed this number - barrier was not moved for a long time and fast erases is not efficient in this case.
        // It is better to fall back to zeroing erases a little bit earlier,
        // than continue every erase call Compact method to check erase sector overfill.
        static constexpr ui32 ErasesBufferLsnCount = ErasesBufferSize - 32;
        static constexpr ui32 MaxSectorsPerBufferRecord = 128;

        struct TRecord : public TPersistentBufferHeaderRecordBase {
            TPersistentBufferSectorInfo Locations[MaxSectorsPerBufferRecord];
        };

        struct TBarrier : public TPersistentBufferHeaderBarrierBase {
            TPersistentBufferHeaderBarrierRecord Barriers[MaxBarriersPerHeader];
        };

        struct TErase : public TPersistentBufferHeaderEraseBase {
            char CompactLsns[ErasesBufferSize];
        };

        union {
            TRecord Record;
            TBarrier Barrier;
            TErase Erase;
        };

    };

    static_assert(sizeof(TPersistentBufferHeader) == 4096);
}
