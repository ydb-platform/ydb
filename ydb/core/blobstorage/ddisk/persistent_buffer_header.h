#pragma once

#include "ddisk.h"
#include "persistent_buffer.h"

namespace NKikimr::NDDisk {

#pragma pack(push, 1)

    static_assert(DataAlignment >= 4096);

    struct TPersistentBufferHeader {
        static constexpr ui8 PersistentBufferHeaderSignature[16] = {249, 173, 163, 160, 196, 193, 69, 133, 83, 38, 34, 104, 170, 146, 237, 156};

        enum EFlags : ui64 {
            NONE = 0,
            IS_BARRIER = 1,
            IS_ERASE = 2,
            IS_ERASE_COMPACT = 4,
        };

        ui8 Signature[16];
        ui64 Checksum;
        ui64 Version;
        ui64 Flags;
        ui64 RecordLsn;
        ui32 RecordIdx;

        ui64 PersistentBufferUniqueId;
        ui32 NodeId;
        ui32 PDiskId;
        ui32 SlotId;

        ui32 Reserved[14];
    };

    static_assert(sizeof(TPersistentBufferHeader) == 128);

    struct TPersistentBufferBarrierRecord {
        ui64 TabletId;
        ui32 Generation;
        ui64 Lsn;
    };

    struct TPersistentBufferBarriers {
        static constexpr ui32 MaxBarriersPerHeader = (DataAlignment - sizeof(TPersistentBufferHeader)) / sizeof(TPersistentBufferBarrierRecord);

        TPersistentBufferHeader Header;
        TPersistentBufferBarrierRecord Barriers[MaxBarriersPerHeader];
    };

    static_assert(sizeof(TPersistentBufferBarriers) <= DataAlignment);

    struct TPersistentBufferFastErases {
        static constexpr ui32 ErasesBufferSize = DataAlignment - sizeof(TPersistentBufferHeader) - sizeof(ui64) - sizeof(ui32);
        // Heuristic based on ErasesBufferSize, means that it's better to fallback on zeroing erases
        // If lsns count exeed this number - barrier was not moved for a long time and fast erases is not efficient in this case.
        // It is better to fall back to zeroing erases a little bit earlier,
        // than continue every erase call Compact method to check erase sector overfill.
        static constexpr ui32 ErasesBufferLsnCount = ErasesBufferSize - 32;

        TPersistentBufferHeader Header;
        ui64 TabletId;
        ui32 Generation;
        ui8 CompactLsns[ErasesBufferSize];
    };

    static_assert(sizeof(TPersistentBufferFastErases) <= DataAlignment);

    struct TPersistentBufferLsnRecordHeader {
        // Max lsn record data size is 128 sectors
        static constexpr ui32 MaxSectorsPerBufferRecord = 128;

        TPersistentBufferHeader Header;
        ui64 TabletId;
        ui32 Generation;
        ui32 Reserved1;
        ui64 VChunkIndex;
        ui32 OffsetInBytes;
        ui32 Size;
        ui64 Lsn;
        TPersistentBufferSectorInfo Locations[MaxSectorsPerBufferRecord];
    };

    static_assert(sizeof(TPersistentBufferLsnRecordHeader) <= DataAlignment);

#pragma pack(pop)
}
