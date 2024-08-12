#pragma once
#include "defs.h"
#include "blobstorage_pdisk_defs.h"
#include <util/stream/output.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TPDiskParams
    // PDisk related constants, obtained from PDisk during initialization
    ////////////////////////////////////////////////////////////////////////////
    struct TPDiskParams : public TThrRefBase {
        const NPDisk::TOwner Owner;
        const ui64 OwnerRound;
        const ui64 ChunkSize;
        const ui32 AppendBlockSize;
        const ui32 RecommendedReadSize;
        const ui64 SeekTimeUs;    // pdisk seek time (measured in us)
        const ui64 ReadSpeedBps;  // pdisk sequential read speed (in bytes per second)
        const ui64 WriteSpeedBps; // pdisk sequential write speed (in bytes per second)
        const ui64 ReadBlockSize; // pdisk approximate read block size (for seq read)
                                  // expect extra seek for each full block size
        const ui64 WriteBlockSize;// pdisk approximate write block size (for seq write)
                                  // expect extra seek for each full block size
        const ui64 BulkWriteBlockSize;// pdisk bulk (SST) write block size (for seq write)

        const ui64 PrefetchSizeBytes; // Pdisk is expected to stream data of this size at 83% of max speed.
        const ui64 GlueRequestDistanceBytes;  // It is faster to read unneeded data of this size than to seek over it.

        const NPDisk::EDeviceType TrueMediaType;

        static ui32 CalculateRecommendedReadSize(ui64 seekTimeUs, ui64 readSpeedBps, ui64 appendBlockSize);
        static ui64 CalculatePrefetchSizeBytes(ui64 seekTimeUs, ui64 readSpeedBps);
        static ui64 CalculateGlueRequestDistanceBytes(ui64 seekTimeUs, ui64 readSpeedBps);

        TPDiskParams(NPDisk::TOwner owner, ui64 ownerRound, ui32 chunkSize, ui32 appendBlockSize,
                     ui64 seekTimeUs, ui64 readSpeedBps, ui64 writeSpeedBps, ui64 readBlockSize,
                     ui64 writeBlockSize, ui64 bulkWriteBlockSize, NPDisk::EDeviceType trueMediaType);
        void OutputHtml(IOutputStream &str) const;
        TString ToString() const;
    };

} // NKikimr
