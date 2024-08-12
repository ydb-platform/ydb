#include "blobstorage_pdisk_params.h"
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

    constexpr ui64 UsPerSec = 1000000ull;
    constexpr ui64 MuchLargerK = 5ull;

    ////////////////////////////////////////////////////////////////////////////
    // TPDiskParams
    ////////////////////////////////////////////////////////////////////////////
    TPDiskParams::TPDiskParams(NPDisk::TOwner owner, ui64 ownerRound, ui32 chunkSize, ui32 appendBlockSize,
                               ui64 seekTimeUs, ui64 readSpeedBps, ui64 writeSpeedBps, ui64 readBlockSize,
                               ui64 writeBlockSize, ui64 bulkWriteBlockSize, NPDisk::EDeviceType trueMediaType)
        : Owner(owner)
        , OwnerRound(ownerRound)
        , ChunkSize(chunkSize)
        , AppendBlockSize(appendBlockSize)
        , RecommendedReadSize(CalculateRecommendedReadSize(seekTimeUs, readSpeedBps, appendBlockSize))
        , SeekTimeUs(seekTimeUs)
        , ReadSpeedBps(readSpeedBps)
        , WriteSpeedBps(writeSpeedBps)
        , ReadBlockSize(readBlockSize)
        , WriteBlockSize(writeBlockSize)
        , BulkWriteBlockSize(bulkWriteBlockSize)
        , PrefetchSizeBytes(CalculatePrefetchSizeBytes(seekTimeUs, readSpeedBps))
        , GlueRequestDistanceBytes(CalculateGlueRequestDistanceBytes(seekTimeUs, readSpeedBps))
        , TrueMediaType(trueMediaType)
    {
        Y_DEBUG_ABORT_UNLESS(AppendBlockSize <= ChunkSize);
    }

    // Read size that allows pdisk to spend at least 50% actually reading the data (not seeking)
    ui32 TPDiskParams::CalculateRecommendedReadSize(ui64 seekTimeUs, ui64 readSpeedBps, ui64 appendBlockSize) {
        if (appendBlockSize) {
            ui64 seekEquivalentBytes = (seekTimeUs * readSpeedBps + UsPerSec - 1ull) / UsPerSec;
            ui64 appendBlockSizeAlignedSeekEquivalentBytes = (seekEquivalentBytes + appendBlockSize - 1ull) /
                appendBlockSize * appendBlockSize;
            return appendBlockSizeAlignedSeekEquivalentBytes;
        }
        return 0;
    }

    // Read size that allows pdisk to spend 'MuchLargerK' times more time actually reading than seeking
    ui64 TPDiskParams::CalculatePrefetchSizeBytes(ui64 seekTimeUs, ui64 readSpeedBps) {
        ui64 muchLognerThanSeekUs = MuchLargerK * seekTimeUs;
        ui64 muchLargerThanSeekEquivalentBytes = muchLognerThanSeekUs * readSpeedBps / UsPerSec;
        return muchLargerThanSeekEquivalentBytes;
    }

    // Distance between reads that is faster to read than to seek
    ui64 TPDiskParams::CalculateGlueRequestDistanceBytes(ui64 seekTimeUs, ui64 readSpeedBps) {
        ui64 seekEquivalentBytes = seekTimeUs * readSpeedBps / UsPerSec;
        return seekEquivalentBytes;
    }

    TString TPDiskParams::ToString() const {
        TStringStream str;
        str << "{TPDiskParams ownerId# " << Owner;
        str << " ownerRound# " << OwnerRound;
        str << " ChunkSize# " << ChunkSize;
        str << " AppendBlockSize# " << AppendBlockSize;
        str << " RecommendedReadSize# " << RecommendedReadSize;
        str << " SeekTimeUs# " << SeekTimeUs;
        str << " ReadSpeedBps# " << ReadSpeedBps;
        str << " WriteSpeedBps# " << WriteSpeedBps;
        str << " ReadBlockSize# " << ReadBlockSize;
        str << " WriteBlockSize# " << WriteBlockSize;
        str << " BulkWriteBlockSize# " << BulkWriteBlockSize;
        str << " PrefetchSizeBytes# " << PrefetchSizeBytes;
        str << " GlueRequestDistanceBytes# " << GlueRequestDistanceBytes;
        str << "}";
        return str.Str();
    }

    void TPDiskParams::OutputHtml(IOutputStream &str) const {
        HTML(str) {
            TABLE_CLASS ("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "Disk param";}
                        TABLEH() {str << "Value";}
                    }
                }
                TABLEBODY() {
                    TABLER() {
                        TABLED() {str << "Owner";}
                        TABLED() {str << Owner;}
                    }
                    TABLER() {
                        TABLED() {str << "ChunkSize";}
                        TABLED() {str << ChunkSize;}
                    }
                    TABLER() {
                        TABLED() {str << "AppendBlockSize";}
                        TABLED() {str << AppendBlockSize;}
                    }
                    TABLER() {
                        TABLED() {str << "RecommendedReadSize";}
                        TABLED() {str << RecommendedReadSize;}
                    }
                    TABLER() {
                        TABLED() {str << "SeekTimeUs";}
                        TABLED() {str << SeekTimeUs;}
                    }
                    TABLER() {
                        TABLED() {str << "ReadSpeedBps";}
                        TABLED() {str << ReadSpeedBps;}
                    }
                    TABLER() {
                        TABLED() {str << "WriteSpeedBps";}
                        TABLED() {str << WriteSpeedBps;}
                    }
                    TABLER() {
                        TABLED() {str << "ReadBlockSize";}
                        TABLED() {str << ReadBlockSize;}
                    }
                    TABLER() {
                        TABLED() {str << "WriteBlockSize";}
                        TABLED() {str << WriteBlockSize;}
                    }
                    TABLER() {
                        TABLED() {str << "BulkWriteBlockSize";}
                        TABLED() {str << BulkWriteBlockSize;}
                    }
                    TABLER() {
                        TABLED() {str << "PrefetchSizeBytes";}
                        TABLED() {str << PrefetchSizeBytes;}
                    }
                    TABLER() {
                        TABLED() {str << "GlueRequestDistanceBytes";}
                        TABLED() {str << GlueRequestDistanceBytes;}
                    }
                }
            }
        }
    }

} // NKikimr
