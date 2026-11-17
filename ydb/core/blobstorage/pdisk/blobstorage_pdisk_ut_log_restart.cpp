#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_env.h"

#include <util/generic/algorithm.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TPDiskLogRestart) {

    // The log can end exactly at the end of a log chunk with no valid reference to the next chunk, for example when
    // the PDisk stops after writing the last sector of the chunk and before writing the reference. After a restart
    // the PDisk continues the log in a new chunk, and the old chunk must be released once no owner needs it.
    Y_UNIT_TEST(ChunkAtTheEndOfTheLogIsReleasedAfterRestart) {
        TActorTestContext testCtx({
            .IsBad = false,
            .DiskSize = 1ull << 30,
            .ChunkSize = 1ull * (1 << 20),
            .SmallDisk = true,
        });
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        // Every log write ends its last sector, so the logger is at the first sector of a new chunk only when the
        // previous chunk is full and nothing has been written into the new one yet.
        do {
            vdisk.SendEvLogSync(1024);
        } while (!testCtx.GetPDisk()->CommonLogger->OnFirstSectorInChunk());

        const auto [chunkIdx, referenceOffset] = testCtx.SafeRunOnPDisk([](NPDisk::TPDisk *pdisk) {
            UNIT_ASSERT_VALUES_EQUAL(pdisk->LogChunks.size(), 2);
            const ui32 chunkIdx = pdisk->LogChunks.front().ChunkIdx;
            return std::make_pair(chunkIdx, pdisk->Format.Offset(chunkIdx, pdisk->UsableSectorsPerLogChunk()));
        });

        // Lose the reference to the next chunk, as if the PDisk had stopped before writing it.
        const TString zeroes(testCtx.GetPDisk()->Format.SectorSize * NPDisk::ReplicationFactor, '\0');
        UNIT_ASSERT(testCtx.TestCtx.SectorMap->Write((const ui8*)zeroes.data(), zeroes.size(), referenceOffset));

        testCtx.RestartPDiskSync();
        vdisk.Init();
        vdisk.ReadLog();
        testCtx.SafeRunOnPDisk([&](NPDisk::TPDisk *pdisk) {
            Cerr << "after restart logger chunkIdx# " << pdisk->CommonLogger->ChunkIdx
                << " sectorIdx# " << pdisk->CommonLogger->SectorIdx << " log chunks#";
            for (const NPDisk::TLogChunkInfo& info : pdisk->LogChunks) {
                Cerr << " " << info.ChunkIdx << ":" << (ui32)pdisk->ChunkState[info.ChunkIdx].CommitState;
            }
            Cerr << Endl;
        });

        // Move the owner's log past the old chunk.
        vdisk.SendEvLogSync();
        vdisk.CutLogAllButOne();

        auto isLogChunk = [&] {
            return testCtx.SafeRunOnPDisk([&](NPDisk::TPDisk *pdisk) {
                return AnyOf(pdisk->LogChunks, [&](const NPDisk::TLogChunkInfo& info) {
                    return info.ChunkIdx == chunkIdx;
                });
            });
        };
        for (ui32 i = 0; i < 500 && isLogChunk(); ++i) {
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_C(!isLogChunk(), "chunkIdx# " << chunkIdx << " is still a log chunk although no owner needs it");
    }
}

} // namespace NKikimr
