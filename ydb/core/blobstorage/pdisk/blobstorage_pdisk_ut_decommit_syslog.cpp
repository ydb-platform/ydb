#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_env.h"

#include <util/generic/size_literals.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TPDiskDecommitSysLog) {

    // A chunk that the owner deletes with DeleteToDecommitted is DATA_COMMITTED_DECOMMIT_IN_PROGRESS until the commit
    // record is written. A sys log restore point written in this window must keep the chunk with its owner: if the
    // PDisk stops before the commit record is written, the chunk still belongs to the owner.
    Y_UNIT_TEST(DecommitInProgressChunkKeepsItsOwnerInSysLog) {
        TActorTestContext testCtx({
            .IsBad = false,
            .DiskSize = 1ull << 30,
            .ChunkSize = 1ull * (1 << 20),
            .SmallDisk = true,
        });
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        const TChunkIdx chunkIdx = *vdisk.Chunks[EChunkState::COMMITTED].begin();

        // Move the log past the commit record: a start reads commit records from every log chunk it still keeps
        // (ProcessReadLogRecord), so the log chunk with this one must be released, and only the sys log then knows the
        // chunk owner. Log chunks are 1 MiB here; write and cut 3 MiB of log.
        for (ui32 i = 0; i < 48; ++i) {
            vdisk.SendEvLogSync(64_KB);
            vdisk.CutLogAllButOne();
        }

        // The window between the decommit request and its durable commit record, with a restore point written in it.
        testCtx.SafeRunOnPDisk([&](NPDisk::TPDisk *pdisk) {
            UNIT_ASSERT_EQUAL(pdisk->ChunkState[chunkIdx].CommitState, NPDisk::TChunkState::DATA_COMMITTED);
            pdisk->ChunkState[chunkIdx].CommitState = NPDisk::TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS;
            pdisk->WriteSysLogRestorePoint(nullptr, NPDisk::TReqId(NPDisk::TReqId::MarkDirtySysLog, 0), {});
        });
        // a log write after the restore point, so that the restore point is on the disk before the restart
        vdisk.SendEvLogSync();

        // The PDisk stops before the commit record is written: the chunk must still be the owner's.
        testCtx.RestartPDiskSync();
        vdisk.Init();
    }
}

} // namespace NKikimr
