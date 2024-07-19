#include "blobstorage_pdisk_ut_env.h"

namespace NKikimr {
void RecreateOwner(TActorTestContext& testCtx, TVDiskIDOwnerRound& vdisk) {
    testCtx.TestResponse<NPDisk::TEvSlayResult>(
            new NPDisk::TEvSlay(vdisk.VDiskID, vdisk.OwnerRound + 1, 0, 0),
            NKikimrProto::OK);

    const auto evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
            new NPDisk::TEvYardInit(vdisk.OwnerRound + 1, vdisk.VDiskID, testCtx.TestCtx.PDiskGuid),
            NKikimrProto::OK);

    vdisk.OwnerRound =  evInitRes->PDiskParams->OwnerRound;
}

void TestChunkWriteReleaseRun() {
    TActorTestContext testCtx({ false });

    const TVDiskID vDiskID(0, 1, 0, 0, 0);
    const auto evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
            new NPDisk::TEvYardInit(2, vDiskID, testCtx.TestCtx.PDiskGuid),
            NKikimrProto::OK);
    const auto evReserveRes = testCtx.TestResponse<NPDisk::TEvChunkReserveResult>(
            new NPDisk::TEvChunkReserve(evInitRes->PDiskParams->Owner, evInitRes->PDiskParams->OwnerRound, 1),
            NKikimrProto::OK);
    UNIT_ASSERT(evReserveRes->ChunkIds.size() == 1);

    const ui32 reservedChunk = evReserveRes->ChunkIds.front();
    NPDisk::TCommitRecord commitRecord;
    commitRecord.CommitChunks.push_back(reservedChunk);
    testCtx.TestResponse<NPDisk::TEvLogResult>(
            new NPDisk::TEvLog(evInitRes->PDiskParams->Owner, evInitRes->PDiskParams->OwnerRound, 0, commitRecord,
                    TRcBuf(TString()), TLsnSeg(1, 1), nullptr),
            NKikimrProto::OK);

    const auto evControlRes = testCtx.TestResponse<NPDisk::TEvYardControlResult>(
            new NPDisk::TEvYardControl(NPDisk::TEvYardControl::GetPDiskPointer, nullptr),
            NKikimrProto::OK);
    auto *pDisk = reinterpret_cast<NPDisk::TPDisk*>(evControlRes->Cookie);
    pDisk->PDiskThread.StopSync();

    {
        NPDisk::TCommitRecord commitRecord;
        commitRecord.DeleteChunks.push_back(reservedChunk);
        NPDisk::TEvLog ev(evInitRes->PDiskParams->Owner, evInitRes->PDiskParams->OwnerRound, 0, commitRecord,
                TRcBuf(TString()), TLsnSeg(2, 2), nullptr);
        NPDisk::TLogWrite *log = new NPDisk::TLogWrite(ev, testCtx.Sender, 0, {}, {});
        bool ok = pDisk->PreprocessRequest(log);
        UNIT_ASSERT(ok);
        pDisk->RouteRequest(log);
    }
    pDisk->ProcessLogWriteQueueAndCommits();

    {
        TString chunkWriteData = PrepareData(1024);
        NPDisk::TEvChunkWrite ev(evInitRes->PDiskParams->Owner, evInitRes->PDiskParams->OwnerRound, reservedChunk,
                0, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(chunkWriteData), nullptr, false, 0);
        NPDisk::TChunkWrite *chunkWrite = new NPDisk::TChunkWrite(ev, testCtx.Sender, {}, {});
        bool ok = pDisk->PreprocessRequest(chunkWrite);
        UNIT_ASSERT(!ok);
    }

    pDisk->ProcessChunkWriteQueue();

    testCtx.TestResponse<NPDisk::TEvLogResult>(
            nullptr,
            NKikimrProto::OK);
    testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
            nullptr,
            NKikimrProto::ERROR);

    testCtx.Send(new NActors::TEvents::TEvPoisonPill());
}

std::atomic<ui64> TVDiskMock::Idx = 0;
std::atomic<ui64> TVDiskMock::OwnerRound = 2;

}
