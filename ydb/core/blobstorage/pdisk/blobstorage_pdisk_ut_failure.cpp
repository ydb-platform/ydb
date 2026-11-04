#include "blobstorage_pdisk_ut.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include "blobstorage_pdisk_ut_env.h"
#include <ydb/core/blobstorage/base/vdisk_priorities.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TPDiskFailureInjection) {


    Y_UNIT_TEST(TestSectorMapDirectWriteError) {
        auto sectorMap = MakeIntrusive<NPDisk::TSectorMap>(static_cast<ui64>(1024 * 1024), NPDisk::NSectorMap::DM_HDD);
        auto* failureProbs = sectorMap->GetFailureProbabilities();
        UNIT_ASSERT(failureProbs != nullptr);

        failureProbs->WriteErrorProbability.store(1.0);

        TString data(4096, 'A');
        bool writeResult = sectorMap->Write((ui8*)data.data(), 4096, 0);
        UNIT_ASSERT_EQUAL(writeResult, false);

        TString readData(4096, '\0');
        bool readResult = sectorMap->Read((ui8*)readData.data(), 4096, 0);
        UNIT_ASSERT_EQUAL(readResult, true);

        TString expectedData(4096, '\x33');
        UNIT_ASSERT_EQUAL(readData, expectedData);
    }

    Y_UNIT_TEST(TestSectorMapDirectReadError) {
        auto sectorMap = MakeIntrusive<NPDisk::TSectorMap>(static_cast<ui64>(1024 * 1024), NPDisk::NSectorMap::DM_HDD);
        auto* failureProbs = sectorMap->GetFailureProbabilities();
        UNIT_ASSERT(failureProbs != nullptr);

        TString data(4096, 'B');
        bool writeResult = sectorMap->Write((ui8*)data.data(), 4096, 0);
        UNIT_ASSERT_EQUAL(writeResult, true);

        failureProbs->ReadErrorProbability.store(1.0);

        TString readData(4096, '\0');
        bool readResult = sectorMap->Read((ui8*)readData.data(), 4096, 0);
        UNIT_ASSERT_EQUAL(readResult, false);
    }

    Y_UNIT_TEST(TestSectorMapDirectSilentWriteFail) {
        auto sectorMap = MakeIntrusive<NPDisk::TSectorMap>(static_cast<ui64>(1024 * 1024), NPDisk::NSectorMap::DM_HDD);
        auto* failureProbs = sectorMap->GetFailureProbabilities();
        UNIT_ASSERT(failureProbs != nullptr);

        failureProbs->SilentWriteFailProbability.store(1.0);

        TString data(4096, 'C');
        bool writeResult = sectorMap->Write((ui8*)data.data(), 4096, 0);
        UNIT_ASSERT_EQUAL(writeResult, true);

        TString readData(4096, '\0');
        bool readResult = sectorMap->Read((ui8*)readData.data(), 4096, 0);
        UNIT_ASSERT_EQUAL(readResult, true);

        TString expectedData(4096, '\x33');
        UNIT_ASSERT_EQUAL(readData, expectedData);
    }

    Y_UNIT_TEST(TestSectorMapDirectReplayRead) {
        auto sectorMap = MakeIntrusive<NPDisk::TSectorMap>(static_cast<ui64>(1024 * 1024), NPDisk::NSectorMap::DM_HDD);
        auto* failureProbs = sectorMap->GetFailureProbabilities();
        UNIT_ASSERT(failureProbs != nullptr);

        TString data1(4096, 'D');
        bool writeResult1 = sectorMap->Write((ui8*)data1.data(), 4096, 0);
        UNIT_ASSERT_EQUAL(writeResult1, true);

        failureProbs->ReadReplayProbability.store(1.0);

        TString data2(4096, 'E');
        bool writeResult2 = sectorMap->Write((ui8*)data2.data(), 4096, 0);
        UNIT_ASSERT_EQUAL(writeResult2, true);

        TString readData(4096, '\0');
        bool readResult = sectorMap->Read((ui8*)readData.data(), 4096, 0);
        UNIT_ASSERT_EQUAL(readResult, true);
        UNIT_ASSERT_EQUAL(readData, data1);

        failureProbs->ReadReplayProbability.store(0.0);
        TString readData2(4096, '\0');
        bool readResult2 = sectorMap->Read((ui8*)readData2.data(), 4096, 0);
        UNIT_ASSERT_EQUAL(readResult2, true);
        UNIT_ASSERT_EQUAL(readData2, data2);
    }

    Y_UNIT_TEST(TestFakeErrorPDiskWriteProbability) {
        TActorTestContext testCtx({.DiskMode = NPDisk::NSectorMap::DM_HDD, .UseSectorMap = true});

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        UNIT_ASSERT(vdisk.Chunks[EChunkState::COMMITTED].size() == 1);
        const ui32 reservedChunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

        {
            auto *runtime = testCtx.GetRuntime();
            auto &appData = runtime->GetAppData();
            TAtomic prevValue = 0;
            const ui32 pdiskId = testCtx.GetPDisk()->PCtx->PDiskId;
            TString controlName = Sprintf("PDisk_%u_SectorMapWriteErrorProbability", pdiskId);
            const TAtomicBase oneProb = 1000000000;
            appData.Dcb->SetValue(controlName, oneProb, prevValue);
        }

        auto* failureProbs = testCtx.TestCtx.SectorMap->GetFailureProbabilities();
        UNIT_ASSERT(failureProbs != nullptr);
        while (failureProbs->WriteErrorProbability.load() < 0.99) {
            Sleep(TDuration::MilliSeconds(10));
        }

        TString data = PrepareData(4096);
        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, 0, new NPDisk::TEvChunkWrite::TAlignedParts(std::move(data)), nullptr, false, NPriRead::HullOnlineOther),
                NKikimrProto::CORRUPTED);
    }

    Y_UNIT_TEST(TestFakeErrorPDiskReadProbability) {
        TActorTestContext testCtx({.DiskMode = NPDisk::NSectorMap::DM_HDD, .UseSectorMap = true});

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        UNIT_ASSERT(vdisk.Chunks[EChunkState::COMMITTED].size() == 1);
        const ui32 reservedChunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

        {
            auto *runtime = testCtx.GetRuntime();
            auto &appData = runtime->GetAppData();
            TAtomic prevValue = 0;
            const ui32 pdiskId = testCtx.GetPDisk()->PCtx->PDiskId;
            TString controlName = Sprintf("PDisk_%u_SectorMapReadErrorProbability", pdiskId);
            const TAtomicBase oneProb = 1000000000;
            appData.Dcb->SetValue(controlName, oneProb, prevValue);
        }

        auto* failureProbs = testCtx.TestCtx.SectorMap->GetFailureProbabilities();
        UNIT_ASSERT(failureProbs != nullptr);
        while (failureProbs->ReadErrorProbability.load() < 0.99) {
            Sleep(TDuration::MilliSeconds(10));
        }

        TString data = PrepareData(4096);
        TString originalData = data;
        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, 0, new NPDisk::TEvChunkWrite::TAlignedParts(std::move(data)), nullptr, false, NPriRead::HullOnlineOther),
                NKikimrProto::OK);

        testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, 0, 4096, NPriRead::HullOnlineOther, nullptr),
                NKikimrProto::CORRUPTED);
    }

    Y_UNIT_TEST(TestSilentWriteFailPDisk) {
        TActorTestContext testCtx({.DiskMode = NPDisk::NSectorMap::DM_HDD, .UseSectorMap = true});

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        UNIT_ASSERT(vdisk.Chunks[EChunkState::COMMITTED].size() == 1);
        const ui32 reservedChunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

        {
            auto *runtime = testCtx.GetRuntime();
            auto &appData = runtime->GetAppData();
            TAtomic prevValue = 0;
            const ui32 pdiskId = testCtx.GetPDisk()->PCtx->PDiskId;
            TString controlName = Sprintf("PDisk_%u_SectorMapSilentWriteFailProbability", pdiskId);
            const TAtomicBase oneProb = 1000000000;
            appData.Dcb->SetValue(controlName, oneProb, prevValue);
        }

        auto* failureProbs = testCtx.TestCtx.SectorMap->GetFailureProbabilities();
        UNIT_ASSERT(failureProbs != nullptr);
        while (failureProbs->SilentWriteFailProbability.load() < 0.99) {
            Sleep(TDuration::MilliSeconds(10));
        }

        TString data = PrepareData(4096);
        TString originalData = data;
        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, 0, new NPDisk::TEvChunkWrite::TAlignedParts(std::move(data)), nullptr, false, NPriRead::HullOnlineOther),
                NKikimrProto::OK);

        auto res = testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, 0, 4096, NPriRead::HullOnlineOther, nullptr),
                NKikimrProto::OK);

        UNIT_ASSERT(!res->Data.IsReadable());
    }

    Y_UNIT_TEST(TestChunkWriteFailsIfAnyPieceFails) {
        // A chunk write is split into pieces, and only the last device write of the last piece carries the completion
        // that answers the VDisk. If an earlier piece fails, the answer must not be OK, otherwise the VDisk commits a
        // blob whose failed piece was never written. The dangerous case is a failed first piece and a written last
        // piece: SectorMap does not change sectors on a failed write, so the test can tell which pieces were written.
        ui32 firstPieceLostLastWritten = 0;
        for (ui32 attempt = 0; attempt < 100 && firstPieceLostLastWritten < 3; ++attempt) {
            TActorTestContext testCtx({
                .DiskMode = NPDisk::NSectorMap::DM_HDD,
                .UseSectorMap = true,
                .EnableSectorEncryption = true,
            });

            // Let both pieces of the write leave in the same PDisk cycle.
            auto cfg = testCtx.GetPDiskConfig();
            cfg->UseNoopScheduler = true;
            testCtx.UpdateConfigRecreatePDisk(cfg);

            TVDiskMock vdisk(&testCtx);
            vdisk.InitFull();

            vdisk.ReserveChunk();
            vdisk.CommitReservedChunks();
            UNIT_ASSERT(vdisk.Chunks[EChunkState::COMMITTED].size() == 1);
            const ui32 reservedChunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

            // A full piece followed by a short last piece.
            const auto& format = testCtx.GetPDisk()->Format;
            const ui32 pieceSize = testCtx.GetPDisk()->Cfg->IoPieceSizeBytes;
            const ui32 size = pieceSize + 64_KB;

            {
                auto *runtime = testCtx.GetRuntime();
                auto &appData = runtime->GetAppData();
                TAtomic prevValue = 0;
                const ui32 pdiskId = testCtx.GetPDisk()->PCtx->PDiskId;
                TString controlName = Sprintf("PDisk_%u_SectorMapWriteErrorProbability", pdiskId);
                const TAtomicBase halfProb = 500000000;
                appData.Dcb->SetValue(controlName, halfProb, prevValue);
            }

            auto* failureProbs = testCtx.TestCtx.SectorMap->GetFailureProbabilities();
            UNIT_ASSERT(failureProbs != nullptr);
            for (ui32 i = 0; failureProbs->WriteErrorProbability.load() < 0.49; ++i) {
                UNIT_ASSERT_C(i < 1000, "the write error probability did not reach the SectorMap");
                Sleep(TDuration::MilliSeconds(10));
            }

            auto& sectorMap = *testCtx.TestCtx.SectorMap;
            const ui64 errorsBefore = sectorMap.EmulatedWriteErrors.load();
            testCtx.Send(new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, 0, new NPDisk::TEvChunkWrite::TAlignedParts(PrepareData(size)), nullptr, false,
                    NPriWrite::HullHugeUserData));
            const auto res = testCtx.Recv<NPDisk::TEvChunkWriteResult>();
            const ui64 failedDeviceWrites = sectorMap.EmulatedWriteErrors.load() - errorsBefore;

            auto isWritten = [&](ui32 sectorIdx) {
                TString sector = TString::Uninitialized(format.SectorSize);
                UNIT_ASSERT(sectorMap.Read((ui8*)sector.Detach(), sector.size(), format.Offset(reservedChunk, sectorIdx)));
                return sector != TString(format.SectorSize, '\x33'); // SectorMap returns 0x33 for unwritten sectors
            };
            if (!isWritten(0) && isWritten(pieceSize / format.SectorSize)) {
                ++firstPieceLostLastWritten;
            }

            if (failedDeviceWrites) {
                UNIT_ASSERT_C(res->Status != NKikimrProto::OK, "attempt# " << attempt
                        << " failedDeviceWrites# " << failedDeviceWrites << " result# " << res->ToString());
            }
        }
        UNIT_ASSERT_C(firstPieceLostLastWritten > 0, "no attempt lost the first piece and wrote the last one");
    }

    Y_UNIT_TEST(TestLogWriteFailsIfAnyBufferFails) {
        // A log record larger than a buffer is written in several buffers, and only the last one carries the completion
        // that answers the VDisk. If an earlier buffer fails, the record must not be reported as written.
        ui32 writesWithFailedBuffers = 0;
        for (ui32 attempt = 0; attempt < 30 && writesWithFailedBuffers < 10; ++attempt) {
            TActorTestContext testCtx({
                .DiskMode = NPDisk::NSectorMap::DM_HDD,
                .UseSectorMap = true,
                .EnableSectorEncryption = true,
            });

            // Let all buffers of the record leave in the same PDisk cycle.
            auto cfg = testCtx.GetPDiskConfig();
            cfg->UseNoopScheduler = true;
            testCtx.UpdateConfigRecreatePDisk(cfg);

            TVDiskMock vdisk(&testCtx);
            vdisk.InitFull();

            // Spans three buffers.
            const ui32 size = 2 * testCtx.GetPDisk()->Cfg->BufferPoolBufferSizeBytes;

            {
                auto *runtime = testCtx.GetRuntime();
                auto &appData = runtime->GetAppData();
                TAtomic prevValue = 0;
                const ui32 pdiskId = testCtx.GetPDisk()->PCtx->PDiskId;
                TString controlName = Sprintf("PDisk_%u_SectorMapWriteErrorProbability", pdiskId);
                const TAtomicBase halfProb = 500000000;
                appData.Dcb->SetValue(controlName, halfProb, prevValue);
            }

            auto* failureProbs = testCtx.TestCtx.SectorMap->GetFailureProbabilities();
            UNIT_ASSERT(failureProbs != nullptr);
            for (ui32 i = 0; failureProbs->WriteErrorProbability.load() < 0.49; ++i) {
                UNIT_ASSERT_C(i < 1000, "the write error probability did not reach the SectorMap");
                Sleep(TDuration::MilliSeconds(10));
            }

            auto& sectorMap = *testCtx.TestCtx.SectorMap;
            const ui64 errorsBefore = sectorMap.EmulatedWriteErrors.load();
            testCtx.Send(new NPDisk::TEvLog(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound, 0,
                    TRcBuf(PrepareData(size)), vdisk.GetLsnSeg(), nullptr));
            const auto res = testCtx.Recv<NPDisk::TEvLogResult>();
            const ui64 failedDeviceWrites = sectorMap.EmulatedWriteErrors.load() - errorsBefore;

            if (failedDeviceWrites) {
                ++writesWithFailedBuffers;
                UNIT_ASSERT_C(res->Status != NKikimrProto::OK, "attempt# " << attempt
                        << " failedDeviceWrites# " << failedDeviceWrites << " result# " << res->ToString());
            }
        }
        UNIT_ASSERT(writesWithFailedBuffers > 0);
    }

    Y_UNIT_TEST(TestReadReplayPDisk) {
        TActorTestContext testCtx({.DiskMode = NPDisk::NSectorMap::DM_HDD, .UseSectorMap = true});

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        UNIT_ASSERT(vdisk.Chunks[EChunkState::COMMITTED].size() == 1);
        const ui32 reservedChunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

        TString data1 = PrepareData(4096);
        TString originalData1 = data1;
        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, 0, new NPDisk::TEvChunkWrite::TAlignedParts(std::move(data1)), nullptr, false, NPriRead::HullOnlineOther),
                NKikimrProto::OK);

        TString data2 = PrepareData(4096);
        TString originalData2 = data2;
        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, 0, new NPDisk::TEvChunkWrite::TAlignedParts(std::move(data2)), nullptr, false, NPriRead::HullOnlineOther),
                NKikimrProto::OK);

        auto* failureProbs = testCtx.TestCtx.SectorMap->GetFailureProbabilities();
        UNIT_ASSERT(failureProbs != nullptr);

        {
            auto *runtime = testCtx.GetRuntime();
            auto &appData = runtime->GetAppData();
            TAtomic prevValue = 0;
            const ui32 pdiskId = testCtx.GetPDisk()->PCtx->PDiskId;
            TString controlName = Sprintf("PDisk_%u_SectorMapReadReplayProbability", pdiskId);
            const TAtomicBase oneProb = 1000000000;
            appData.Dcb->SetValue(controlName, oneProb, prevValue);
        }
        while (failureProbs->ReadReplayProbability.load() < 0.99) {
            Sleep(TDuration::MilliSeconds(10));
        }

        const auto& res = testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, 0, 4096, NPriRead::HullOnlineOther, nullptr),
                NKikimrProto::OK);
        UNIT_ASSERT_EQUAL(res->Data.ToString(), originalData1);

        {
            auto *runtime = testCtx.GetRuntime();
            auto &appData = runtime->GetAppData();
            TAtomic prevValue = 0;
            const ui32 pdiskId = testCtx.GetPDisk()->PCtx->PDiskId;
            TString controlName = Sprintf("PDisk_%u_SectorMapReadReplayProbability", pdiskId);
            const TAtomicBase zeroProb = 0;
            appData.Dcb->SetValue(controlName, zeroProb, prevValue);
        }
        while (failureProbs->ReadReplayProbability.load() > 0.01) {
            Sleep(TDuration::MilliSeconds(10));
        }

        const auto& res2 = testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, 0, 4096, NPriRead::HullOnlineOther, nullptr),
                NKikimrProto::OK);
        UNIT_ASSERT_EQUAL(res2->Data.ToString(), originalData2);
    }
}

} // namespace NKikimr
