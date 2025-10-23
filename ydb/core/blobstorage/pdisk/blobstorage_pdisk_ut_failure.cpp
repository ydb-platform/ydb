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
