#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_impl.h"

#include "blobstorage_pdisk_ut.h"
#include "blobstorage_pdisk_ut_actions.h"
#include "blobstorage_pdisk_ut_helpers.h"
#include "blobstorage_pdisk_ut_run.h"

#include <ydb/core/blobstorage/crypto/default.h>

#include <ydb/core/testlib/actors/test_runtime.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TYardTest) {

/*
YARD_UNIT_TEST(TestLotsOfNonceJumps) {
    TTestContext tc(false, true);
    Run<TTestInit<true, 1>>(&tc, 1, MIN_CHUNK_SIZE);
    // for (size_t i = 0; i < 3 * MIN_CHUNK_SIZE / 4096 / 5; ++i) {
    for (size_t i = 0; i < 204; ++i) {
        // Cerr << "i# " << i << Endl;
        // Run<TTestInit<false>>(&tc, 1, MIN_CHUNK_SIZE, false, disk);
        Run<TTestLogWrite<1, 2023>>(&tc, 1, MIN_CHUNK_SIZE);
    }
    Run<TTestInit<false, 302>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 303>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 304>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 305>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 306>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 307>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 308>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 309>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 310>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 311>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 312>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 313>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 314>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 315>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 316>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 317>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 318>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 319>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 320>>(&tc, 1, MIN_CHUNK_SIZE);
}
*/

YARD_UNIT_TEST(TestBadDeviceInit) {
    TTestContext tc(false, true);
    Run<TTestInitCorruptedError>(&tc, 1, MIN_CHUNK_SIZE, true);
}

YARD_UNIT_TEST(TestInit) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestInit<true, 1>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 2>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInit<false, 3>>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestInitOnIncompleteFormat) {
    TTestContext tc(false, true);
    TTestRunConfig cfg(&tc);
    cfg.ChunkSize = MIN_CHUNK_SIZE;
    cfg.TestContext = &tc;
    FillDeviceWithPattern(&tc, MIN_CHUNK_SIZE, NPDisk::MagicIncompleteFormat);
    Run<TTestInit<true, 1>>(cfg);
    Run<TTestInit<false, 2>>(cfg);
    Run<TTestInit<false, 3>>(cfg);
}

YARD_UNIT_TEST(TestInitOwner) {
    TTestContext tc(false, true);
    Run<TTestInitOwner>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestIncorrectRequests) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestIncorrectRequests>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestEmptyLogRead) {
    TTestContext tc(false, true);
    Run<TTestEmptyLogRead>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestWholeLogRead) {
    TTestContext tc(false, true);
    Run<TTestWholeLogRead>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestLogWriteRead) {
    TTestContext tc(false, true);
    Run<TTestLogWriteRead<17>>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestLogWriteReadMedium) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestLogWriteRead<6000>>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestLogWriteReadMediumWithHddSectorMap) {
    TTestContext tc(false, true, NPDisk::NSectorMap::DM_HDD);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestLogWriteRead<6000>>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestLogWriteReadLarge) {
    TTestContext tc(false, true);
    Run<TTestLogWriteRead<9000>>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestLogWriteCutEqual) {
    for (int i = 0; i < 10; ++i) {
        TTestContext tc(false, true);
        FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
        Run<TTestLogWriteCut<true>>(&tc, 2, MIN_CHUNK_SIZE);
        TTestLogWriteCut<true>::Reset();
        Run<TTestWholeLogRead>(&tc, 1, MIN_CHUNK_SIZE);
    }
}

YARD_UNIT_TEST(TestLogWriteCutEqualRandomWait) {
    for (int i = 0; i < 10; ++i) {
        TTestContext tc(false, true);
        tc.SectorMap->ImitateRandomWait = {TDuration::MicroSeconds(500), TDuration::MicroSeconds(1000)};
        FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
        Run<TTestLogWriteCut<true>>(&tc, 2, MIN_CHUNK_SIZE);
        TTestLogWriteCut<true>::Reset();
        Run<TTestWholeLogRead>(&tc, 1, MIN_CHUNK_SIZE);
    }
}

YARD_UNIT_TEST(TestSysLogReordering) {
    for (int i = 0; i < 10; ++i) {
        TTestContext tc(false, true);
        FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
        Run<TTestSysLogReordering>(&tc, 5, MIN_CHUNK_SIZE);
        TTestSysLogReordering::VDiskNum = 0;
        Run<TTestSysLogReorderingLogCheck>(&tc, 5, MIN_CHUNK_SIZE);
        TTestSysLogReorderingLogCheck::VDiskNum = 0;
    }
}

YARD_UNIT_TEST(TestLogWriteCutUnequal) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestLogWriteCut<false>>(&tc, 2, MIN_CHUNK_SIZE);
    TTestLogWriteCut<false>::Reset();
    Run<TTestWholeLogRead>(&tc, 2, MIN_CHUNK_SIZE);

    Run<TTestLogWriteCut<false>>(&tc, 2, MIN_CHUNK_SIZE);
    TTestLogWriteCut<false>::Reset();
}

YARD_UNIT_TEST(TestChunkReadRandomOffset) {
    {
        TTestContext tc(false, true);
        Run<TTestChunkReadRandomOffset<4096, 117, 10>>(&tc, 1, 8 << 20, false);
    }
    {
        TTestContext tc(false, true);
        constexpr ui32 sectorPayload = 4064;
        NPDisk::TDiskFormat format;
        format.Clear();
        UNIT_ASSERT(sectorPayload == format.SectorPayloadSize());
        constexpr ui32 sizeWithHalfOfBlockSize = sectorPayload * 512 - sectorPayload / 2;
        Run<TTestChunkReadRandomOffset<sizeWithHalfOfBlockSize, 217, 20>>(&tc, 1, 8 << 20, false);
    }
    {
        TTestContext tc(false, true);
        Run<TTestChunkReadRandomOffset<1 << 20, 1525, 10>>(&tc, 1, 8 << 20, false);
    }
    {
        TTestContext tc(false, true);
        Run<TTestChunkReadRandomOffset<2079573, 1450, 10>>(&tc, 1, 8 << 20, false);
    }
}

YARD_UNIT_TEST(TestChunkWriteRead) {
    TTestContext tc(false, true);
    Run<TTestChunkWriteRead<30000, 2 << 20>>(&tc, 1, 5 << 20);
}

YARD_UNIT_TEST(TestChunkWriteReadWithHddSectorMap) {
    TTestContext tc(false, true, NPDisk::NSectorMap::DM_HDD);
    Run<TTestChunkWriteRead<30000, 2 << 20>>(&tc, 1, 5 << 20);
}

YARD_UNIT_TEST(TestChunkWriteReadMultiple) {
    {
        TTestContext tc(false, true);
        Run<TTestChunkWriteRead<6000000, 6500000>>(&tc, 1, 16 << 20, false);
    }
    {
        TTestContext tc(false, true);
        Run<TTestChunkWriteRead<3000000, 3500000>>(&tc, 1, 8 << 20, false);
    }
    {
        TTestContext tc(false, true);
        Run<TTestChunkWriteRead<2 << 20, 2 << 20>>(&tc, 1, 8 << 20, false);
    }
    {
        TTestContext tc(false, true);
        Run<TTestChunkWriteRead<1000000, 1500000>>(&tc, 1, 4 << 20, false);
    }
}

YARD_UNIT_TEST(TestChunkWriteReadMultipleWithHddSectorMap) {
    {
        TTestContext tc(false, true, NPDisk::NSectorMap::DM_HDD);
        Run<TTestChunkWriteRead<6000000, 6500000>>(&tc, 1, 16 << 20, false);
    }
    {
        TTestContext tc(false, true, NPDisk::NSectorMap::DM_HDD);
        Run<TTestChunkWriteRead<3000000, 3500000>>(&tc, 1, 8 << 20, false);
    }
    {
        TTestContext tc(false, true, NPDisk::NSectorMap::DM_HDD);
        Run<TTestChunkWriteRead<2 << 20, 2 << 20>>(&tc, 1, 8 << 20, false);
    }
    {
        TTestContext tc(false, true, NPDisk::NSectorMap::DM_HDD);
        Run<TTestChunkWriteRead<1000000, 1500000>>(&tc, 1, 4 << 20, false);
    }
}

YARD_UNIT_TEST(TestChunkWriteReadWhole) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestChunkWriteReadWhole>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkWriteReadWholeWithHddSectorMap) {
    TTestContext tc(false, true, NPDisk::NSectorMap::DM_HDD);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestChunkWriteReadWhole>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkWrite20Read02) {
    TTestContext tc(false, true);
    // 2 << 20 is the read/write burst size, that's why.
    Run<TTestChunkWrite20Read02>(&tc, 1, 2 << 20);
}

YARD_UNIT_TEST(TestStartingPoints) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestLogStartingPoint>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestInitStartingPoints>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestLogMultipleWriteRead) {
    TTestContext tc(false, true);
    Run<TTestLogMultipleWriteRead<4000, 4100, 5000>>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestLogContinuityPersistence) {
    TTestContext tc(false, true);
    Run<TTestLogWrite<9000, 123>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestLogWrite<1000, 124>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestLogWrite<7000, 125>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestLog3Read<9000, 1000, 7000>>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestLogContinuityPersistenceLarge) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    for (int i = 0; i < 4; ++i) {
        Run<TTestLogWrite<20000, 123>>(&tc, 1, MIN_CHUNK_SIZE);
        Run<TTestLogWrite<20000, 124>>(&tc, 1, MIN_CHUNK_SIZE);
        Run<TTestLogWrite<20000, 125>>(&tc, 1, MIN_CHUNK_SIZE);
    }
    Run<TTestLog3Read<20000, 20000, 20000>>(&tc, 1, MIN_CHUNK_SIZE);

}

YARD_UNIT_TEST(TestLogWriteLsnConsistency) {
    TTestContext tc(false, true);
    Run<TTestLogWriteLsnConsistency<150>>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkContinuity2) {
    TTestContext tc(false, true);
    Run<TTestChunk3WriteRead<2>>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkContinuity3000) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestChunk3WriteRead<3000>>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkContinuity9000) {
    TTestContext tc(false, true);
    Run<TTestChunk3WriteRead<9000>>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkLock) {
    TTestContext tc(false, true);
    Run<TTestChunkLock>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkUnlock) {
    TTestContext tc(false, true);
    Run<TTestChunkUnlock>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkUnlockHarakiri) {
    TTestContext tc(false, true);
    Run<TTestChunkUnlockHarakiri>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkUnlockRestart) {
    TTestContext tc(false, true);
    Run<TTestChunkUnlockRestart>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkReserve) {
    TTestContext tc(false, true);
    Run<TTestChunkReserve>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestCheckSpace) {
    TTestContext tc(false, true);
    Run<TTestCheckSpace>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestHttpInfo) {
    TTestContext tc(false, true);
    Run<TTestHttpInfo>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestHttpInfoFileDoesntExist) {
    TTestContext tc(false, true);
    Run<TTestHttpInfoFileDoesntExist>(&tc, 1, MIN_CHUNK_SIZE, true);
}

YARD_UNIT_TEST(TestBootingState) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestBootingState>(&tc, 1, MIN_CHUNK_SIZE, false, TString(), 5);
}

YARD_UNIT_TEST(TestWhiteboard) {
    TTestContext tc(false, true);
    Run<TTestWhiteboard>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(Test3AsyncLog) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestLog3Write<100, 101, 102>>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestLog3Read<100, 101, 102>>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestFirstRecordToKeep) {
    TTestContext tc(false, true);
    Run<TTestFirstRecordToKeepWriteAB>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestFirstRecordToKeepReadB>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkRecommit) {
    TTestContext tc(false, true);
    Run<TTestChunkRecommit>(&tc);
}

YARD_UNIT_TEST(TestChunkRestartRecommit) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestChunkRestartRecommit1>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestChunkRestartRecommit2>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkDelete) {
    TTestContext tc(false, true);
    Run<TTestChunkDelete1>(&tc, 1, MIN_CHUNK_SIZE);
    Run<TTestChunkDelete2>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkForget) {
    TTestContext tc(false, true);
    Run<TTestChunkForget1>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(Test3HugeAsyncLog) {
    TTestContext tc(false, true);
    constexpr ui32 chunkSize = MIN_CHUNK_SIZE;
    Run<TTestLog3Write<chunkSize / 2, chunkSize / 2, chunkSize * 2>>(&tc, 1, chunkSize);
    Run<TTestLog3Read<chunkSize / 2, chunkSize / 2, chunkSize * 2>>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestLotsOfTinyAsyncLogLatency) {
    TTestContext tc(false, true);
    Run<TTestLotsOfTinyAsyncLogLatency>(&tc);
}

YARD_UNIT_TEST(TestHugeChunkAndLotsOfTinyAsyncLogOrder) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, 128 << 20);
    Run<TTestHugeChunkAndLotsOfTinyAsyncLogOrder>(&tc);
}

YARD_UNIT_TEST(TestLogLatency) {
    TTestContext tc(false, true);
    Run<TTestLogLatency>(&tc);
}

YARD_UNIT_TEST(TestMultiYardLogLatency) {
    TTestContext tc(false, true);
    Run<TTestLogLatency>(&tc, 4);
}

YARD_UNIT_TEST(TestMultiYardFirstRecordToKeep) {
    TTestContext tc(false, true);
    Run<TTestFirstRecordToKeepWriteAB>(&tc, 4, MIN_CHUNK_SIZE);
    Run<TTestFirstRecordToKeepReadB>(&tc, 4, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestMultiYardStartingPoints) {
    TTestContext tc(false, true);
    FillDeviceWithZeroes(&tc, MIN_CHUNK_SIZE);
    Run<TTestLogStartingPoint>(&tc, 4, MIN_CHUNK_SIZE);
    Run<TTestInitStartingPoints>(&tc, 4, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestMultiYardLogMultipleWriteRead) {
    TTestContext tc(false, true);
    Run<TTestLogMultipleWriteRead<4000, 4100, 5000>>(&tc, 4);
}

YARD_UNIT_TEST(TestSysLogOverwrite) {
    TTestContext tc(false, true);
    ui32 chunkSize = 128 << 10;

    TString dataPath;
    if (tc.TempDir) {
        TString databaseDirectory = MakeDatabasePath((*tc.TempDir)().c_str());
        dataPath = MakePDiskPath((*tc.TempDir)().c_str());
        MakeDirIfNotExist(databaseDirectory.c_str());
    }
    EntropyPool().Read(&tc.PDiskGuid, sizeof(tc.PDiskGuid));
    FormatPDiskForTest(dataPath, tc.PDiskGuid, chunkSize, 2048ull << 20, false, tc.SectorMap);

    Run<TTestInit<true, 1>>(&tc, 1, chunkSize, false);

    ui32 dataSize = chunkSize*3;
    NPDisk::TAlignedData dataBefore(dataSize);
    ReadPdiskFile(&tc, dataSize, dataBefore);

    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false);
    NPDisk::TAlignedData dataAfter1(dataSize);
    ReadPdiskFile(&tc, dataSize, dataAfter1);

    OutputSectorMap(dataBefore, dataAfter1, dataSize);

    ui64 firstSector = RestoreLastSectors(&tc, dataBefore, dataAfter1, dataSize, 3);
    ReadPdiskFile(&tc, dataSize, dataAfter1);

    OutputSectorMap(dataBefore, dataAfter1, dataSize);

    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false);
    NPDisk::TAlignedData dataAfter2(dataSize);
    ReadPdiskFile(&tc, dataSize, dataAfter2);
    OutputSectorMap(dataAfter1, dataAfter2, dataSize);

    WriteSectors(&tc, dataAfter1, firstSector - 3, 6 * 3);
    ReadPdiskFile(&tc, dataSize, dataAfter2);
    OutputSectorMap(dataBefore, dataAfter2, dataSize);
    OutputSectorMap(dataAfter1, dataAfter2, dataSize);

    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false);
}

/*
YARD_UNIT_TEST(TestDamagedLogContinuityPersistence) {
    TTestContext tc(false, true);
    ui32 chunkSize = 8 << 20;
    Run<TTestInit<true, 1>>(&tc, 1, chunkSize, false, true);

    ui32 dataSize = 3 * chunkSize;
    NPDisk::TAlignedData dataBefore(dataSize);
    ReadPdiskFile(&tc, dataSize, dataBefore);

    Run<TTestLog2Records3Sectors>(&tc, 1, chunkSize, false, true);

    NPDisk::TAlignedData dataAfter(dataSize);
    ReadPdiskFile(&tc, dataSize, dataAfter);

    DestroyLastSectors(&tc, dataBefore, dataAfter, dataSize, 2);

    Run<TTestLogDamageSector3Append1>(&tc, 1, chunkSize, false, true);
    Run<TTestLogRead2Sectors>(&tc, 1, chunkSize, false, true);
}
*/

YARD_UNIT_TEST(TestDamagedFirstRecordToKeep) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    Run<TTestLogFillChunkPlus1>(&tc, 1, chunkSize);

    // Read format info to get raw chunk size
    TString dataPath;
    if (tc.TempDir) {
        TString databaseDirectory = MakeDatabasePath((*tc.TempDir)().c_str());
        dataPath = MakePDiskPath((*tc.TempDir)().c_str());
        MakeDirIfNotExist(databaseDirectory.c_str());
    }
    TPDiskInfo info;
    const NPDisk::TMainKey mainKey{ .Keys = { NPDisk::YdbDefaultPDiskSequence } };
    bool isOk = ReadPDiskFormatInfo(dataPath, mainKey, info, false, tc.SectorMap);
    UNIT_ASSERT_VALUES_EQUAL(isOk, true);

    ui32 dataSize = info.SystemChunkCount * info.RawChunkSizeBytes;
    NPDisk::TAlignedData dataBefore(dataSize);
    ReadPdiskFile(&tc, dataSize, dataBefore);

    Run<TTestLogKeep5Plus1>(&tc, 1, chunkSize);

    NPDisk::TAlignedData dataAfter(dataSize);
    ReadPdiskFile(&tc, dataSize, dataAfter);

    DestroyLastSectors(&tc, dataBefore, dataAfter, dataSize, 3);

    Run<TTestLogReadRecords2To5>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestUpsAndDownsAtTheBoundary) {
    ui32 chunkSize = 8 << 20;
    {
        TTestContext tc(false, true);
        Run<TTestLogWrite<7009 << 10, 1>>(&tc, 1, chunkSize);
        Run<TTestLogWrite<4000, 2>>(&tc, 1, chunkSize);
        Run<TTestLogWrite<4000, 3>>(&tc, 1, chunkSize);
    }
    {
        TTestContext tc(false, true);
        Run<TTestLogWrite<7014 << 10, 4>>(&tc, 1, chunkSize);
        Run<TTestLogWrite<4000, 5>>(&tc, 1, chunkSize);
        Run<TTestLogWrite<4000, 6>>(&tc, 1, chunkSize);
    }
    {
        TTestContext tc(false, true);
        Run<TTestLogWrite<7019 << 10, 7>>(&tc, 1, chunkSize);
        Run<TTestLogWrite<4000, 8>>(&tc, 1, chunkSize);
        Run<TTestLogWrite<4000, 9>>(&tc, 1, chunkSize);
    }
    {
        TTestContext tc(false, true);
        Run<TTestLogWrite<7024 << 10, 10>>(&tc, 1, chunkSize);
        Run<TTestLogWrite<4000, 11>>(&tc, 1, chunkSize);
        Run<TTestLogWrite<4000, 12>>(&tc, 1, chunkSize);
    }
}

YARD_UNIT_TEST(TestDamageAtTheBoundary) {
    ui32 chunkSize = 8 << 20;
    ui32 dataSize = 3 * chunkSize;
    NPDisk::TAlignedData dataBefore(dataSize);
    NPDisk::TAlignedData dataAfter(dataSize);
    {
        TTestContext tc(false, true);
        Run<TTestLogWrite<4000, 1>>(&tc, 1, chunkSize);
        ReadPdiskFile(&tc, dataSize, dataBefore);
        Run<TTestLogWrite<7009 << 10, 2>>(&tc, 1, chunkSize);
        ReadPdiskFile(&tc, dataSize, dataAfter);
        DestroyLastSectors(&tc, dataBefore, dataAfter, dataSize, 3);
        Run<TTestLogWrite<4000, 3>>(&tc, 1, chunkSize);
        Run<TTestLogWrite<4000, 4>>(&tc, 1, chunkSize);
    }
    {
        TTestContext tc(false, true);
        Run<TTestLogWrite<4000, 5>>(&tc, 1, chunkSize);
        ReadPdiskFile(&tc, dataSize, dataBefore);
        Run<TTestLogWrite<7014 << 10, 6>>(&tc, 1, chunkSize);
        ReadPdiskFile(&tc, dataSize, dataAfter);
        DestroyLastSectors(&tc, dataBefore, dataAfter, dataSize, 3);
        Run<TTestLogWrite<4000, 7>>(&tc, 1, chunkSize);
        Run<TTestLogWrite<4000, 8>>(&tc, 1, chunkSize);
    }
    {
        TTestContext tc(false, true);
        Run<TTestLogWrite<4000, 9>>(&tc, 1, chunkSize);
        ReadPdiskFile(&tc, dataSize, dataBefore);
        Run<TTestLogWrite<7019 << 10, 10>>(&tc, 1, chunkSize);
        ReadPdiskFile(&tc, dataSize, dataAfter);
        DestroyLastSectors(&tc, dataBefore, dataAfter, dataSize, 3);
        Run<TTestLogWrite<4000, 11>>(&tc, 1, chunkSize);
        Run<TTestLogWrite<4000, 12>>(&tc, 1, chunkSize);
    }
    {
        TTestContext tc(false, true);
        Run<TTestLogWrite<4000, 13>>(&tc, 1, chunkSize);
        ReadPdiskFile(&tc, dataSize, dataBefore);
        Run<TTestLogWrite<7024 << 10, 14>>(&tc, 1, chunkSize);
        ReadPdiskFile(&tc, dataSize, dataAfter);
        DestroyLastSectors(&tc, dataBefore, dataAfter, dataSize, 3);
        Run<TTestLogWrite<4000, 15>>(&tc, 1, chunkSize);
        Run<TTestLogWrite<4000, 16>>(&tc, 1, chunkSize);
    }
}

YARD_UNIT_TEST(TestUnflushedChunk) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    Run<TTestWriteAndReleaseChunk2A>(&tc, 1, chunkSize);

    ui32 dataSize = 6 * chunkSize;
    NPDisk::TAlignedData dataBefore(dataSize);
    ReadPdiskFile(&tc, dataSize, dataBefore);

    Run<TTestWriteAndCheckChunk2B>(&tc, 1, chunkSize);

    NPDisk::TAlignedData dataAfter(dataSize);
    ReadPdiskFile(&tc, dataSize, dataAfter);

    RestoreLastSectors(&tc, dataBefore, dataAfter, dataSize, 100);

    Run<TTestCheckErrorChunk2B>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestLogOverwriteRestarts) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    Run<TTestWriteAndCutLogChunk>(&tc, 1, chunkSize);
    for (ui32 i = 0; i < 15; ++i) {
        Run<TTestLogMoreSectors<1000>>(&tc, 1, chunkSize);
    }
    for (ui32 i = 0; i < 15; ++i) {
        Run<TTestLogMoreSectors<5000>>(&tc, 1, chunkSize);
    }
    for (ui32 i = 0; i < 15; ++i) {
        Run<TTestLogMoreSectors<9000>>(&tc, 1, chunkSize);
    }
    for (ui32 i = 0; i < 15; ++i) {
        Run<TTestLogMoreSectors<13000>>(&tc, 1, chunkSize);
    }
    for (ui32 i = 0; i < 15; ++i) {
        Run<TTestLogMoreSectors<17000>>(&tc, 1, chunkSize);
    }
}

YARD_UNIT_TEST(TestChunkFlushReboot) {
    TTestContext tc(false, true);
    Run<TTestChunkFlush>(&tc);
    Run<TTestChunkUnavailable>(&tc);
}

YARD_UNIT_TEST(TestRedZoneSurvivability) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    Ctest << "TestRedZoneSurvivability chunkSize# " << chunkSize << Endl;
    Run<TTestRedZoneSurvivability>(&tc, 1, chunkSize);
}

/*
YARD_UNIT_TEST(TestNonceJumpRewriteMin) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    ui32 dataSize = 6 * chunkSize;
    NPDisk::TAlignedData data0(dataSize);
    NPDisk::TAlignedData data1(dataSize);
    NPDisk::TAlignedData data2(dataSize);

    Run<TTestInit<true, 1>>(&tc, 1, chunkSize, false, true);
    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);
    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);

    ReadPdiskFile(&tc, dataSize, data0);

    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);

    ReadPdiskFile(&tc, dataSize, data1);
    i64 lastDifferenceA = FindLastDifferingBytes(data0, data1, dataSize);

    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);

    ReadPdiskFile(&tc, dataSize, data2);
    i64 lastDifferenceB = FindLastDifferingBytes(data1, data2, dataSize);

    i64 distance = lastDifferenceB - lastDifferenceA;

    if (Abs(distance) >= 8) {
        Sleep(TDuration::Seconds(5));
    }

    ASSERT_YTHROW(Abs(distance) <= 8,
            "Log length changed while it wasnt expected to. lastDifferenceA# " << lastDifferenceA
            << " lastDifferenceB# " << lastDifferenceB);
}*/

/*
YARD_UNIT_TEST(TestNonceJumpRewrite) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    ui32 dataSize = 6 * chunkSize;
    NPDisk::TAlignedData data0(dataSize);
    NPDisk::TAlignedData data1(dataSize);

    Run<TTestInit<true, 1>>(&tc, 1, chunkSize, false, true);
    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);
    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);
    Run<TTestLogWrite<2000, 123>>(&tc, 1, chunkSize, false, true);
    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);
    Run<TTestLogWrite<2000, 124>>(&tc, 1, chunkSize, false, true);
    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);
    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);
    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);

    ReadPdiskFile(&tc, dataSize, data0);

    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);

    ReadPdiskFile(&tc, dataSize, data1);
    i64 lastDifferenceA = FindLastDifferingBytes(data0, data1, dataSize);

    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);

    ReadPdiskFile(&tc, dataSize, data0);
    i64 lastDifferenceB = FindLastDifferingBytes(data0, data1, dataSize);

    i64 distance = lastDifferenceB - lastDifferenceA;

    ASSERT_YTHROW(Abs(distance) <= 8,
            "Log length changed while it wasnt expected to. lastDifferenceA# " << lastDifferenceA
            << " lastDifferenceB# " << lastDifferenceB);
    Run<TTestLogWrite<2000, 125>>(&tc, 1, chunkSize, false, true);
    Run<TTestLog3Read<2000, 2000, 2000>>(&tc, 1, chunkSize, false, true);
}
*/

YARD_UNIT_TEST(TestSlay) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    FillDeviceWithZeroes(&tc, chunkSize);
    Run<TTestSlay>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestSlayRace) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    Run<TTestSlayRace>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestSlayRecreate) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    Run<TTestSlayRecreate>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestSlayLogWriteRaceActor) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    Run<TActorTestSlayLogWriteRace>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestMultiYardHarakiri) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    Ctest << Endl << "Fill with zeroes" << Endl;
    FillDeviceWithZeroes(&tc, chunkSize * 2);
    Ctest << Endl << "Phase 1" << Endl;
    Run<TTestFillDiskPhase1>(&tc, 4, chunkSize);
    Ctest << Endl << "Phase 2" << Endl;
    Run<TTestFillDiskPhase2>(&tc, 4, chunkSize);
    Ctest << Endl << "TestHarakiri" << Endl;
    Run<TTestHarakiri>(&tc, 1, chunkSize);
    Ctest << Endl << "TestLogWrite" << Endl;
    Run<TTestLogWrite<1000, 1>>(&tc, 1, chunkSize);
    Ctest << Endl << "TestSimpleHarakiri" << Endl;
    Run<TTestSimpleHarakiri>(&tc, 4, chunkSize);
    Ctest << Endl << "TestLogWrite 2" << Endl;
    Run<TTestLogWrite<1000, 2>>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestDestroySystem) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    Run<TTestInit<true, 1>>(&tc, 1, chunkSize);

    ui32 dataSize = chunkSize;
    NPDisk::TAlignedData dataAfter(dataSize);
    ReadPdiskFile(&tc, dataSize, dataAfter);

    DestroySectors(&tc, dataAfter, dataSize, 0, 1);
    Run<TTestInitCorruptedError>(&tc, 1, chunkSize);

    DestroySectors(&tc, dataAfter, dataSize, 24, 1);
    Run<TTestInitCorruptedError>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestAllocateAllChunks) {
    TTestContext tc(false, true);
    Run<TTestAllocateAllChunks>(&tc, 1, MIN_CHUNK_SIZE);
}

YARD_UNIT_TEST(TestChunkDeletionWhileWriting) {
    TTestContext tc(false, true);
    ui32 chunkSize = 16 << 20;
    Run<TTestChunkDeletionWhileWritingIt>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestCutMultipleLogChunks) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    FillDeviceWithZeroes(&tc, chunkSize);
    Run<TTestCutMultipleLogChunks1>(&tc, 1, chunkSize);
    Run<TTestCutMultipleLogChunks2>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestLogOwerwrite) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    Run<TTestLogOwerwrite1>(&tc, 1, chunkSize);
    Run<TTestLogOwerwrite2>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestDestructionWhileWritingChunk) {
    TTestContext tc(false, true);
    ui32 chunkSize = 8 << 20;
    FillDeviceWithZeroes(&tc, chunkSize);
    Run<TTestDestructionWhileWritingChunk>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestDestructionWhileReadingChunk) {
    TTestContext tc(false, true);
    ui32 chunkSize = 8 << 20;
    Run<TTestDestructionWhileReadingChunk>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestDestructionWhileReadingLog) {
    TTestContext tc(false, true);
    ui32 chunkSize = 8 << 20;
    Run<TTestDestructionWhileReadingLog>(&tc, 1, chunkSize);
}

YARD_UNIT_TEST(TestChunkPriorityBlock) {
    TTestContext tc(false, true);
    Run<TTestChunkPriorityBlock>(&tc);
}

YARD_UNIT_TEST(TestFormatInfo) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    TString dataPath;
    if (tc.TempDir) {
        TString databaseDirectory = MakeDatabasePath((*tc.TempDir)().c_str());
        dataPath = MakePDiskPath((*tc.TempDir)().c_str());
        if (!NFs::Exists(databaseDirectory.c_str())) {
            MakeDirIfNotExist(databaseDirectory.c_str());
        }
    }
    EntropyPool().Read(&tc.PDiskGuid, sizeof(tc.PDiskGuid));
    FormatPDiskForTest(dataPath, tc.PDiskGuid, chunkSize, 1 << 30, false, tc.SectorMap);

    TPDiskInfo info;
    const NPDisk::TMainKey mainKey{.Keys = { NPDisk::YdbDefaultPDiskSequence } };
    bool isOk = ReadPDiskFormatInfo(dataPath, mainKey, info, false, tc.SectorMap);
    UNIT_ASSERT_VALUES_EQUAL(isOk, true);
    UNIT_ASSERT_VALUES_EQUAL(info.TextMessage, "Info");
}

YARD_UNIT_TEST(TestStartingPointReboots) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    TString dataPath;
    if (tc.TempDir) {
        TString databaseDirectory = MakeDatabasePath((*tc.TempDir)().c_str());
        dataPath = MakePDiskPath((*tc.TempDir)().c_str());
        MakeDirIfNotExist(databaseDirectory.c_str());
    }
    EntropyPool().Read(&tc.PDiskGuid, sizeof(tc.PDiskGuid));
    FormatPDiskForTest(dataPath, tc.PDiskGuid, chunkSize, 1 << 30, false, tc.SectorMap);
    for (ui32 i = 0; i < 32; ++i) {
        Run<TTestStartingPointRebootsIteration>(&tc, 1, chunkSize);
    }
}

YARD_UNIT_TEST(TestRestartAtNonceJump) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    // Write a full chunk of logs (assume it's chunk# SystemChunkCount)
    Run<TTestContinueWriteLogChunk>(&tc, 1, chunkSize, false);
    // Read format info to get raw chunk size
    TString dataPath;
    if (tc.TempDir) {
        TString databaseDirectory = MakeDatabasePath((*tc.TempDir)().c_str());
        dataPath = MakePDiskPath((*tc.TempDir)().c_str());
        MakeDirIfNotExist(databaseDirectory.c_str());
    }
    TPDiskInfo info;
    const NPDisk::TMainKey mainKey{ .Keys = { NPDisk::YdbDefaultPDiskSequence } };
    bool isOk = ReadPDiskFormatInfo(dataPath, mainKey, info, false, tc.SectorMap);
    UNIT_ASSERT_VALUES_EQUAL(isOk, true);
    // Destroy data in chunks starting at# SystemChunkCount + 1
    ui32 dataSize = 8 * chunkSize;
    NPDisk::TAlignedData dataAfter(dataSize);
    ReadPdiskFile(&tc, dataSize, dataAfter);

    ui64 firstSector = (info.SystemChunkCount + 1) * info.RawChunkSizeBytes / info.SectorSizeBytes
        - 3; // to get into the situation where we have filled the chunks but did not write the next chunk reference
    DestroySectors(&tc, dataAfter, dataSize, firstSector, 1);
    // Write another full chunk of logs (assume it's chunk# SystemChunkCount + 1)
    Run<TTestContinueWriteLogChunk>(&tc, 1, chunkSize, false);
    // Check that last log Lsn is somewhere out of the first log chunk
    Run<TTestLastLsn>(&tc, 1, chunkSize, false);
}

YARD_UNIT_TEST(TestRestartAtChunkEnd) {
    TTestContext tc(false, true);
    ui32 chunkSize = MIN_CHUNK_SIZE;
    // Write a full chunk of logs (assume it's chunk# SystemChunkCount)
    Run<TTestContinueWriteLogChunk>(&tc, 1, chunkSize, false);
    // Read format info to get raw chunk size
    TString dataPath;
    if (tc.TempDir) {
        TString databaseDirectory = MakeDatabasePath((*tc.TempDir)().c_str());
        dataPath = MakePDiskPath((*tc.TempDir)().c_str());
        MakeDirIfNotExist(databaseDirectory.c_str());
    }
    TPDiskInfo info;
    const NPDisk::TMainKey mainKey{ .Keys = { NPDisk::YdbDefaultPDiskSequence } };
    bool isOk = ReadPDiskFormatInfo(dataPath, mainKey, info, false, tc.SectorMap);
    UNIT_ASSERT_VALUES_EQUAL(isOk, true);
    // Destroy data in chunks starting at# SystemChunkCount + 1
    ui32 dataSize = 8 * chunkSize;
    NPDisk::TAlignedData dataAfter(dataSize);
    ReadPdiskFile(&tc, dataSize, dataAfter);

    ui64 firstSector = (info.SystemChunkCount + 1) * info.RawChunkSizeBytes / info.SectorSizeBytes;
    DestroySectors(&tc, dataAfter, dataSize, firstSector, 1);
    // Write another full chunk of logs (assume it's chunk# SystemChunkCount + 1)
    Run<TTestContinueWriteLogChunk>(&tc, 1, chunkSize, false);
    // Check that last log Lsn is somewhere out of the first log chunk
    Run<TTestLastLsn>(&tc, 1, chunkSize, false);
}

YARD_UNIT_TEST(TestEnormousDisk) {
    TTestContext tc(false, true);
    ui32 chunkSize = 512 << 20;
    ui64 diskSize = 100ull << 40;

    TString dataPath;
    EntropyPool().Read(&tc.PDiskGuid, sizeof(tc.PDiskGuid));
    FormatPDiskForTest(dataPath, tc.PDiskGuid, chunkSize, diskSize, false, tc.SectorMap);

    Run<TTestInit<true, 1>>(&tc, 1, chunkSize, false);
    Run<TTestCommitChunks<(31998)>>(&tc, 1, chunkSize, false);
    Run<TTestLogWrite<512000000, 16>>(&tc, 1, chunkSize);
    Run<TTestLogWrite<512000000, 17>>(&tc, 1, chunkSize);
    Run<TTestLogWrite<128000000, 18>>(&tc, 1, chunkSize);
    Run<TTestChunkWriteRead<30000, 2 << 20>>(&tc, 1, 5 << 20);
}

/*
// TODO(cthulhu): Shorten test data, move it to a proper place
YARD_UNIT_TEST(TestInitOnOldDisk) {
    TTestContext tc(false, true);
    ui32 chunkSize = 134217728;
    ui32 dataSize = 8 * chunkSize;
    NPDisk::TAlignedData data0(dataSize);

    Run<TTestInit<true, 1>>(&tc, 1, chunkSize, false, true);

    ReadPdiskFile(&tc, dataSize, data0);
    Cerr << Endl << Endl << Endl;
    tc.PDiskGuid = 8308644718352142590ull;

    TString path = "/place/home/cthulhu/tmp_hdd2";
    ASSERT_YTHROW(NFs::Exists(path), "File " << path << " does not exist.");
    {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        TPDiskMon mon(counters);
        THolder<NPDisk::IBlockDevice> device(NPDisk::CreateSyncBlockDevice(path, 999, mon));
        VERBOSE_COUT("  Performing Pread of " << dataSize);
        device->PreadAsync(data0.Get(), dataSize, 0, nullptr, 9999, {});
    }

    WriteSectors(&tc, data0, 0, dataSize/4096);
    Run<TTestInit<false, 1>>(&tc, 1, chunkSize, false, true);
}

*/
}
} // namespace NKikimr
