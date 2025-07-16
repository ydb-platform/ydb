#include "blobstorage_pdisk_ut.h"

#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_ut_env.h"

#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/driver_lib/version/ut/ut_helpers.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <util/system/hp_timer.h>

namespace NKikimr {

NPDisk::TEvChunkWrite::TPartsPtr GenParts(TReallyFastRng32& rng, size_t size) {
    static int testCase = 0;
    switch(testCase++) {
        case 0: {
            auto data = PrepareData(size);

            auto counter = MakeIntrusive<::NMonitoring::TCounterForPtr>();
            TMemoryConsumer consumer(counter);
            TTrackableBuffer buffer(std::move(consumer), data.data(), data.size());
            return MakeIntrusive<NPDisk::TEvChunkWrite::TBufBackedUpParts>(std::move(buffer));
        }
        case 1: {
            size_t partsCount = rng.Uniform(1, 10);
            TRope rope;
            size_t createdBytes = 0;
            if (size >= partsCount) {
                for (size_t i = 0; i < partsCount - 1; ++i) {
                    TRope x(PrepareData(rng.Uniform(1, size / partsCount)));
                    createdBytes += x.size();
                    rope.Insert(rope.End(), std::move(x));
                }
            }
            if (createdBytes < size) {
                rope.Insert(rope.End(), TRope(PrepareData(size - createdBytes)));
            }
            return MakeIntrusive<NPDisk::TEvChunkWrite::TRopeAlignedParts>(std::move(rope), size);
        }
        case 2: {
            testCase = 0;
            return MakeIntrusive<NPDisk::TEvChunkWrite::TAlignedParts>(PrepareData(size));
        }
    }
    UNIT_ASSERT(false);
    return nullptr;
}

Y_UNIT_TEST_SUITE(TPDiskTest) {
    Y_UNIT_TEST(TestAbstractPDiskInterface) {
        TString path = "/tmp/asdqwe";
        TIntrusivePtr<TPDiskConfig> cfg = new TPDiskConfig(path, 12345, 0xffffffffull,
                    TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw());
        const TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(new ::NMonitoring::TDynamicCounters);

        auto pCtx = std::make_shared<NPDisk::TPDiskCtx>();
        THolder<NPDisk::IPDisk> pDisk = MakeHolder<NPDisk::TPDisk>(pCtx, cfg, counters);
        pDisk->Wakeup();
    }

    Y_UNIT_TEST(TestThatEveryValueOfEStateEnumKeepsItIntegerValue) {
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Warning!
        // Kikimr Admins use Integer values of EState in their scripts!
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::Initial == 0);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::InitialFormatRead == 1);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::InitialFormatReadError == 2);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::InitialSysLogRead == 3);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::InitialSysLogReadError == 4);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::InitialSysLogParseError == 5);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::InitialCommonLogRead == 6);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::InitialCommonLogReadError == 7);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::InitialCommonLogParseError == 8);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::CommonLoggerInitError == 9);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::Normal == 10);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::OpenFileError == 11);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::ChunkQuotaError == 12);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::DeviceIoError == 13);
        UNIT_ASSERT(NKikimrBlobStorage::TPDiskState::Stopped == 14);
    }

    Y_UNIT_TEST(TestPDiskActorErrorState) {
        TActorTestContext testCtx({ true });

        const TVDiskID vDiskID(0, 1, 0, 0, 0);
        testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(1, vDiskID, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::CORRUPTED);
        testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
                new NPDisk::TEvCheckSpace(1, 1),
                NKikimrProto::CORRUPTED);
        testCtx.TestResponse<NPDisk::TEvLogResult>(
                new NPDisk::TEvLog(1, 1, 0, TRcBuf(TString()), TLsnSeg(1, 1), nullptr),
                NKikimrProto::CORRUPTED);
        testCtx.TestResponse<NPDisk::TEvReadLogResult>(
                new NPDisk::TEvReadLog(1, 1, NPDisk::TLogPosition{0, 0}),
                NKikimrProto::CORRUPTED);

        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                new NPDisk::TEvChunkWrite(1, 1, 1, 0, nullptr, nullptr, false, 1),
                NKikimrProto::CORRUPTED);
        testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                new NPDisk::TEvChunkRead(1, 1, 17, 0, 4096, 1, nullptr),
                NKikimrProto::CORRUPTED);
        testCtx.TestResponse<NPDisk::TEvHarakiriResult>(
                new NPDisk::TEvHarakiri(1, 1),
                NKikimrProto::CORRUPTED);
        testCtx.TestResponse<NPDisk::TEvSlayResult>(
                new NPDisk::TEvSlay(vDiskID, 1, 1, 1),
                NKikimrProto::CORRUPTED);
        testCtx.TestResponse<NPDisk::TEvChunkReserveResult>(
                new NPDisk::TEvChunkReserve(1, 1, 3),
                NKikimrProto::CORRUPTED);
        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::ActionPause, nullptr),
                NKikimrProto::CORRUPTED);

        testCtx.Send(new NActors::TEvents::TEvPoisonPill());
    }

    Y_UNIT_TEST(TestPDiskActorPDiskStopStart) {
        TActorTestContext testCtx{{}};

        const TVDiskID vDiskID(0, 1, 0, 0, 0);
        testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(2, vDiskID, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::OK);
        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStop, nullptr),
                NKikimrProto::OK);
        testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(3, vDiskID, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::CORRUPTED);
        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStart, reinterpret_cast<void*>(&testCtx.MainKey)),
                NKikimrProto::OK);
        testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(3, vDiskID, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::OK);

        testCtx.Send(new NActors::TEvents::TEvPoisonPill());
    }

    Y_UNIT_TEST(TestPDiskActorPDiskStopBroken) {
        TActorTestContext testCtx{{}};

        testCtx.GetRuntime()->WaitFor("Block device start", [&] {
            return testCtx.SafeRunOnPDisk([&] (auto* pdisk) {
                // Check that the PDisk is up
                return pdisk->BlockDevice->IsGood();
            });
        });

        testCtx.Send(new NPDisk::TEvDeviceError("test"));

        // This doesn't stop the PDisk, it will be stopped by TEvDeviceError some time in the future
        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStop, nullptr),
                NKikimrProto::CORRUPTED);

        testCtx.GetRuntime()->WaitFor("Block device stop", [&] {
            return testCtx.SafeRunOnPDisk([&] (auto* pdisk) {
                // Check that the PDisk is stopped
                return !pdisk->BlockDevice->IsGood();
            });
        });

        testCtx.Send(new NActors::TEvents::TEvPoisonPill());
    }

    Y_UNIT_TEST(TestPDiskActorPDiskStopUninitialized) {
        TActorTestContext testCtx{{}};

        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStop, nullptr),
                NKikimrProto::OK);

        testCtx.Send(new NActors::TEvents::TEvPoisonPill());
    }

    Y_UNIT_TEST(TestChunkWriteRelease) {
        for (ui32 i = 0; i < 16; ++i) {
            TestChunkWriteReleaseRun();
        }
    }

    Y_UNIT_TEST(TestPDiskOwnerRecreation) {
        TActorTestContext testCtx{{}};

        const TVDiskID vDiskID(0, 1, 0, 0, 0);
        for (ui32 i = 2; i < 2000; ++i) {
            const auto evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                    new NPDisk::TEvYardInit(i, vDiskID, testCtx.TestCtx.PDiskGuid),
                    NKikimrProto::OK);

            testCtx.TestResponse<NPDisk::TEvSlayResult>(
                    new NPDisk::TEvSlay(vDiskID, evInitRes->PDiskParams->OwnerRound + 1, 0, 0),
                    NKikimrProto::OK);
        }
    }

    Y_UNIT_TEST(TestPDiskOwnerRecreationWithStableOwner) {
        TActorTestContext testCtx{{}};

        // Create "stable" owner, who will be alive during all test
        ui32 i = 2;
        const TVDiskID vDiskID_stable(0, 1, 0, 0, 0);
        testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(i++, vDiskID_stable, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::OK);

        const TVDiskID vDiskID(1, 1, 0, 0, 0);
        for (; i < 2000; ++i) {
            const auto evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                    new NPDisk::TEvYardInit(i, vDiskID, testCtx.TestCtx.PDiskGuid),
                    NKikimrProto::OK);

            testCtx.TestResponse<NPDisk::TEvSlayResult>(
                    new NPDisk::TEvSlay(vDiskID, evInitRes->PDiskParams->OwnerRound + 1, 0, 0),
                    NKikimrProto::OK);
        }
    }

    Y_UNIT_TEST(TestPDiskManyOwnersInitiation) {
        TActorTestContext testCtx{{}};

        TVector<TVDiskIDOwnerRound> goodIds;
        ui64 badIdsCount = 0;

        for (int i = 2; i < 2000; ++i) {
            const TVDiskID vDiskID(i, 1, 0, 0, 0);
            if (badIdsCount == 0) {
                testCtx.Send(new NPDisk::TEvYardInit(i, vDiskID, testCtx.TestCtx.PDiskGuid));
                const auto evInitRes = testCtx.Recv<NPDisk::TEvYardInitResult>();
                if (evInitRes->Status == NKikimrProto::OK) {
                    goodIds.push_back({vDiskID, evInitRes->PDiskParams->OwnerRound});
                } else {
                    ++badIdsCount;
                }
            } else {
                const auto evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                        new NPDisk::TEvYardInit(i, vDiskID, testCtx.TestCtx.PDiskGuid),
                        NKikimrProto::ERROR);
                ++badIdsCount;
            }
        }

        RecreateOwner(testCtx, goodIds.front());

        UNIT_ASSERT(badIdsCount > 0 && goodIds.size() > 0);
        for (auto v : goodIds) {
            testCtx.TestResponse<NPDisk::TEvSlayResult>(
                    new NPDisk::TEvSlay(v.VDiskID, v.OwnerRound + 1, 0, 0),
                    NKikimrProto::OK);
        }
    }


    // the test is supposed to be fast enough to be used with a lot of different configuration parameters
    void BasicTest(TActorTestContext& testCtx, TVDiskMock& mock) {
        for (ui32 restarts = 0; restarts < 2; restarts++) {
            mock.InitFull();
            const int logsSent = 100;
            for (int i = 0; i < logsSent; ++i) {
                mock.SendEvLogSync();
                mock.ReserveChunk();
                mock.CommitReservedChunks();
            }

            mock.Init(); // asserts owned chunks
            UNIT_ASSERT(mock.ReadLog() == mock.OwnedLogRecords());

            mock.DeleteCommitedChunks();
            mock.CutLogAllButOne();
            mock.Init(); // asserts log is cut and no chunks owned
            testCtx.RestartPDiskSync();
        }
    }

    Y_UNIT_TEST(TestVDiskMock) {
        TActorTestContext testCtx{{}};
        TVDiskMock mock(&testCtx);
        BasicTest(testCtx, mock);
    }

    Y_UNIT_TEST(TestRealFile) {
        TActorTestContext testCtx({ .UseSectorMap = false });
        TVDiskMock mock(&testCtx);

        BasicTest(testCtx, mock);
    }

    Y_UNIT_TEST(TestLogWriteReadWithRestarts) {
        TActorTestContext testCtx{{}};

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        for (ui32 i = 0; i < 1000; ++i) {
            vdisk.SendEvLogSync(12346);
            if (i % 17 == 16) {
                vdisk.InitFull();
            }
            if (i % 117 == 116) {
                testCtx.RestartPDiskSync();
                vdisk.InitFull();
            }
        }
    }

    // Test to reproduce bug from KIKIMR-10192
    Y_UNIT_TEST(TestLogSpliceNonceJump) {
        TActorTestContext testCtx({
            .IsBad = false,
            .DiskSize = 1ull << 30,
            .ChunkSize = 1ull * (1 << 20),
            .SmallDisk = true
        });
        TVDiskMock sporadicVDisk(&testCtx);
        TVDiskMock intensiveVDisk(&testCtx);

        sporadicVDisk.InitFull(); // writes log into logChunk# 1
        intensiveVDisk.InitFull();

        sporadicVDisk.SendEvLogSync();

        for (ui32 i = 0; i < 5; i++) {
            do {
                intensiveVDisk.SendEvLogSync(1024);
            } while (!testCtx.GetPDisk()->CommonLogger->OnFirstSectorInChunk());
        }
        // expect log chunks list looks like 1 -> 2 -> ... -> 6 (empty)

        testCtx.RestartPDiskSync(); // writes NonceJump
        UNIT_ASSERT_C(testCtx.GetPDisk()->CommonLogger->SectorIdx == 1, "To reproduce bug nonce jump record"
                " should be written in chunk's first sector");
        sporadicVDisk.InitFull(); // sends EvLog into chunk with recently written nonce jump
        intensiveVDisk.InitFull();

        {
            // initiate log splicing, expect transition to be
            // 1 -> 2 -> ... -> 6
            NPDisk::TPDisk *pdisk = testCtx.GetPDisk();
            UNIT_ASSERT(pdisk->LogChunks.size() == 6);
            intensiveVDisk.CutLogAllButOne();
            // 1 -> 6
            pdisk->PDiskThread.StopSync();
            while (pdisk->LogChunks.size() != 2) {
                pdisk->Update();
            }
        }

        testCtx.RestartPDiskSync();
        intensiveVDisk.Init();
        UNIT_ASSERT_VALUES_EQUAL(intensiveVDisk.ReadLog(), intensiveVDisk.OwnedLogRecords());
        sporadicVDisk.Init();
        UNIT_ASSERT_VALUES_EQUAL(sporadicVDisk.ReadLog(), sporadicVDisk.OwnedLogRecords());

        testCtx.Send(new NActors::TEvents::TEvPoisonPill());
    }

    // Test to reproduce bug with multiple chunk splices from
    Y_UNIT_TEST(TestMultipleLogSpliceNonceJump) {
        TActorTestContext testCtx({
            .IsBad = false,
            .DiskSize = 1ull << 30,
            .ChunkSize = 1ull * (1 << 20),
            .SmallDisk = true
        });
        TVDiskMock sporadicVDisk(&testCtx);
        TVDiskMock moderateVDisk(&testCtx);
        TVDiskMock intensiveVDisk(&testCtx);

        sporadicVDisk.InitFull(); // writes log into logChunk# 1
        moderateVDisk.InitFull(); // writes log into logChunk# 1
        intensiveVDisk.InitFull();

        sporadicVDisk.SendEvLogSync();

        for (ui32 i = 0; i < 8; i++) {
            bool alreadyWriteThisChunk = false;
            do {
                if (1 <= i && i <= 4 && !alreadyWriteThisChunk) {
                    alreadyWriteThisChunk = true;
                    moderateVDisk.SendEvLogSync(1024);
                } else {
                    intensiveVDisk.SendEvLogSync(1024);
                }
            } while (!testCtx.GetPDisk()->CommonLogger->OnFirstSectorInChunk());
        }
        // expect log chunks list looks like 1 -> 2 -> ... -> 9 (empty)

        testCtx.RestartPDiskSync(); // writes NonceJump
        UNIT_ASSERT_C(testCtx.GetPDisk()->CommonLogger->SectorIdx == 1, "To reproduce bug nonce jump record"
                " should be written in chunk's first sector");
        sporadicVDisk.InitFull(); // sends EvLog into chunk with recently written nonce jump
        moderateVDisk.InitFull();
        intensiveVDisk.InitFull();

        {
            // initiate log splicing, expect transition to be
            // 1 -> 2 -> ... -> 9
            NPDisk::TPDisk *pdisk = testCtx.GetPDisk();
            UNIT_ASSERT_C(pdisk->LogChunks.size() == 9, pdisk->LogChunks.size());
            intensiveVDisk.CutLogAllButOne();
            // 1 -> 2 -> 3 -> 4 -> 5 -> 9
            pdisk->PDiskThread.StopSync();
            while (pdisk->LogChunks.size() != 6 || pdisk->IsLogChunksReleaseInflight) {
                pdisk->Update();
            }
        }

        testCtx.RestartPDiskSync();
        moderateVDisk.Init();
        {
            // initiate log splicing, expect transition to be
            // 1 -> 2 -> 3 -> 4 -> 5 -> 9
            NPDisk::TPDisk *pdisk = testCtx.GetPDisk();
            UNIT_ASSERT(pdisk->LogChunks.size() == 6);
            moderateVDisk.CutLogAllButOne();
            // 1 -> 9
            pdisk->PDiskThread.StopSync();
            while (pdisk->LogChunks.size() != 2 || pdisk->IsLogChunksReleaseInflight) {
                pdisk->Update();
            }
        }

        testCtx.RestartPDiskSync();
        intensiveVDisk.Init();
        UNIT_ASSERT_VALUES_EQUAL(intensiveVDisk.ReadLog(), intensiveVDisk.OwnedLogRecords());
        moderateVDisk.Init();
        UNIT_ASSERT_VALUES_EQUAL(moderateVDisk.ReadLog(), moderateVDisk.OwnedLogRecords());
        sporadicVDisk.Init();
        UNIT_ASSERT_VALUES_EQUAL(sporadicVDisk.ReadLog(), sporadicVDisk.OwnedLogRecords());

        testCtx.Send(new NActors::TEvents::TEvPoisonPill());
    }

    Y_UNIT_TEST(TestFakeErrorPDiskManyLogWrite) {
        TActorTestContext testCtx{{}};
        testCtx.TestCtx.SectorMap->IoErrorEveryNthRequests = 1000;

        const TVDiskID vDiskID(0, 1, 0, 0, 0);
        const auto evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(2, vDiskID, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::OK);

        ui32 errors = 0;
        ui32 lsn = 2;
        for (ui32 i = 0; i < 10'000; ++i) {
            testCtx.Send(new NPDisk::TEvLog(evInitRes->PDiskParams->Owner, evInitRes->PDiskParams->OwnerRound, 0,
                        TRcBuf(TString("abc")), TLsnSeg(lsn, lsn), nullptr));
            ++lsn;
            const auto logRes = testCtx.Recv<NPDisk::TEvLogResult>();
            if (logRes->Status != NKikimrProto::OK) {
                ++errors;
            } else {
                UNIT_ASSERT(errors == 0);
            }
        }
        UNIT_ASSERT(errors > 0);
    }

    Y_UNIT_TEST(TestFakeErrorPDiskLogRead) {
        TActorTestContext testCtx{{}};

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        for (int i = 0; i < 100; i++) {
            vdisk.SendEvLogSync(1024);
        }

        testCtx.RestartPDiskSync();

        vdisk.Init();

        // Make sure there will be read error.
        testCtx.TestCtx.SectorMap->ReadIoErrorEveryNthRequests = 1;

        auto res = vdisk.ReadLog(true);

        // Zero log records should be read.
        UNIT_ASSERT_EQUAL(0, res);

        auto device = testCtx.GetPDisk()->BlockDevice.Get();

        // After unsuccessful log read, pdisk should be shut down.
        UNIT_ASSERT(!device->IsGood());
    }

    Y_UNIT_TEST(TestFakeErrorPDiskSysLogRead) {
        TActorTestContext testCtx{{}};

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        // Make sure there will be syslog read error.
        testCtx.TestCtx.SectorMap->ReadIoErrorEveryNthRequests = 1;

        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStop, nullptr),
                NKikimrProto::OK);

        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStart, (void*)(&testCtx.MainKey)),
                NKikimrProto::CORRUPTED);
    }

    Y_UNIT_TEST(TestFakeErrorPDiskManyChunkRead) {
        TActorTestContext testCtx{{}};
        testCtx.TestCtx.SectorMap->ReadIoErrorEveryNthRequests = 100;

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        UNIT_ASSERT(vdisk.Chunks[EChunkState::COMMITTED].size() == 1);
        const ui32 reservedChunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, 0, new NPDisk::TEvChunkWrite::TAlignedParts(PrepareData(1024)), nullptr, false, 0),
                NKikimrProto::OK);

        bool printed = false;
        ui32 errors = 0;
        for (ui32 i = 0; i < 10'000; ++i) {
            testCtx.Send(new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, 0, 1024, 0, nullptr));

            const auto res = testCtx.Recv<NPDisk::TEvChunkReadResult>();
            //Ctest << res->ToString() << Endl;
            if (res->Status != NKikimrProto::OK) {
                if (!printed) {
                    printed = true;
                    Ctest << res->ToString() << Endl;
                }
                ++errors;
            }
        }
        UNIT_ASSERT(errors > 0);
        // Check that PDisk is in working state now
        vdisk.InitFull();
    }

    Y_UNIT_TEST(TestFakeErrorPDiskManyChunkWrite) {
        TActorTestContext testCtx{{}};
        testCtx.TestCtx.SectorMap->IoErrorEveryNthRequests = 1000;

        const TVDiskID vDiskID(0, 1, 0, 0, 0);
        const auto evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(2, vDiskID, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::OK);

        const auto evReserveRes = testCtx.TestResponse<NPDisk::TEvChunkReserveResult>(
                new NPDisk::TEvChunkReserve(evInitRes->PDiskParams->Owner, evInitRes->PDiskParams->OwnerRound, 1),
                NKikimrProto::OK);
        UNIT_ASSERT(evReserveRes->ChunkIds.size() == 1);
        const ui32 reservedChunk = evReserveRes->ChunkIds.front();

        ui32 errors = 0;
        bool printed = false;
        for (ui32 i = 0; i < 10'000; ++i) {
            testCtx.Send(new NPDisk::TEvChunkWrite(evInitRes->PDiskParams->Owner, evInitRes->PDiskParams->OwnerRound,
                    reservedChunk, 0, new NPDisk::TEvChunkWrite::TAlignedParts(PrepareData(1024)), nullptr, false, 0));

            const auto res = testCtx.Recv<NPDisk::TEvChunkWriteResult>();
            //Ctest << res->ToString() << Endl;
            if (res->Status != NKikimrProto::OK) {
                ++errors;
                if (!printed) {
                    printed = true;
                    Ctest << res->ToString() << Endl;
                }
            } else {
                UNIT_ASSERT(errors == 0);
            }
        }
        UNIT_ASSERT(errors > 0);
    }

    Y_UNIT_TEST(TestSIGSEGVInTUndelivered) {
        TActorTestContext testCtx{{}};
        const TVDiskID vDiskID(0, 1, 0, 0, 0);
        const auto evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(2, vDiskID, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::OK);

        TEvents::TEvUndelivered::TPtr ev = reinterpret_cast<TEventHandle<TEvents::TEvUndelivered>*>(
            new IEventHandle(
                testCtx.Sender, testCtx.Sender,
                new TEvents::TEvUndelivered(0, 0)
            )
        );

        const auto& sender = ev->Sender;
        THolder<NPDisk::TUndelivered> req{testCtx.GetPDisk()->ReqCreator.CreateFromEv<NPDisk::TUndelivered>(ev, sender)};
    }

    Y_UNIT_TEST(PDiskRestart) {
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();

        testCtx.GracefulPDiskRestart();

        vdisk.InitFull();
        vdisk.SendEvLogSync();
    }

    Y_UNIT_TEST(PDiskOwnerSlayRace) {
        TActorTestContext testCtx{{}};
        testCtx.GetPDisk(); // inits pdisk
        TVDiskMock vdisk(&testCtx);

        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
            new NPDisk::TEvYardControl(NPDisk::TEvYardControl::ActionPause, nullptr),
            NKikimrProto::OK);

        auto* yardInit = new NPDisk::TEvYardInit(TVDiskMock::OwnerRound.fetch_add(1), vdisk.VDiskID, testCtx.TestCtx.PDiskGuid, testCtx.Sender);
        testCtx.Send(yardInit);
        auto* slay = new NPDisk::TEvSlay(vdisk.VDiskID, TVDiskMock::OwnerRound.load(), 0, 0);
        testCtx.Send(slay);

        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
            new NPDisk::TEvYardControl(NPDisk::TEvYardControl::ActionResume, nullptr),
            NKikimrProto::OK);

        auto slayResult = testCtx.Recv<NPDisk::TEvSlayResult>();

        UNIT_ASSERT_VALUES_EQUAL(NKikimrProto::NOTREADY, slayResult->Status);

        testCtx.TestResponse<NPDisk::TEvSlayResult>(
            new NPDisk::TEvSlay(vdisk.VDiskID, TVDiskMock::OwnerRound.load(), 0, 0),
            NKikimrProto::OK);
    }

    Y_UNIT_TEST(PDiskRestartManyLogWrites) {
        TActorTestContext testCtx{{}};

        const TVDiskID vDiskID(0, 1, 0, 0, 0);
        const auto evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(2, vDiskID, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::OK);

        ui32 lsn = 2;
        TRcBuf logData = TRcBuf(PrepareData(4096));

        for (ui32 i = 0; i < 1000; ++i) {
            testCtx.Send(new NPDisk::TEvLog(evInitRes->PDiskParams->Owner, evInitRes->PDiskParams->OwnerRound, 0,
                        logData, TLsnSeg(lsn, lsn), nullptr));
            if (i == 100) {
                testCtx.GracefulPDiskRestart(false);
            }
            if (i == 600) {
                const auto evInitRes = testCtx.Recv<TEvBlobStorage::TEvNotifyWardenPDiskRestarted>();
                UNIT_ASSERT_VALUES_EQUAL(NKikimrProto::EReplyStatus::OK, evInitRes->Status);
            }
            ++lsn;
        }

        for (ui32 i = 0; i < 100;) {
            const auto logRes = testCtx.Recv<NPDisk::TEvLogResult>();
            i += logRes->Results.size();
            if (logRes->Status == NKikimrProto::OK) {
                Ctest << "TEvLogResult status is ok" << Endl;
            } else {
                Ctest << "TEvLogResult status is error" << Endl;
            }
        }
    }

    Y_UNIT_TEST(CommitDeleteChunks) {
        TActorTestContext testCtx{{}};
        TVDiskMock intensiveVDisk(&testCtx);
        intensiveVDisk.InitFull();
        intensiveVDisk.ReserveChunk();
        intensiveVDisk.ReserveChunk();
        intensiveVDisk.CommitReservedChunks();
        intensiveVDisk.SendEvLogSync();
        intensiveVDisk.DeleteCommitedChunks();
        intensiveVDisk.InitFull();
    }

    // Test to reproduce bug from
    Y_UNIT_TEST(TestLogSpliceChunkReserve) {
        TActorTestContext testCtx({
            .IsBad = false,
            .DiskSize = 1ull << 30,
            .ChunkSize = 1ull * (1 << 20),
            .SmallDisk = true
        });

        TVDiskMock intensiveVDisk(&testCtx);    // idx# 1
        TVDiskMock formerVDisk(&testCtx);       // idx# 2
        TVDiskMock latterVDisk(&testCtx);       // idx# 3
        TVDiskMock fillerVDisk(&testCtx);

        // [1, 2, 3] -> [2] -> [1, 2, 3(+15)] -> [3(-15)] -> [1, 2, 3] -> ... -> [1, 2, 3]
        //
        // latter (owner 3) cuts log
        // [1, 2]  ->   [2] -> [1, 2]         *->            [1, 2]    -> ... -> [1, 2, 3]
        //
        // former (owner 2) cuts log
        // [1]     *->         [1]            *->            [1]       -> ... -> [1, 2, 3]

        intensiveVDisk.InitFull();
        formerVDisk.InitFull();
        latterVDisk.InitFull();
        fillerVDisk.InitFull();


        auto logChunks = [&] () {
            return testCtx.SafeRunOnPDisk([](NPDisk::TPDisk *pdisk) {
                return pdisk->LogChunks.size();
            });
        };

        const ui32 targetLogChunkCount = 9;
        for (ui32 i = 0; i < targetLogChunkCount - 1; i++) {
            if (i != 1 && i != 3) {
                intensiveVDisk.SendEvLogSync(16);
            }

            if (i != 3) {
                formerVDisk.SendEvLogSync(16);
            }

            if (i != 1) {
                latterVDisk.SendEvLogSync(16);
            }
            if (i == 2) {
                latterVDisk.ReserveChunk();
                latterVDisk.CommitReservedChunks();
            }
            if (i == 3) {
                latterVDisk.DeleteCommitedChunks();
            }

            do {
                fillerVDisk.SendEvLogSync(16);
            } while (logChunks() != i + 2);
        }

        // To remove obstructing owner in PDisk's Log prints
        fillerVDisk.CutLogAllButOne();

        {
            auto printLog = [&] () {
                testCtx.SafeRunOnPDisk([](NPDisk::TPDisk *pdisk) {
                    TStringStream out;
                    for (auto& info : pdisk->LogChunks) {
                        out << "[";
                        out << info.ChunkIdx << ": ";
                        for (size_t i = 0; i < info.OwnerLsnRange.size(); ++i) {
                            const NPDisk::TLogChunkInfo::TLsnRange &range = info.OwnerLsnRange[i];
                            if (range.IsPresent) {
                                out << i << ",";
                            }
                        }
                        out << "]";
                        out << " -> ";
                    }
                    out << Endl;
                    Ctest << out.Str();
                });
            };

            printLog();

            UNIT_ASSERT_C(logChunks() == targetLogChunkCount, "LogChunks.size()# " << logChunks());

            do {
                latterVDisk.CutLogAllButOne();
                printLog();
                Sleep(TDuration::Seconds(1));
            } while (logChunks() != targetLogChunkCount - 1);
            printLog();

            do {
                formerVDisk.CutLogAllButOne();
                printLog();
                Sleep(TDuration::Seconds(1));
            } while (logChunks() != targetLogChunkCount - 2);
            printLog();

            testCtx.RestartPDiskSync();
        }

        intensiveVDisk.InitFull();
        formerVDisk.InitFull();
        latterVDisk.InitFull();
    }

    Y_UNIT_TEST(SpaceColor) {
        return; // Enable test after KIKIMR-12880

        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);

        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        for (auto color : {
                    TColor::GREEN,
                    TColor::CYAN,
                    TColor::LIGHT_YELLOW,
                    TColor::YELLOW,
                    TColor::LIGHT_ORANGE,
                    TColor::ORANGE,
                    TColor::RED,
                    //TColor::BLACK,
                } ){
            auto pdiskConfig = testCtx.GetPDiskConfig();
            pdiskConfig->SpaceColorBorder = color;
            pdiskConfig->ExpectedSlotCount = 10;
            testCtx.UpdateConfigRecreatePDisk(pdiskConfig);

            vdisk.InitFull();
            auto initialSpace = testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
                    new NPDisk::TEvCheckSpace(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound),
                    NKikimrProto::OK);
            for (ui32 i = 0; i < initialSpace->FreeChunks + 1; ++i) {
                vdisk.ReserveChunk();
            }
            vdisk.CommitReservedChunks();
            auto resultSpace = testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
                    new NPDisk::TEvCheckSpace(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound),
                    NKikimrProto::OK);
            UNIT_ASSERT(color == StatusFlagToSpaceColor(resultSpace->StatusFlags));
            vdisk.DeleteCommitedChunks();
        }
    }

    Y_UNIT_TEST(DeviceHaltTooLong) {
        TActorTestContext testCtx{{}};
        testCtx.TestCtx.SectorMap->ImitateRandomWait = {TDuration::Seconds(1), TDuration::Seconds(2)};

        TVDiskMock mock(&testCtx);

        mock.InitFull();
        const int logsSent = 100;
        for (int i = 0; i < logsSent; ++i) {
            mock.SendEvLogSync();
        }

        mock.Init();
        UNIT_ASSERT(mock.ReadLog() == mock.OwnedLogRecords());
    }

    Y_UNIT_TEST(TestPDiskOnDifferentKeys) {
        TActorTestContext testCtx{{}};

        int round = 2;
        const TVDiskID vDiskID(0, 1, 0, 0, 0);
        auto evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(round, vDiskID, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::OK);
        testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
                new NPDisk::TEvCheckSpace(evInitRes->PDiskParams->Owner, evInitRes->PDiskParams->OwnerRound),
                NKikimrProto::OK);
        round = evInitRes->PDiskParams->OwnerRound + 1;

        testCtx.MainKey.Keys[0] += 123;
        testCtx.UpdateConfigRecreatePDisk(testCtx.GetPDiskConfig());

        evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(round, vDiskID, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::CORRUPTED);
        testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
                new NPDisk::TEvCheckSpace(evInitRes->PDiskParams->Owner, evInitRes->PDiskParams->OwnerRound),
                NKikimrProto::CORRUPTED);
    }

    Y_UNIT_TEST(ChangePDiskKey) {
        const TString data = PrepareData(4096);

        TActorTestContext testCtx{{}};

        TVDiskMock mock(&testCtx);
        mock.InitFull();

        mock.ReserveChunk();
        const ui32 chunk = *mock.Chunks[EChunkState::RESERVED].begin();

        auto readChunk = [&]() {
            auto evReadRes = testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                    new NPDisk::TEvChunkRead(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                            chunk, 0, data.size(), 0, nullptr),
                    NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(evReadRes->Data.ToString(), data);
        };

        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(new NPDisk::TEvChunkWrite(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
            chunk, 0, new NPDisk::TEvChunkWrite::TAlignedParts(TString(data)), nullptr, false, 0),
            NKikimrProto::OK);
        mock.CommitReservedChunks();

        readChunk();

        testCtx.MainKey.Keys.push_back(0xFull);
        testCtx.RestartPDiskSync();
        mock.InitFull();
        readChunk();

        testCtx.MainKey.Keys = { 0xFull };
        testCtx.RestartPDiskSync();
        mock.InitFull();
        readChunk();

        testCtx.MainKey.Keys = { 0xFull, 0xA };
        testCtx.RestartPDiskSync();
        mock.InitFull();
        readChunk();

        testCtx.MainKey.Keys = { 0xFull, 0xA, 0xB, 0xC };
        testCtx.RestartPDiskSync();
        mock.InitFull();
        readChunk();

        testCtx.MainKey.Keys = { 0xC };
        testCtx.RestartPDiskSync();
        mock.InitFull();
        readChunk();
    }


    Y_UNIT_TEST(WrongPDiskKey) {
        const TString data = PrepareData(4096);

        TActorTestContext testCtx{{}};

        TVDiskMock mock(&testCtx);
        mock.InitFull();

        mock.ReserveChunk();
        const ui32 chunk = *mock.Chunks[EChunkState::RESERVED].begin();

        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(new NPDisk::TEvChunkWrite(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
            chunk, 0, new NPDisk::TEvChunkWrite::TAlignedParts(TString(data)), nullptr, false, 0),
            NKikimrProto::OK);
        mock.CommitReservedChunks();
        testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
                new NPDisk::TEvCheckSpace(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound),
                NKikimrProto::OK);
        testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                new NPDisk::TEvChunkRead(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                        chunk, 0, data.size(), 0, nullptr),
                NKikimrProto::OK);

        testCtx.MainKey.Keys = { 0xABCDEF };
        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStop, nullptr),
                NKikimrProto::OK);

        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStart, (void*)(&testCtx.MainKey)),
                NKikimrProto::CORRUPTED);
    }

    Y_UNIT_TEST(RecreateWithInvalidPDiskKey) {
        TActorTestContext testCtx{{}};
        int round = 2;
        const TVDiskID vDiskID(0, 1, 0, 0, 0);

        auto evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(round, vDiskID, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::OK);
        round = evInitRes->PDiskParams->OwnerRound + 1;

        testCtx.MainKey.Keys = {};
        testCtx.UpdateConfigRecreatePDisk(testCtx.GetPDiskConfig());

        evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
                new NPDisk::TEvYardInit(round, vDiskID, testCtx.TestCtx.PDiskGuid),
                NKikimrProto::CORRUPTED);
    }

    Y_UNIT_TEST(SmallDisk10Gb) {
        ui64 diskSize = 10_GB;
        TActorTestContext testCtx({
            .IsBad = false,
            .DiskSize = diskSize,
            .SmallDisk = true,
        });

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        ui64 reservedSpace = 0;
        // Assert that it is possible for single owner to allocated and commit 85% of small disk size
        while (reservedSpace < diskSize * 0.85) {
            vdisk.ReserveChunk();
            vdisk.CommitReservedChunks();
            reservedSpace += NPDisk::SmallDiskMaximumChunkSize;
        }
    }

    Y_UNIT_TEST(SuprisinglySmallDisk) {
        try {
            TActorTestContext testCtx({
                .IsBad = false,
                .DiskSize = 16_MB,
                .SmallDisk = true,
            });

            UNIT_ASSERT(false);
        } catch (const yexception &e) {
            UNIT_ASSERT_STRING_CONTAINS(e.what(), "Total chunks# 0, System chunks needed# 1, cant run with < 3 free chunks!");
        }

        TActorTestContext testCtx({
            .IsBad = false,
            .DiskSize = 16_MB,
            .SmallDisk = true,
            .InitiallyZeroed = true
        });

        TVDiskID vdiskID;

        const auto evInitRes = testCtx.TestResponse<NPDisk::TEvYardInitResult>(
            new NPDisk::TEvYardInit(1, vdiskID, testCtx.TestCtx.PDiskGuid),
            NKikimrProto::CORRUPTED);

        UNIT_ASSERT_STRING_CONTAINS(evInitRes->ErrorReason, "Total chunks# 0, System chunks needed# 1, cant run with < 3 free chunks!");
    }

    Y_UNIT_TEST(FailedToFormatDiskInfoUpdate) {
        TActorTestContext testCtx({
            .IsBad = false,
            .DiskSize = 16_MB,
            .SmallDisk = true,
            .InitiallyZeroed = true
        });

        TVDiskID vdiskID;

        testCtx.TestResponse<NPDisk::TEvYardInitResult>(
            new NPDisk::TEvYardInit(1, vdiskID, testCtx.TestCtx.PDiskGuid),
            NKikimrProto::CORRUPTED);

        bool received = false;

        struct TTestActor : public NActors::TActorBootstrapped<TTestActor> {

            bool& Received;

            TTestActor(bool& received) : Received(received) {}

            void Bootstrap() {
                Become(&TThis::StateDefault);
            }
            
            STATEFN(StateDefault) {
                switch (ev->GetTypeRewrite()) {
                    case TEvBlobStorage::TEvControllerUpdateDiskStatus::EventType: {
                        Received = true;
                        break;
                    }
                }
            }
        };

        TActorId nodeWardenFake = testCtx.GetRuntime()->Register(new TTestActor(received));

        testCtx.Send(new TEvents::TEvWakeup());

        testCtx.GetRuntime()->RegisterService(MakeBlobStorageNodeWardenID(1), nodeWardenFake);

        testCtx.GetRuntime()->WaitFor("TEvControllerUpdateDiskStatus", [&received]() {
            return received;
        });
    }

    Y_UNIT_TEST(PDiskIncreaseLogChunksLimitAfterRestart) {
        TActorTestContext testCtx({
            .IsBad=false,
            .DiskSize = 1_GB,
            .ChunkSize = 1_MB,
        });

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();

        TRcBuf buf(TString(64_MB, 'a'));
        auto writeLog = [&]() {
            testCtx.Send(new NPDisk::TEvLog(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound, 0,
                        buf, vdisk.GetLsnSeg(), nullptr));
            const auto logRes = testCtx.Recv<NPDisk::TEvLogResult>();
            return logRes->Status;
        };

        while (writeLog() == NKikimrProto::OK) {}
        UNIT_ASSERT_VALUES_EQUAL(writeLog(), NKikimrProto::OUT_OF_SPACE);

        testCtx.GracefulPDiskRestart();

        vdisk.InitFull();
        vdisk.SendEvLogSync();

        UNIT_ASSERT_VALUES_EQUAL(writeLog(), NKikimrProto::OK);
    }

    void AwaitAndCheckEvPDiskStateUpdate(
        TActorTestContext& testCtx,
        ui32 expectedSlotSizeInUnits,
        ui32 expectedNumActiveSlots
    ) {
        bool assertion_disarmed = false;
        Cerr << (TStringBuilder() << "... Awaiting EvPDiskStateUpdate"
            << " SlotSizeInUnits# " << expectedSlotSizeInUnits
            << " NumActiveSlots# " << expectedNumActiveSlots
            << Endl);
        for (int num_inspect=10; num_inspect>0; --num_inspect) {
            const auto evPDiskStateUpdate = testCtx.Recv<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateUpdate>();
            NKikimrWhiteboard::TPDiskStateInfo pdiskInfo = evPDiskStateUpdate.Get()->Record;
            Cerr << (TStringBuilder() << "- Got EvPDiskStateUpdate# " << evPDiskStateUpdate->ToString() << Endl);
            if (!pdiskInfo.HasSlotSizeInUnits()) {
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(pdiskInfo.GetSlotSizeInUnits(), expectedSlotSizeInUnits);
            UNIT_ASSERT_VALUES_EQUAL(pdiskInfo.GetNumActiveSlots(), expectedNumActiveSlots);
            assertion_disarmed = true;
            break;
        }
        UNIT_ASSERT_C(assertion_disarmed, "No appropriate TEvPDiskStateUpdate received");
    };

    Y_UNIT_TEST(PDiskSlotSizeInUnits) {
        TActorTestContext testCtx({
            .IsBad=false,
            .DiskSize = 1_GB,
            .ChunkSize = 1_MB,
        });

        // Setup receiving whiteboard state updates
        testCtx.GetRuntime()->SetDispatchTimeout(10 * TDuration::MilliSeconds(testCtx.GetPDiskConfig()->StatisticsUpdateIntervalMs));
        testCtx.GetRuntime()->RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(1), testCtx.Sender);
        AwaitAndCheckEvPDiskStateUpdate(testCtx, 0u, 0);

        // Setup 2 vdisks
        TVDiskMock vdisk1(&testCtx);
        TVDiskMock vdisk2(&testCtx);

        vdisk1.InitFull(0u);
        vdisk2.InitFull(4u);

        vdisk1.ReserveChunk();
        vdisk2.ReserveChunk();

        vdisk1.CommitReservedChunks();
        vdisk2.CommitReservedChunks();

        // State:
        // PDisk.SlotSizeUnits: 0
        // Owners.GroupSizeInUnits: [0, 4]

        // Assert NumActiveSlots == 5
        const auto evCheckSpaceResponse1 = testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
            new NPDisk::TEvCheckSpace(vdisk1.PDiskParams->Owner, vdisk1.PDiskParams->OwnerRound),
            NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(evCheckSpaceResponse1->NumActiveSlots, 5);
        AwaitAndCheckEvPDiskStateUpdate(testCtx, 0u, 5);

        // Graceful restart with same config
        testCtx.GracefulPDiskRestart();
        vdisk1.InitFull(0u);
        vdisk1.SendEvLogSync();

        // Don't restart vdisk2 yet, check NumActiveSlots
        const auto evCheckSpaceResponse2 = testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
            new NPDisk::TEvCheckSpace(vdisk1.PDiskParams->Owner, vdisk1.PDiskParams->OwnerRound),
            NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(evCheckSpaceResponse2->NumActiveSlots, 5);
        AwaitAndCheckEvPDiskStateUpdate(testCtx, 0u, 5);

        // Check YardResize handler validates Owner and OwnerRound
        testCtx.TestResponse<NPDisk::TEvYardResizeResult>(
            new NPDisk::TEvYardResize(vdisk1.PDiskParams->Owner-1, 1, 2u),
            NKikimrProto::INVALID_OWNER);
        testCtx.TestResponse<NPDisk::TEvYardResizeResult>(
            new NPDisk::TEvYardResize(NPDisk::OwnerEndUser-1, 1, 2u),
            NKikimrProto::INVALID_OWNER);
        testCtx.TestResponse<NPDisk::TEvYardResizeResult>(
            new NPDisk::TEvYardResize(NPDisk::OwnerSystem, 1, 2u),
            NKikimrProto::ERROR);
        testCtx.TestResponse<NPDisk::TEvYardResizeResult>(
            new NPDisk::TEvYardResize(vdisk1.PDiskParams->Owner, vdisk1.PDiskParams->OwnerRound-1, 2u),
            NKikimrProto::INVALID_ROUND);

        // Successfull YardResize for vdiskId1 with GroupSizeInUnits=2
        testCtx.TestResponse<NPDisk::TEvYardResizeResult>(
            new NPDisk::TEvYardResize(vdisk1.PDiskParams->Owner, vdisk1.PDiskParams->OwnerRound, 2u),
            NKikimrProto::OK);

        // State:
        // PDisk.SlotSizeUnits: 0
        // Owners.GroupSizeInUnits: [2, 4]

        // Assert NumActiveSlots == 6
        const auto evCheckSpaceResponse3 = testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
            new NPDisk::TEvCheckSpace(vdisk1.PDiskParams->Owner, vdisk1.PDiskParams->OwnerRound),
            NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(evCheckSpaceResponse3->NumActiveSlots, 6);
        AwaitAndCheckEvPDiskStateUpdate(testCtx, 0u, 6);

        // Graceful restart with SlotSizeInUnits=2
        auto cfg = testCtx.GetPDiskConfig();
        cfg->SlotSizeInUnits = 2;
        testCtx.UpdateConfigRecreatePDisk(cfg);
        TVDiskMock vdisk3(&testCtx);
        vdisk3.InitFull(2u);

        // State:
        // PDisk.SlotSizeUnits: 2
        // Owners.GroupSizeInUnits: [2, 4, 2]

        // Assert NumActiveSlots == 4
        const auto evCheckSpaceResponse4 = testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
            new NPDisk::TEvCheckSpace(vdisk3.PDiskParams->Owner, vdisk3.PDiskParams->OwnerRound),
            NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(evCheckSpaceResponse4->NumActiveSlots, 4);
        AwaitAndCheckEvPDiskStateUpdate(testCtx, 2u, 4);
    }

    Y_UNIT_TEST(TestChunkWriteCrossOwner) {
        TActorTestContext testCtx{{}};

        TVDiskMock vdisk1(&testCtx);
        TVDiskMock vdisk2(&testCtx);

        vdisk1.InitFull();
        vdisk2.InitFull();

        vdisk1.ReserveChunk();
        vdisk2.ReserveChunk();

        vdisk1.CommitReservedChunks();
        vdisk2.CommitReservedChunks();

        UNIT_ASSERT(vdisk1.Chunks[EChunkState::COMMITTED].size() == 1);
        UNIT_ASSERT(vdisk2.Chunks[EChunkState::COMMITTED].size() == 1);

        auto chunk1 = *vdisk1.Chunks[EChunkState::COMMITTED].begin();
        auto chunk2 = *vdisk2.Chunks[EChunkState::COMMITTED].begin();

        auto parts = MakeIntrusive<NPDisk::TEvChunkWrite::TAlignedParts>(TString(123, '0'));

        // write to own chunk is OK
        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(new NPDisk::TEvChunkWrite(
            vdisk1.PDiskParams->Owner, vdisk1.PDiskParams->OwnerRound,
            chunk1, 0, parts, nullptr, false, 0),
            NKikimrProto::OK);
        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(new NPDisk::TEvChunkWrite(
            vdisk2.PDiskParams->Owner, vdisk2.PDiskParams->OwnerRound,
            chunk2, 0, parts, nullptr, false, 0),
            NKikimrProto::OK);

        // write to neighbour's chunk is ERROR
        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(new NPDisk::TEvChunkWrite(
            vdisk1.PDiskParams->Owner, vdisk1.PDiskParams->OwnerRound,
            chunk2, 0, parts, nullptr, false, 0),
            NKikimrProto::ERROR);
        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(new NPDisk::TEvChunkWrite(
            vdisk2.PDiskParams->Owner, vdisk2.PDiskParams->OwnerRound,
            chunk1, 0, parts, nullptr, false, 0),
            NKikimrProto::ERROR);
    }

    Y_UNIT_TEST(AllRequestsAreAnsweredOnPDiskRestart) {
        TActorTestContext testCtx({ false });
        TVDiskMock vdisk(&testCtx);

        vdisk.InitFull();
        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        UNIT_ASSERT(vdisk.Chunks[EChunkState::COMMITTED].size() == 1);
        auto chunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

        ui32 logBuffSize = 250;
        ui32 chunkBuffSize = 128_KB;

        for (ui32 testCase = 0; testCase < 8; testCase++) {
            Cerr << "restart# " << bool(testCase & 4) << " start with noop scheduler# " << bool(testCase & 1)
                << " end with noop scheduler# " << bool(testCase & 2) << Endl;
            testCtx.SafeRunOnPDisk([=] (NPDisk::TPDisk* pdisk) {
                pdisk->UseNoopSchedulerHDD = testCase & 1;
                pdisk->UseNoopSchedulerSSD = testCase & 1;
            });

            vdisk.InitFull();
            for (ui32 i = 0; i < 100; ++i) {
                testCtx.Send(new NPDisk::TEvLog(
                    vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound, 0, TRcBuf(PrepareData(logBuffSize)), vdisk.GetLsnSeg(), nullptr));
                auto parts = MakeIntrusive<NPDisk::TEvChunkWrite::TAlignedParts>(PrepareData(chunkBuffSize));
                testCtx.Send(new NPDisk::TEvChunkWrite(
                    vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    chunk, 0, parts, nullptr, false, 0));
                testCtx.Send(new NPDisk::TEvChunkRead(
                    vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    chunk, 0, chunkBuffSize, 0, nullptr));
            }

            if (testCase & 4) {
                Cerr << "restart" << Endl;
                testCtx.RestartPDiskSync();
            }

            testCtx.SafeRunOnPDisk([=] (NPDisk::TPDisk* pdisk) {
                pdisk->UseNoopSchedulerHDD = testCase & 2;
                pdisk->UseNoopSchedulerSSD = testCase & 2;
            });


            for (ui32 i = 0; i < 100; ++i) {
                auto read = testCtx.Recv<NPDisk::TEvChunkReadResult>();
            }
            Cerr << "all chunk reads are received" << Endl;

            for (ui32 i = 0; i < 100; ++i) {
                auto write = testCtx.Recv<NPDisk::TEvChunkWriteResult>();
            }
            Cerr << "all chunk writes are received" << Endl;

            for (ui32 i = 0; i < 100;) {
                auto result = testCtx.Recv<NPDisk::TEvLogResult>();
                i += result->Results.size();
            }
            Cerr << "all log writes are received" << Endl;
        }
    }

    NPDisk::TEvChunkWrite::TPartsPtr GenAlignedPart(size_t size, bool useRope) {
        if (useRope) {
            TRope rope(PrepareData(size));
            return MakeIntrusive<NPDisk::TEvChunkWrite::TRopeAlignedParts>(std::move(rope), size);
        } else {
            static TString data = PrepareData(size);
            TString copy = data;
            return MakeIntrusive<NPDisk::TEvChunkWrite::TAlignedParts>(std::move(copy));
        }
    }

    TString ConvertIPartsToString(NPDisk::TEvChunkWrite::IParts* parts) {
        auto data = TString::Uninitialized(parts->ByteSize());
        char *ptr = data.Detach();
        for (ui32 i = 0; i < parts->Size(); ++i) {
            auto [buf, bufSize] = (*parts)[i];
            memcpy(ptr, buf, bufSize);
            ptr += bufSize;
        }
        return data;
    }

    void ChunkWriteDifferentOffsetAndSizeImpl(bool plainDataChunks) {
        TActorTestContext testCtx({
            .PlainDataChunks = plainDataChunks,
        });
        Cerr << "plainDataChunks# " << plainDataChunks << Endl;

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        UNIT_ASSERT(vdisk.Chunks[EChunkState::COMMITTED].size() == 1);
        const ui32 reservedChunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

        auto seed = TInstant::Now().MicroSeconds();
        Cerr << "seed# " << seed << Endl;
        TReallyFastRng32 rng(seed);

        auto blockSize = vdisk.PDiskParams->AppendBlockSize;
        size_t maxSize = 8 * blockSize;
        for (ui32 offset = 0; offset <= vdisk.PDiskParams->ChunkSize - maxSize; offset += rng.Uniform(vdisk.PDiskParams->ChunkSize / 100)) {
            offset = offset / blockSize * blockSize;
            auto size = rng.Uniform(1, maxSize + 1); // + 1 for maxSize to be included in distribution
            Cerr << "offset# " << offset << " size# " << size << Endl;
            NPDisk::TEvChunkWrite::TPartsPtr parts = GenParts(rng, size);
            testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                    new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                        reservedChunk, offset, parts, nullptr, false, 0),
                    NKikimrProto::OK);
            auto res = testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                    new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                        reservedChunk, offset, size, 0, 0),
                    NKikimrProto::OK);
            Cerr << ConvertIPartsToString(parts.Get()).size() << " ?= " << res->Data.ToString().size() << Endl;
            UNIT_ASSERT_EQUAL(ConvertIPartsToString(parts.Get()).size(), res->Data.ToString().size());

            if (false) {
                Cerr << "ConvertIPartsToString(parts.Get())# ";
                Cerr << ConvertIPartsToString(parts.Get()).Quote() << Endl;
                Cerr << "res->Data.ToString()# ";
                auto x = res->Data.ToString();
                Cerr << TString(x.data(), x.size()).Quote() << Endl;
            }
            UNIT_ASSERT(ConvertIPartsToString(parts.Get()) == res->Data.ToString().Slice());
        }
    }
    Y_UNIT_TEST(ChunkWriteDifferentOffsetAndSize) {
        for (int i = 0; i <= 1; ++i) {
            ChunkWriteDifferentOffsetAndSizeImpl(i);
        }
    }

    Y_UNIT_TEST(PlainChunksWriteReadALot) {
        TActorTestContext testCtx{{
            .PlainDataChunks = true,
        }};

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        for (ui32 i = 0; i < 10; ++i) {
            vdisk.ReserveChunk();
        }
        vdisk.CommitReservedChunks();
        UNIT_ASSERT(vdisk.Chunks[EChunkState::COMMITTED].size() >= 1);
        const ui32 reservedChunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

        auto seed = TInstant::Now().MicroSeconds();
        Cerr << "seed# " << seed << Endl;
        TReallyFastRng32 rng(seed);

        size_t size = 2_MB;
        const bool useRope = false;
        size = size / vdisk.PDiskParams->AppendBlockSize * vdisk.PDiskParams->AppendBlockSize;
        size_t written = 0;
        auto duration = TDuration::Seconds(15);
        TInstant end = TInstant::Now() + duration;
        while (TInstant::Now() < end) {
            for (ui32 offset = 0; offset <= vdisk.PDiskParams->ChunkSize - size; offset += size) {
                NPDisk::TEvChunkWrite::TPartsPtr parts = GenAlignedPart(size, useRope);
                testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                        new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                            reservedChunk, offset, parts, nullptr, false, 0),
                        NKikimrProto::OK);
                auto res = testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                        new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                            reservedChunk, offset, size, 0, 0),
                        NKikimrProto::OK);
                written += size;
                UNIT_ASSERT_EQUAL(ConvertIPartsToString(parts.Get()).size(), res->Data.ToString().size());

                if (false) {
                    Cerr << "ConvertIPartsToString(parts.Get())# ";
                    Cerr << ConvertIPartsToString(parts.Get()).Quote() << Endl;
                    Cerr << "res->Data.ToString()# ";
                    auto x = res->Data.ToString();
                    Cerr << TString(x.data(), x.size()).Quote() << Endl;
                }
                UNIT_ASSERT(ConvertIPartsToString(parts.Get()) == res->Data.ToString().Slice());
            }
        }
        Cerr << " total_speed# " << 2 * written / duration.SecondsFloat() / 1e9 << " GB/s" << Endl;
    }

    Y_UNIT_TEST(ChunkWriteBadOffset) {
        TActorTestContext testCtx{{}};

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        UNIT_ASSERT(vdisk.Chunks[EChunkState::COMMITTED].size() == 1);
        const ui32 reservedChunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

        auto seed = TInstant::Now().MicroSeconds();
        Cerr << "seed# " << seed << Endl;
        TReallyFastRng32 rng(seed);

        auto blockSize = vdisk.PDiskParams->AppendBlockSize;
        for (ui32 offset = 1; offset < blockSize; offset += rng.Uniform(1, blockSize / 20)) {
            NPDisk::TEvChunkWrite::TPartsPtr parts = GenParts(rng, 1);
            testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                    new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                        reservedChunk, offset, parts, nullptr, false, 0),
                    NKikimrProto::ERROR);
        }
    }

    Y_UNIT_TEST(TestStartEncryptedOrPlainAndRestart) {
        for (ui32 plain = 0; plain <= 1; ++plain) {
            TActorTestContext testCtx({
                .PlainDataChunks = static_cast<bool>(plain),
            });
            TVDiskMock vdisk(&testCtx);

            vdisk.InitFull();
            vdisk.ReserveChunk();
            vdisk.CommitReservedChunks();
            UNIT_ASSERT(vdisk.Chunks[EChunkState::COMMITTED].size() == 1);
            auto chunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

            ui32 logBuffSize = 250;
            ui32 chunkBuffSize = 128_KB;

            for (ui32 testCase = 0; testCase < 4; testCase++) {
                Cerr << "testCase# " << testCase << Endl;
                auto cfg = testCtx.GetPDiskConfig();
                cfg->PlainDataChunks = testCase & 1;
                Cerr << "plainDataChunk# " << cfg->PlainDataChunks  << Endl;
                testCtx.UpdateConfigRecreatePDisk(cfg);

                vdisk.InitFull();
                for (ui32 i = 0; i < 100; ++i) {
                    testCtx.Send(new NPDisk::TEvLog(
                        vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound, 0, TRcBuf(PrepareData(logBuffSize)), vdisk.GetLsnSeg(), nullptr));
                    auto parts = MakeIntrusive<NPDisk::TEvChunkWrite::TAlignedParts>(PrepareData(chunkBuffSize));
                    testCtx.Send(new NPDisk::TEvChunkWrite(
                        vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                        chunk, 0, parts, nullptr, false, 0));
                    testCtx.Send(new NPDisk::TEvChunkRead(
                        vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                        chunk, 0, chunkBuffSize, 0, nullptr));
                }

                if (testCase & 2) {
                    Cerr << "restart" << Endl;
                    testCtx.RestartPDiskSync();
                }

                for (ui32 i = 0; i < 100; ++i) {
                    auto read = testCtx.Recv<NPDisk::TEvChunkReadResult>();
                }
                Cerr << "all chunk reads are received" << Endl;

                for (ui32 i = 0; i < 100; ++i) {
                    auto write = testCtx.Recv<NPDisk::TEvChunkWriteResult>();
                }
                Cerr << "all chunk writes are received" << Endl;

                for (ui32 i = 0; i < 100;) {
                    auto result = testCtx.Recv<NPDisk::TEvLogResult>();
                    i += result->Results.size();
                }
                Cerr << "all log writes are received" << Endl;
            }

            Cerr << "reformat" << Endl;
            auto cfg = testCtx.GetPDiskConfig();
            cfg->PlainDataChunks = true;
            testCtx.Settings.PlainDataChunks = true;
            testCtx.UpdateConfigRecreatePDisk(cfg, true);
            testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                new NPDisk::TEvChunkRead( vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    chunk, 0, chunkBuffSize, 0, nullptr),
                NKikimrProto::CORRUPTED);

            TVDiskMock vdisk2(&testCtx);
            vdisk2.InitFull();
        }
    }
}

Y_UNIT_TEST_SUITE(PDiskCompatibilityInfo) {
    using TCurrent = NKikimrConfig::TCurrentCompatibilityInfo;
    THolder<NPDisk::TEvYardInitResult> RestartPDisk(TActorTestContext& testCtx, ui32 pdiskId, TVDiskMock& vdisk, TCurrent* newInfo) {
        TCompatibilityInfoTest::Reset(newInfo);
        Y_UNUSED(pdiskId);
        testCtx.GracefulPDiskRestart();
        testCtx.Send(new NPDisk::TEvYardInit(vdisk.OwnerRound.fetch_add(1), vdisk.VDiskID, testCtx.TestCtx.PDiskGuid));
        return testCtx.Recv<NPDisk::TEvYardInitResult>();
    }

    void TestRestartWithDifferentVersion(TCurrent oldInfo, TCurrent newInfo, bool isCompatible, bool suppressCompatibilityCheck = false) {
        TCompatibilityInfoTest::Reset(&oldInfo);

        TActorTestContext testCtx({
            .IsBad = false,
            .SuppressCompatibilityCheck = suppressCompatibilityCheck,
        });

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        auto pdiskId = testCtx.GetPDisk()->PCtx->PDiskId;

        const auto evInitRes = RestartPDisk(testCtx, pdiskId, vdisk, &newInfo);
        if (isCompatible) {
            UNIT_ASSERT(evInitRes->Status == NKikimrProto::OK);
        } else {
            UNIT_ASSERT(evInitRes->Status != NKikimrProto::OK);
        }
    }

    void TestMajorVerionMigration(TCurrent oldInfo, TCurrent intermediateInfo, TCurrent newInfo) {
        TCompatibilityInfoTest::Reset(&oldInfo);

        TActorTestContext testCtx({
            .IsBad = false,
            .SuppressCompatibilityCheck = false,
        });

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        auto pdiskId = testCtx.GetPDisk()->PCtx->PDiskId;

        {
            const auto evInitRes = RestartPDisk(testCtx, pdiskId, vdisk, &intermediateInfo);
            UNIT_ASSERT(evInitRes->Status == NKikimrProto::OK);
        }

        {
            const auto evInitRes = RestartPDisk(testCtx, pdiskId, vdisk, &newInfo);
            UNIT_ASSERT(evInitRes->Status == NKikimrProto::OK);
        }
    }

    Y_UNIT_TEST(OldCompatible) {
        TestRestartWithDifferentVersion(
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TCompatibilityInfo::TProtoConstructor::TVersion{ .Year = 23, .Major = 1, .Minor = 26, .Hotfix = 0 },
            }.ToPB(),
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TCompatibilityInfo::TProtoConstructor::TVersion{ .Year = 23, .Major = 2, .Minor = 1, .Hotfix = 0 },
            }.ToPB(),
            true
        );
    }

    Y_UNIT_TEST(Incompatible) {
        TestRestartWithDifferentVersion(
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TCompatibilityInfo::TProtoConstructor::TVersion{ .Year = 23, .Major = 1, .Minor = 26, .Hotfix = 0 },
            }.ToPB(),
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TCompatibilityInfo::TProtoConstructor::TVersion{ .Year = 23, .Major = 3, .Minor = 1, .Hotfix = 0 },
            }.ToPB(),
            false
        );
    }

    Y_UNIT_TEST(NewIncompatibleWithDefault) {
        TestRestartWithDifferentVersion(
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TCompatibilityInfo::TProtoConstructor::TVersion{ .Year = 24, .Major = 3, .Minor = 1, .Hotfix = 0 },
            }.ToPB(),
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TCompatibilityInfo::TProtoConstructor::TVersion{ .Year = 24, .Major = 4, .Minor = 1, .Hotfix = 0 },
            }.ToPB(),
            true
        );
    }

    Y_UNIT_TEST(Trunk) {
        TestRestartWithDifferentVersion(
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "ydb",
            }.ToPB(),
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "ydb",
            }.ToPB(),
            true
        );
    }

    Y_UNIT_TEST(SuppressCompatibilityCheck) {
        TestRestartWithDifferentVersion(
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "trunk",
            }.ToPB(),
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TCompatibilityInfo::TProtoConstructor::TVersion{ .Year = 23, .Major = 3, .Minor = 8, .Hotfix = 0 },
            }.ToPB(),
            true,
            true
        );
    }

    Y_UNIT_TEST(Migration) {
        TestMajorVerionMigration(
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TCompatibilityInfo::TProtoConstructor::TVersion{ .Year = 23, .Major = 3, .Minor = 20, .Hotfix = 0 },
            }.ToPB(),
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TCompatibilityInfo::TProtoConstructor::TVersion{ .Year = 23, .Major = 4, .Minor = 1, .Hotfix = 0 },
            }.ToPB(),
            TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo{
                .Application = "ydb",
                .Version = TCompatibilityInfo::TProtoConstructor::TVersion{ .Year = 23, .Major = 5, .Minor = 1, .Hotfix = 0 },
            }.ToPB()
        );
    }

}


Y_UNIT_TEST_SUITE(WilsonTrace) {
    Y_UNIT_TEST(LogWriteChunkWriteChunkRead) {
        TActorTestContext testCtx{{}};
        auto* uploader = testCtx.WilsonUploader;

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        const ui32 reservedChunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

        ui64 seed = 0;
        size_t size = 1_MB;
        size_t offset = 0;
        TReallyFastRng32 rng(seed);
        NPDisk::TEvChunkWrite::TPartsPtr parts = GenParts(rng, size);
        Ctest << "offset# " << offset << " size# " << size << Endl;
        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, offset, parts, nullptr, false, 0),
                NKikimrProto::OK);
        testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    reservedChunk, offset, size, 0, 0),
                NKikimrProto::OK);

        UNIT_ASSERT(uploader->BuildTraceTrees());

        UNIT_ASSERT(uploader->BuildTraceTrees());
        UNIT_ASSERT(uploader->Traces.size() > 0);

        TStringStream str;
        for (auto& [_, trace] : uploader->Traces) {
            str << trace.ToString() << Endl;
        }
        auto string = str.Str();
        Cerr << string;
        UNIT_ASSERT(string.Contains("LogWrite"));
        UNIT_ASSERT(string.Contains("LogRead"));
        UNIT_ASSERT(string.Contains("ChunkWrite"));
        UNIT_ASSERT(string.Contains("ChunkRead"));
    }
}


Y_UNIT_TEST_SUITE(ReadOnlyPDisk) {
    Y_UNIT_TEST(SimpleRestartReadOnly) {
        TActorTestContext testCtx{{}};

        auto cfg = testCtx.GetPDiskConfig();
        cfg->ReadOnly = true;
        testCtx.UpdateConfigRecreatePDisk(cfg);
    }

    Y_UNIT_TEST(StartReadOnlyUnformattedShouldFail) {
        TActorTestContext testCtx{{
            .ReadOnly = true,
        }};
        auto res = testCtx.TestResponse<NPDisk::TEvYardControlResult>(
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStart, (void*)(&testCtx.MainKey)),
                NKikimrProto::CORRUPTED);

        UNIT_ASSERT_STRING_CONTAINS(res->ErrorReason, "Magic sector is not present on disk");
    }

    Y_UNIT_TEST(StartReadOnlyZeroedShouldFail) {
        TActorTestContext testCtx{{
            .ReadOnly = true,
            .InitiallyZeroed = true,
        }};
        auto res = testCtx.TestResponse<NPDisk::TEvYardControlResult>(
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStart, (void*)(&testCtx.MainKey)),
                NKikimrProto::CORRUPTED);

        UNIT_ASSERT_STRING_CONTAINS(res->ErrorReason, "PDisk is in read-only mode");
    }

    Y_UNIT_TEST(VDiskStartsOnReadOnlyPDisk) {
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();

        auto cfg = testCtx.GetPDiskConfig();
        cfg->ReadOnly = true;
        testCtx.UpdateConfigRecreatePDisk(cfg);

        vdisk.Init(); // Should start ok.
        vdisk.ReadLog(); // Should be able to read log.
    }

    template <class Request, class Response, NKikimrProto::EReplyStatus ExpectedStatus = NKikimrProto::CORRUPTED, class... Args>
    auto CheckReadOnlyRequest(Args&&... args) {
        return [args = std::make_tuple(std::forward<Args>(args)...)](TActorTestContext& testCtx) {
            Request* req = std::apply([](auto&&... unpackedArgs) {
                return new Request(std::forward<decltype(unpackedArgs)>(unpackedArgs)...);
            }, args);

            THolder<Response> res = testCtx.TestResponse<Response>(req);

            UNIT_ASSERT_VALUES_EQUAL(res->Status, ExpectedStatus);
            UNIT_ASSERT_STRING_CONTAINS(res->ErrorReason, "PDisk is in read-only mode");
        };
    }

    Y_UNIT_TEST(ReadOnlyPDiskEvents) {
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();

        auto cfg = testCtx.GetPDiskConfig();
        cfg->ReadOnly = true;
        testCtx.UpdateConfigRecreatePDisk(cfg);

        vdisk.Init();
        vdisk.ReadLog();

        std::vector<std::function<void(TActorTestContext&)>> eventSenders = {
            // Should fail on writing log. (ERequestType::RequestLogWrite)
            CheckReadOnlyRequest<NPDisk::TEvLog, NPDisk::TEvLogResult>(
                vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound, 0,
                TRcBuf(PrepareData(1)), TLsnSeg(), nullptr
            ),
            // Should fail on writing chunk. (ERequestType::RequestChunkWrite)
            CheckReadOnlyRequest<NPDisk::TEvChunkWrite, NPDisk::TEvChunkWriteResult>(
                vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                0, 0, nullptr, nullptr, false, 0
            ),
            // Should fail on reserving chunk. (ERequestType::RequestChunkReserve)
            CheckReadOnlyRequest<NPDisk::TEvChunkReserve, NPDisk::TEvChunkReserveResult>(
                vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound, 1
            ),
            // Should fail on chunk lock. (ERequestType::RequestChunkLock)
            CheckReadOnlyRequest<NPDisk::TEvChunkLock, NPDisk::TEvChunkLockResult>(
                NPDisk::TEvChunkLock::ELockFrom::LOG, 0, NKikimrBlobStorage::TPDiskSpaceColor::YELLOW
            ),
            // Should fail on chunk unlock. (ERequestType::RequestChunkUnlock)
            CheckReadOnlyRequest<NPDisk::TEvChunkUnlock, NPDisk::TEvChunkUnlockResult>(
                NPDisk::TEvChunkLock::ELockFrom::LOG
            ),
            // Should fail on chunk forget. (ERequestType::RequestChunkForget)
            CheckReadOnlyRequest<NPDisk::TEvChunkForget, NPDisk::TEvChunkForgetResult>(
                vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound + 1
            ),
            // Should fail on harakiri. (ERequestType::RequestHarakiri)
            CheckReadOnlyRequest<NPDisk::TEvHarakiri, NPDisk::TEvHarakiriResult>(
                vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound + 1
            ),
            // Should fail on slaying vdisk. (ERequestType::RequestYardSlay)
            CheckReadOnlyRequest<NPDisk::TEvSlay, NPDisk::TEvSlayResult, NKikimrProto::NOTREADY>(
                vdisk.VDiskID, vdisk.PDiskParams->OwnerRound + 1, 0, 0
            )
        };

        for (auto sender : eventSenders) {
            sender(testCtx);
        }
    }
}

Y_UNIT_TEST_SUITE(ShredPDisk) {
    Y_UNIT_TEST(EmptyShred) {
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        THolder<NPDisk::TEvShredPDiskResult> res = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(new NPDisk::TEvShredPDisk(shredGeneration), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res->ShredGeneration, shredGeneration);
    }
    Y_UNIT_TEST(SimpleShred) {
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
        vdisk.RespondToCutLog();
        THolder<NPDisk::TEvShredPDiskResult> res = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res->ShredGeneration, shredGeneration);
    }
    Y_UNIT_TEST(SimpleShredRepeat) {
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
        vdisk.RespondToCutLog();
        THolder<NPDisk::TEvShredPDiskResult> res = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res->ShredGeneration, shredGeneration);

        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        res = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->ShredGeneration, shredGeneration);
    }
    Y_UNIT_TEST(SimpleShredRepeatAfterPDiskRestart) {
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
        vdisk.RespondToCutLog();
        THolder<NPDisk::TEvShredPDiskResult> res = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res->ShredGeneration, shredGeneration);

        testCtx.RestartPDiskSync();
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        res = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->ShredGeneration, shredGeneration);
    }
    Y_UNIT_TEST(SimpleShredDirtyChunks) {
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        vdisk.MarkCommitedChunksDirty();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
        if (NPDisk::TPDisk::IS_SHRED_ENABLED) {
            vdisk.RespondToShred(shredGeneration, NKikimrProto::OK, "");
        }
        vdisk.RespondToCutLog();
        THolder<NPDisk::TEvShredPDiskResult> res = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res->ShredGeneration, shredGeneration);
    }
    Y_UNIT_TEST(KillVDiskWhilePreShredding) {
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        THolder<NPDisk::TEvPreShredCompactVDisk> evReq = testCtx.Recv<NPDisk::TEvPreShredCompactVDisk>();
        UNIT_ASSERT_VALUES_UNEQUAL(evReq.Get(), nullptr);
        vdisk.PerformHarakiri();
        THolder<NPDisk::TEvShredPDiskResult> res = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(
            nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res->ShredGeneration, shredGeneration);
    }
    Y_UNIT_TEST(KillVDiskWhileShredding) {
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        vdisk.MarkCommitedChunksDirty();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
        if (NPDisk::TPDisk::IS_SHRED_ENABLED) {
            THolder<NPDisk::TEvShredVDisk> evReq = testCtx.Recv<NPDisk::TEvShredVDisk>();
            UNIT_ASSERT_VALUES_UNEQUAL(evReq.Get(), nullptr);
        }
        vdisk.PerformHarakiri();
        THolder<NPDisk::TEvShredPDiskResult> res = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(
            nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res->ShredGeneration, shredGeneration);
    }
    Y_UNIT_TEST(InitVDiskAfterShredding) {
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        TVDiskMock vdisk2(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        vdisk2.InitFull();
        vdisk2.SendEvLogSync();
        testCtx.RestartPDiskSync();

        vdisk.InitFull();
        vdisk.SendEvLogSync();
        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        vdisk.MarkCommitedChunksDirty();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
        vdisk2.InitFull();
        vdisk2.ReserveChunk();
        vdisk2.CommitReservedChunks();
        vdisk2.MarkCommitedChunksDirty();
        vdisk2.SendEvLogSync();
        vdisk2.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
        if (NPDisk::TPDisk::IS_SHRED_ENABLED) {
            vdisk.RespondToShred(shredGeneration, NKikimrProto::OK, "");
            vdisk2.RespondToShred(shredGeneration, NKikimrProto::OK, "");
        }
        vdisk.RespondToCutLog();
        vdisk2.RespondToCutLog();
        THolder<NPDisk::TEvShredPDiskResult> res = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res->ShredGeneration, shredGeneration);
    }
    Y_UNIT_TEST(ReinitVDiskWhilePreShredding) {
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        vdisk.MarkCommitedChunksDirty();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        THolder<NPDisk::TEvPreShredCompactVDisk> evReq = testCtx.Recv<NPDisk::TEvPreShredCompactVDisk>();
        UNIT_ASSERT_VALUES_UNEQUAL(evReq.Get(), nullptr);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
        if (NPDisk::TPDisk::IS_SHRED_ENABLED) {
            vdisk.RespondToShred(shredGeneration, NKikimrProto::OK, "");
        }
        vdisk.RespondToCutLog();
        THolder<NPDisk::TEvShredPDiskResult> res = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res->ShredGeneration, shredGeneration);
    }
    Y_UNIT_TEST(ReinitVDiskWhileShredding) {
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        vdisk.MarkCommitedChunksDirty();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
        if (NPDisk::TPDisk::IS_SHRED_ENABLED) {
            THolder<NPDisk::TEvShredVDisk> evReq = testCtx.Recv<NPDisk::TEvShredVDisk>();
            UNIT_ASSERT_VALUES_UNEQUAL(evReq.Get(), nullptr);
        }
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        if (NPDisk::TPDisk::IS_SHRED_ENABLED) {
            vdisk.RespondToShred(shredGeneration, NKikimrProto::OK, "");
        }
        vdisk.RespondToCutLog();
        THolder<NPDisk::TEvShredPDiskResult> res = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res->ShredGeneration, shredGeneration);
    }
    Y_UNIT_TEST(RetryPreShredCompactError) {
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        vdisk.MarkCommitedChunksDirty();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::ERROR, "");
        THolder<NPDisk::TEvShredPDiskResult> res1 = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(res1->ShredGeneration, shredGeneration);
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
        if (NPDisk::TPDisk::IS_SHRED_ENABLED) {
            vdisk.RespondToShred(shredGeneration, NKikimrProto::OK, "");
        }
        vdisk.RespondToCutLog();
        THolder<NPDisk::TEvShredPDiskResult> res2 = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res2->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res2->ShredGeneration, shredGeneration);
    }
    Y_UNIT_TEST(RetryShredError) {
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        vdisk.MarkCommitedChunksDirty();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
        if (NPDisk::TPDisk::IS_SHRED_ENABLED) {
            vdisk.RespondToShred(shredGeneration, NKikimrProto::ERROR, "");
            THolder<NPDisk::TEvShredPDiskResult> res1 = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::ERROR);
            UNIT_ASSERT_VALUES_EQUAL(res1->ShredGeneration, shredGeneration);

            testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
            vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
            vdisk.RespondToShred(shredGeneration, NKikimrProto::OK, "");
        }
        vdisk.RespondToCutLog();
        THolder<NPDisk::TEvShredPDiskResult> res2 = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res2->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res2->ShredGeneration, shredGeneration);
    }
#ifdef ENABLE_PDISK_SHRED
    Y_UNIT_TEST(RetryShredErrorAfterPDiskRestart) {
        UNIT_ASSERT_VALUES_EQUAL(true, NPDisk::TPDisk::IS_SHRED_ENABLED);
        ui64 shredGeneration = 1;
        TActorTestContext testCtx{{}};
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        vdisk.MarkCommitedChunksDirty();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        vdisk.RespondToPreShredCompact(shredGeneration, NKikimrProto::OK, "");
        vdisk.RespondToShred(shredGeneration, NKikimrProto::ERROR, "");
        THolder<NPDisk::TEvShredPDiskResult> res1 = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(res1->ShredGeneration, shredGeneration);
        testCtx.RestartPDiskSync();
        vdisk.InitFull();
        vdisk.SendEvLogSync();
        testCtx.Send(new NPDisk::TEvShredPDisk(shredGeneration));
        vdisk.RespondToShred(shredGeneration, NKikimrProto::OK, "");
        vdisk.RespondToCutLog();
        THolder<NPDisk::TEvShredPDiskResult> res2 = testCtx.TestResponse<NPDisk::TEvShredPDiskResult>(nullptr, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res2->ErrorReason, "");
        UNIT_ASSERT_VALUES_EQUAL(res2->ShredGeneration, shredGeneration);
    }
#endif
}

} // namespace NKikimr
