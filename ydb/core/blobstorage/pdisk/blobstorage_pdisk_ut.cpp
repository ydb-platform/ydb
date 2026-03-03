#include "blobstorage_pdisk_ut.h"

#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_params.h"
#include "blobstorage_pdisk_tools.h"
#include "blobstorage_pdisk_ut_env.h"

#include <type_traits>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/driver_lib/version/ut/ut_helpers.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

#include <util/system/hp_timer.h>

namespace NKikimr {

NPDisk::TEvChunkWrite::TPartsPtr GenParts(TReallyFastRng32& rng, size_t size) {
    if (size == 0) {
        return MakeIntrusive<NPDisk::TEvChunkWrite::TAlignedParts>(PrepareData(0));
    }
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
            partsCount = Min(partsCount, size);
            TRope rope;
            size_t createdBytes = 0;
            if (size >= partsCount) {
                for (size_t i = 0; i < partsCount - 1; ++i) {
                    TRope x(PrepareData(1 + rng.Uniform(size / partsCount)));
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
        TActorTestContext::TSettings settings{};
        settings.IsBad = true;
        TActorTestContext testCtx(settings);

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
        testCtx.TestResponse<NPDisk::TEvYardResizeResult>(
                new NPDisk::TEvYardResize(1, 1, 1),
                NKikimrProto::CORRUPTED);
        testCtx.TestResponse<NPDisk::TEvChangeExpectedSlotCountResult>(
                new NPDisk::TEvChangeExpectedSlotCount(1, 1u),
                NKikimrProto::CORRUPTED);

        testCtx.Send(new NActors::TEvents::TEvPoisonPill());
    }

    Y_UNIT_TEST(TestPDiskActorPDiskStopStart) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);

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
    }

    Y_UNIT_TEST(TestPDiskActorPDiskStopBroken) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);

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
    }

    Y_UNIT_TEST(TestPDiskActorPDiskStopUninitialized) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);

            testCtx.TestResponse<NPDisk::TEvYardControlResult>(
                    new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStop, nullptr),
                    NKikimrProto::OK);

            testCtx.Send(new NActors::TEvents::TEvPoisonPill());
        }
    }

    Y_UNIT_TEST(TestChunkWriteRelease) {
        for (ui32 i = 0; i < 16; ++i) {
            TestChunkWriteReleaseRun(true);
        }
    }

    Y_UNIT_TEST(TestChunkWriteReleaseNoEncryption) {
        for (ui32 i = 0; i < 16; ++i) {
            TestChunkWriteReleaseRun(false);
        }
    }

    Y_UNIT_TEST(TestPDiskOwnerRecreation) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);

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
    }

    Y_UNIT_TEST(TestPDiskOwnerRecreationWithStableOwner) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);

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
    }

    Y_UNIT_TEST(TestPDiskManyOwnersInitiation) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);

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
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);

            TVDiskMock mock(&testCtx);
            BasicTest(testCtx, mock);
        }
    }

    Y_UNIT_TEST(TestRealFile) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            settings.UseSectorMap = false;
            TActorTestContext testCtx(settings);

            TVDiskMock mock(&testCtx);

            BasicTest(testCtx, mock);
        }
    }

    Y_UNIT_TEST(TestLogWriteReadWithRestarts) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);

            TVDiskMock vdisk(&testCtx, true);
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
    }

    // Test to reproduce bug from KIKIMR-10192
    Y_UNIT_TEST(TestLogSpliceNonceJump) {
        for (bool encryption: {true, false}) {
            TActorTestContext testCtx({
                .IsBad = false,
                .DiskSize = 1ull << 30,
                .ChunkSize = 1ull * (1 << 20),
                .SmallDisk = true,
                .EnableFormatAndMetadataEncryption = encryption,
                .EnableSectorEncryption = encryption,
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
    }

    // Test to reproduce bug with multiple chunk splices from
    Y_UNIT_TEST(TestMultipleLogSpliceNonceJump) {
        for (bool encryption: {true, false}) {
            TActorTestContext testCtx({
                .IsBad = false,
                .DiskSize = 1ull << 30,
                .ChunkSize = 1ull * (1 << 20),
                .SmallDisk = true,
                .EnableFormatAndMetadataEncryption = encryption,
                .EnableSectorEncryption = encryption,
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
    }

    Y_UNIT_TEST(TestFakeErrorPDiskManyLogWrite) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);
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
    }

    Y_UNIT_TEST(TestFakeErrorPDiskLogRead) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);

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
    }

    Y_UNIT_TEST(TestFakeErrorPDiskSysLogRead) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);

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
    }

    Y_UNIT_TEST(TestFakeErrorPDiskManyChunkRead) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);
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
    }

    Y_UNIT_TEST(TestFakeErrorPDiskManyChunkWrite) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);
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
    }

    Y_UNIT_TEST(TestSIGSEGVInTUndelivered) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);
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
    }

    Y_UNIT_TEST(PDiskRestart) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);
            TVDiskMock vdisk(&testCtx);
            vdisk.InitFull();
            vdisk.SendEvLogSync();

            testCtx.GracefulPDiskRestart();

            vdisk.InitFull();
            vdisk.SendEvLogSync();
        }
    }

    Y_UNIT_TEST(StartWithAllEncryptionDisabled) {
        TActorTestContext::TSettings settings{};
        settings.UseSectorMap = false;

        TActorTestContext testCtx(settings);
        {
            auto cfg = testCtx.GetPDiskConfig();
            cfg->FeatureFlags.SetEnablePDiskDataEncryption(false);
            cfg->EnableFormatAndMetadataEncryption = false;
            testCtx.UpdateConfigRecreatePDisk(cfg, true);
        }

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        UNIT_ASSERT_VALUES_EQUAL(vdisk.Chunks[EChunkState::COMMITTED].size(), 1);
        const ui32 chunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();
        vdisk.SendEvLogSync();

        const TString writeData = PrepareData(4096);
        testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    chunk, 0, new NPDisk::TEvChunkWrite::TAlignedParts(TString(writeData)), nullptr, false, 0),
                NKikimrProto::OK);

        testCtx.RestartPDiskSync();

        vdisk.InitFull();

        const auto readRes = testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                    chunk, 0, writeData.size(), 0, nullptr),
                NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(readRes->Data.ToString(), writeData);
    }

    void AfterObliterateShouldNotReadLog(bool enableFormatAndMetadataEncryption) {
        // regression test for #31337
        const TString expectedLogData = PrepareData(12346);
        TActorTestContext::TSettings settings{};
        settings.UseSectorMap = true;
        settings.InitiallyZeroed = true;
        settings.ChunkSize = NPDisk::SmallDiskMaximumChunkSize;
        settings.DiskSize = (ui64)settings.ChunkSize * 50;
        settings.SmallDisk = true;
        settings.EnableFormatAndMetadataEncryption = enableFormatAndMetadataEncryption;
        settings.EnableSectorEncryption = false;
        settings.NonceRandNum = 13;

        TActorTestContext testCtx(settings);

        {
            TVDiskMock vdisk(&testCtx);
            vdisk.InitFull();
            vdisk.SendEvLogSync(12346);

            vdisk.Init();
            bool foundExpected = false;
            const ui64 logRecordsRead = vdisk.ReadLog(false, [&](const NPDisk::TLogRecord& rec) {
                if (rec.Data.size() == expectedLogData.size()) {
                    const TString data = TRcBuf(rec.Data).ExtractUnderlyingContainerOrCopy<TString>();
                    if (data == expectedLogData) {
                        foundExpected = true;
                    }
                }
            });
            UNIT_ASSERT(logRecordsRead > 0);
            UNIT_ASSERT(foundExpected);
        }

        {
            const ui64 formatBytes = NPDisk::FormatSectorSize * NPDisk::ReplicationFactor;
            const ui64 obliterateSectors = formatBytes /  NPDisk::NSectorMap::SECTOR_SIZE;
            testCtx.TestCtx.SectorMap->ZeroInit(obliterateSectors);

            // finally restart: disk must be clean/empty
            testCtx.RestartPDiskSync();

            TVDiskMock vdisk(&testCtx);
            vdisk.Init();
            const ui64 logRecordsRead = vdisk.ReadLog();
            UNIT_ASSERT_VALUES_EQUAL(logRecordsRead, 0UL);

            // if the old log is still readable, startup randomizes nonces by ForceLogNonceDiff
            const auto [nonceLog, nonceLogDiff] = testCtx.SafeRunOnPDisk([&](NPDisk::TPDisk* pdisk) {
                return std::pair{
                    pdisk->SysLogRecord.Nonces.Value[NPDisk::NonceLog],
                    pdisk->ForceLogNonceDiff.Value[NPDisk::NonceLog],
                };
            });

            const ui64 initialNonce = 1;
            ui64 nonceDelta = nonceLogDiff + 1 + *settings.NonceRandNum % nonceLogDiff;
            ui64 expectedNonce = initialNonce + nonceDelta + 1;
            UNIT_ASSERT_VALUES_EQUAL(nonceLog, expectedNonce);
        }
    }

    Y_UNIT_TEST(AfterObliterateShouldNotReadLog) {
        AfterObliterateShouldNotReadLog(true);
    }

    Y_UNIT_TEST(AfterObliterateShouldNotReadLogNoEncryption) {
        AfterObliterateShouldNotReadLog(false);
    }

    void AfterObliterateDontReuseOldChunkData(bool enableFormatAndMetadataEncryption) {
        // regression test for #31337
        const TString writeData = PrepareData(4096);
        const size_t chunkCount = 10;
        TVector<ui32> firstChunks;

        TActorTestContext::TSettings settings{};
        settings.UseSectorMap = true;
        settings.InitiallyZeroed = true;
        settings.ChunkSize = NPDisk::SmallDiskMaximumChunkSize;
        settings.DiskSize = (ui64)settings.ChunkSize * 50;
        settings.SmallDisk = true;
        settings.PlainDataChunks = false;
        settings.EnableFormatAndMetadataEncryption = enableFormatAndMetadataEncryption;
        settings.EnableSectorEncryption = false;
        settings.NonceRandNum = 13;

        TActorTestContext testCtx(settings);

        {
            TVDiskMock vdisk(&testCtx);
            vdisk.InitFull();

            // here we write unencrypted data
            for (ui32 i = 0; i < chunkCount; ++i) {
                vdisk.ReserveChunk();
            }
            vdisk.CommitReservedChunks();
            UNIT_ASSERT_VALUES_EQUAL(vdisk.Chunks[EChunkState::COMMITTED].size(), chunkCount);
            firstChunks.assign(vdisk.Chunks[EChunkState::COMMITTED].begin(), vdisk.Chunks[EChunkState::COMMITTED].end());
            vdisk.SendEvLogSync();

            for (ui32 chunkIdx = 0; chunkIdx < firstChunks.size(); ++chunkIdx) {
                testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                        new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                            firstChunks[chunkIdx], 0, new NPDisk::TEvChunkWrite::TAlignedParts(TString(writeData)), nullptr, false, 0),
                        NKikimrProto::OK);
            }

            // sanity check read
            for (ui32 chunkIdx = 0; chunkIdx < firstChunks.size(); ++chunkIdx) {
                const auto readRes = testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                    new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                        firstChunks[chunkIdx], 0, writeData.size(), 0, nullptr),
                    std::nullopt);

                if (readRes->Status == NKikimrProto::OK) {
                    UNIT_ASSERT(readRes->Data.IsReadable(0, writeData.size()));
                    UNIT_ASSERT_VALUES_EQUAL(writeData, readRes->Data.ToString());
                }
            }
        }

        // below we want format disk and check there is no way to read old chunks

        {
            // now, we format PDisk via obliterate
            const ui64 formatBytes = NPDisk::FormatSectorSize * NPDisk::ReplicationFactor;
            const ui64 obliterateSectors = formatBytes /  NPDisk::NSectorMap::SECTOR_SIZE;
            testCtx.TestCtx.SectorMap->ZeroInit(obliterateSectors);

            // also we zero log (it's beyond obliteration): with the bug we are fixing,
            // we were able to read the log and advance nonce as it would after regular
            // restart: with nonce advanced we can't read old chunk data. However,
            // it's still legit to have log zeroed (for example after dd'ing).

            ui64 sysLogOffsetBytes = 0;
            ui64 sysLogBytes = 0;
            ui64 chunkSizeBytes = 0;
            TSet<ui32> logChunkIds;
            testCtx.SafeRunOnPDisk([&](auto* pdisk) {
                const auto& format = pdisk->Format;
                sysLogOffsetBytes = format.FirstSysLogSectorIdx() * format.SectorSize;
                sysLogBytes = format.SysLogSectorCount * NPDisk::ReplicationFactor * format.SectorSize;
                chunkSizeBytes = format.ChunkSize;
                for (const auto& chunk : pdisk->LogChunks) {
                    logChunkIds.insert(chunk.ChunkIdx);
                }
                logChunkIds.insert(pdisk->SysLogRecord.LogHeadChunkIdx);
            });
            auto zeroRange = [&](ui64 offsetBytes, ui64 sizeBytes) {
                if (sizeBytes == 0) {
                    return;
                }
                const ui64 sectorSize = NPDisk::NSectorMap::SECTOR_SIZE;
                const ui64 alignedSize = (sizeBytes + sectorSize - 1) / sectorSize * sectorSize;
                NPDisk::TAlignedData zero(alignedSize);
                memset(zero.Get(), 0, alignedSize);
                testCtx.TestCtx.SectorMap->Write(zero.Get(), alignedSize, offsetBytes);
            };
            zeroRange(sysLogOffsetBytes, sysLogBytes);
            for (ui32 chunkIdx : logChunkIds) {
                zeroRange(chunkSizeBytes * chunkIdx, chunkSizeBytes);
            }

            // finally restart: disk must be clean/empty
            testCtx.RestartPDiskSync();

            // reserve chunks as we did before (but don't write to chunks)
            TVDiskMock vdisk(&testCtx);
            vdisk.InitFull();
            for (ui32 i = 0; i < chunkCount; ++i) {
                vdisk.ReserveChunk();
            }
            vdisk.CommitReservedChunks();
            UNIT_ASSERT_VALUES_EQUAL(vdisk.Chunks[EChunkState::COMMITTED].size(), chunkCount);
            TVector<ui32> newChunks(vdisk.Chunks[EChunkState::COMMITTED].begin(), vdisk.Chunks[EChunkState::COMMITTED].end());

            // note, chunk ids in firstChunks and newChunks might slightly differ,
            // but it is OK to check both

            // firstChunks
            for (ui32 chunkIdx = 0; chunkIdx < firstChunks.size(); ++chunkIdx) {
                const auto readRes = testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                        new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                            firstChunks[chunkIdx], 0, writeData.size(), 0, nullptr),
                        std::nullopt);

                if (readRes->Status == NKikimrProto::OK) {
                    UNIT_ASSERT(!readRes->Data.IsReadable(0, writeData.size()));
                }
            }

            // newChunks
            for (ui32 chunkIdx = 0; chunkIdx < newChunks.size(); ++chunkIdx) {
                const auto readRes = testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                        new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                            newChunks[chunkIdx], 0, writeData.size(), 0, nullptr),
                        std::nullopt);

                if (readRes->Status == NKikimrProto::OK) {
                    UNIT_ASSERT(!readRes->Data.IsReadable(0, writeData.size()));
                }
            }
        }
    }

    Y_UNIT_TEST(AfterObliterateDontReuseOldChunkData) {
        AfterObliterateDontReuseOldChunkData(true);
    }

    Y_UNIT_TEST(AfterObliterateDontReuseOldChunkDataNoEncryption) {
        AfterObliterateDontReuseOldChunkData(false);
    }

    void ShouldSwitchEncryptionOnOffWithoutReformatting(bool initialEncryption) {
        const TString expectedLogData = PrepareData(12346);
        const TString writeData = PrepareData(4096);

        TVector<ui32> chunkIds;
        size_t expectedLogCount = 0;
        ui32 metadataSeq = 0;
        TString lastMetadata;
        bool hasMetadata = false;

        // we start with a default settings, i.e. sector encryption is on
        TActorTestContext::TSettings settings{};
        settings.UseSectorMap = true;
        settings.InitiallyZeroed = true;
        settings.ChunkSize = NPDisk::SmallDiskMaximumChunkSize;
        settings.DiskSize = (ui64)settings.ChunkSize * 50;
        settings.SmallDisk = true;
        settings.PlainDataChunks = false;
        settings.EnableSectorEncryption = initialEncryption;

        TActorTestContext testCtx(settings);

        // our checker function
        auto verifyVdiskData = [&](TVDiskMock& vdisk, size_t expectedCount) {
            // Re-init to reset HasReadTheWholeLog before verification reads.
            vdisk.Init();
            // sanity log read
            size_t foundExpectedCount = 0;
            const ui64 logRecordsRead = vdisk.ReadLog(false, [&](const NPDisk::TLogRecord& rec) {
                if (rec.Data.size() == expectedLogData.size()) {
                    const TString data = TRcBuf(rec.Data).ExtractUnderlyingContainerOrCopy<TString>();
                    if (data == expectedLogData) {
                        ++foundExpectedCount;
                    }
                }
            });
            UNIT_ASSERT(logRecordsRead > 0);
            UNIT_ASSERT_VALUES_EQUAL(foundExpectedCount, expectedCount);

            // sanity check read chunkdata
            for (ui32 chunkIdx = 0; chunkIdx < chunkIds.size(); ++chunkIdx) {
                const auto readRes = testCtx.TestResponse<NPDisk::TEvChunkReadResult>(
                    new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                        chunkIds[chunkIdx], 0, writeData.size(), 0, nullptr),
                    std::nullopt);

                if (readRes->Status == NKikimrProto::OK) {
                    UNIT_ASSERT(readRes->Data.IsReadable(0, writeData.size()));
                    UNIT_ASSERT_VALUES_EQUAL(writeData, readRes->Data.ToString());
                }
            }
        };

        auto readMetadata = [&]() -> TRcBuf {
            testCtx.Send(new NPDisk::TEvReadMetadata());
            auto res = testCtx.Recv<NPDisk::TEvReadMetadataResult>();
            UNIT_ASSERT_VALUES_EQUAL(res->Outcome, NPDisk::EPDiskMetadataOutcome::OK);
            return std::move(res->Metadata);
        };

        auto doWritesAndCheck = [&]() {
            TVDiskMock vdisk(&testCtx, true);
            vdisk.InitFull();
            vdisk.SendEvLogSync(12346);
            expectedLogCount = 1;

            vdisk.ReserveChunk();
            vdisk.CommitReservedChunks();

            UNIT_ASSERT_VALUES_EQUAL(vdisk.Chunks[EChunkState::COMMITTED].size(), 1UL);
            chunkIds.assign(vdisk.Chunks[EChunkState::COMMITTED].begin(), vdisk.Chunks[EChunkState::COMMITTED].end());
            vdisk.SendEvLogSync();

            auto lastChunkId = chunkIds.back();

            testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(
                    new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                        lastChunkId, 0, new NPDisk::TEvChunkWrite::TAlignedParts(TString(writeData)), nullptr, false, 0),
                    NKikimrProto::OK);

            verifyVdiskData(vdisk, expectedLogCount);

            if (hasMetadata) {
                const TString metadata = TRcBuf(readMetadata()).ExtractUnderlyingContainerOrCopy<TString>();
                UNIT_ASSERT_VALUES_EQUAL(metadata, lastMetadata);
            }

            const TString newMetadata = TStringBuilder() << "metadata-" << metadataSeq++;
            testCtx.Send(new NPDisk::TEvWriteMetadata(TRcBuf(newMetadata)));
            auto writeRes = testCtx.Recv<NPDisk::TEvWriteMetadataResult>();
            UNIT_ASSERT_VALUES_EQUAL(writeRes->Outcome, NPDisk::EPDiskMetadataOutcome::OK);

            const TString readBack = TRcBuf(readMetadata()).ExtractUnderlyingContainerOrCopy<TString>();
            UNIT_ASSERT_VALUES_EQUAL(readBack, newMetadata);
            lastMetadata = newMetadata;
            hasMetadata = true;
        };

        auto restartPDisk = [&](bool enableSectorEncryption) {
            auto cfg = testCtx.GetPDiskConfig();
            cfg->FeatureFlags.SetEnablePDiskDataEncryption(enableSectorEncryption);
            testCtx.RestartPDiskSync();
        };

        // encryption is on
        doWritesAndCheck();

        // off
        restartPDisk(false);
        doWritesAndCheck();

        // on
        restartPDisk(true);
        doWritesAndCheck();

        // off
        restartPDisk(false);
        doWritesAndCheck();

        // on
        restartPDisk(true);
        doWritesAndCheck();
    }

    Y_UNIT_TEST(SectorEncryptionOnOffEncryptedInitially) {
        ShouldSwitchEncryptionOnOffWithoutReformatting(true);
    }

    Y_UNIT_TEST(SectorEncryptionOnOffNonEncryptedInitially) {
        ShouldSwitchEncryptionOnOffWithoutReformatting(false);
    }

    Y_UNIT_TEST(PDiskOwnerSlayRace) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);
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
    }

    Y_UNIT_TEST(PDiskRestartManyLogWrites) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);

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
    }

    Y_UNIT_TEST(CommitDeleteChunks) {
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);
            TVDiskMock intensiveVDisk(&testCtx);
            intensiveVDisk.InitFull();
            intensiveVDisk.ReserveChunk();
            intensiveVDisk.ReserveChunk();
            intensiveVDisk.CommitReservedChunks();
            intensiveVDisk.SendEvLogSync();
            intensiveVDisk.DeleteCommitedChunks();
            intensiveVDisk.InitFull();
        }
    }

    // Test to reproduce bug from
    Y_UNIT_TEST(TestLogSpliceChunkReserve) {
        for (bool encryption: {true, false}) {
            TActorTestContext testCtx({
                .IsBad = false,
                .DiskSize = 1ull << 30,
                .ChunkSize = 1ull * (1 << 20),
                .SmallDisk = true,
                .EnableFormatAndMetadataEncryption = encryption,
                .EnableSectorEncryption = encryption,
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
    }

    Y_UNIT_TEST(SpaceColor) {
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
            auto colorName = NKikimrBlobStorage::TPDiskSpaceColor::E_Name(color);
            Cerr << (TStringBuilder() << "- Testing " << colorName << Endl);

            auto pdiskConfig = testCtx.GetPDiskConfig();
            pdiskConfig->SpaceColorBorder = color;
            pdiskConfig->ExpectedSlotCount = 10;
            testCtx.UpdateConfigRecreatePDisk(pdiskConfig);

            vdisk.InitFull();
            auto initialSpace = testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
                    new NPDisk::TEvCheckSpace(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound),
                    NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(initialSpace->VDiskRawUsage, 0.);
            UNIT_ASSERT_VALUES_EQUAL(initialSpace->PDiskUsage, 0.);

            for (ui32 i = 0; i < initialSpace->TotalChunks + 1; ++i) {
                vdisk.ReserveChunk();
            }
            vdisk.CommitReservedChunks();

            auto resultSpace = testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
                    new NPDisk::TEvCheckSpace(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound),
                    NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(color, StatusFlagToSpaceColor(resultSpace->StatusFlags));
            UNIT_ASSERT_GT(resultSpace->VDiskRawUsage, 100.);
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

        const ui32 firstNodeId = testCtx.GetRuntime()->GetFirstNodeId();
        testCtx.GetRuntime()->RegisterService(MakeBlobStorageNodeWardenID(firstNodeId), nodeWardenFake);

        testCtx.GetRuntime()->WaitFor("TEvControllerUpdateDiskStatus", [&received]() {
            return received;
        });
    }

    Y_UNIT_TEST(PDiskIncreaseLogChunksLimitAfterRestart) {
        for (bool encryption: {true, false}) {
            TActorTestContext testCtx({
                .IsBad=false,
                .DiskSize = 1_GB,
                .ChunkSize = 1_MB,
                .EnableFormatAndMetadataEncryption = encryption,
                .EnableSectorEncryption = encryption,
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
            Cerr << (TStringBuilder() << "Got EvPDiskStateUpdate# " << evPDiskStateUpdate->ToString() << Endl);
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
        TActorTestContext testCtx({});
        const ui32 firstNodeId = testCtx.GetRuntime()->GetFirstNodeId();

        // Setup receiving whiteboard state updates
        testCtx.GetRuntime()->SetDispatchTimeout(10 * TDuration::MilliSeconds(testCtx.GetPDiskConfig()->StatisticsUpdateIntervalMs));
        testCtx.GetRuntime()->RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(firstNodeId), testCtx.Sender);
        AwaitAndCheckEvPDiskStateUpdate(testCtx, 0u, 0);

        // Setup 2 vdisks
        TVDiskMock vdisk1(&testCtx);
        TVDiskMock vdisk2(&testCtx);

        vdisk1.InitFull(0u);
        vdisk2.InitFull(4u);

        vdisk1.SendEvLogSync();
        vdisk2.SendEvLogSync();

        vdisk1.ReserveChunk();
        vdisk2.ReserveChunk();

        vdisk1.CommitReservedChunks();
        vdisk2.CommitReservedChunks();

        // State 1:
        // PDisk.SlotSizeUnits: 0
        // Owners.GroupSizeInUnits: [0, 4]
        Cerr << (TStringBuilder() << "- State 1" << Endl);

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

        // State 2:
        // PDisk.SlotSizeUnits: 0
        // Owners.GroupSizeInUnits: [2, 4]
        Cerr << (TStringBuilder() << "- State 2" << Endl);

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

        // State 3:
        // PDisk.SlotSizeUnits: 2
        // Owners.GroupSizeInUnits: [2, 4, 2]
        Cerr << (TStringBuilder() << "- State 3" << Endl);

        // Assert NumActiveSlots == 4
        const auto evCheckSpaceResponse4 = testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
            new NPDisk::TEvCheckSpace(vdisk3.PDiskParams->Owner, vdisk3.PDiskParams->OwnerRound),
            NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(evCheckSpaceResponse4->NumActiveSlots, 4);
        AwaitAndCheckEvPDiskStateUpdate(testCtx, 2u, 4);
    }

    THolder<NPDisk::TEvCheckSpaceResult> CheckEvCheckSpace(
        TActorTestContext& testCtx,
        const TVDiskMock& vdisk,
        ui32 expectedFreeChunks,
        ui32 expectedTotalChunks,
        ui32 expectedUsedChunks,
        double expectedNormalizedOccupancy,
        double expectedVDiskSlotUsage,
        double expectedPDiskUsage,
        ui32 expectedNumSlots,
        ui32 expectedNumActiveSlots,
        NKikimrBlobStorage::TPDiskSpaceColor::E expectedColor
    ) {
        UNIT_ASSERT_GT(expectedTotalChunks, 0);
        double expectedVDiskRawUsage = 100. * ((double)expectedUsedChunks) / expectedTotalChunks;

        Cerr << (TStringBuilder() << "... Checking EvCheckSpace"
            << " VDisk# " << vdisk.VDiskID
            << " FreeChunks# " << expectedFreeChunks
            << " TotalChunks# " << expectedTotalChunks
            << " UsedChunks# " << expectedUsedChunks
            << " NormalizedOccupancy# " << expectedNormalizedOccupancy
            << " VDiskSlotUsage# " << expectedVDiskSlotUsage
            << " VDiskRawUsage# " << expectedVDiskRawUsage
            << " PDiskUsage# " << expectedPDiskUsage
            << " NumSlots# " << expectedNumSlots
            << " NumActiveSlots# " << expectedNumActiveSlots
            << " Color# " << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(expectedColor)
            << Endl);
        auto evCheckSpaceResult = testCtx.TestResponse<NPDisk::TEvCheckSpaceResult>(
            new NPDisk::TEvCheckSpace(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound),
            NKikimrProto::OK);
        Cerr << (TStringBuilder() << "Got " << evCheckSpaceResult->ToString()
            << " NormalizedOccupancy# " << evCheckSpaceResult->NormalizedOccupancy
            << " VDiskSlotUsage# " << evCheckSpaceResult->VDiskSlotUsage
            << " VDiskRawUsage# " << evCheckSpaceResult->VDiskRawUsage
            << " PDiskUsage# " << evCheckSpaceResult->PDiskUsage
            << Endl);
        UNIT_ASSERT_VALUES_EQUAL(evCheckSpaceResult->FreeChunks, expectedFreeChunks);
        UNIT_ASSERT_VALUES_EQUAL(evCheckSpaceResult->TotalChunks, expectedTotalChunks);
        UNIT_ASSERT_VALUES_EQUAL(evCheckSpaceResult->UsedChunks, expectedUsedChunks);
        UNIT_ASSERT_VALUES_EQUAL(evCheckSpaceResult->NumSlots, expectedNumSlots);
        UNIT_ASSERT_VALUES_EQUAL(evCheckSpaceResult->NumActiveSlots, expectedNumActiveSlots);
        UNIT_ASSERT_VALUES_EQUAL(StatusFlagToSpaceColor(evCheckSpaceResult->StatusFlags), expectedColor);

        UNIT_ASSERT_DOUBLES_EQUAL(evCheckSpaceResult->NormalizedOccupancy, expectedNormalizedOccupancy, 0.01);
        UNIT_ASSERT_DOUBLES_EQUAL(evCheckSpaceResult->VDiskSlotUsage, expectedVDiskSlotUsage, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(evCheckSpaceResult->PDiskUsage, expectedPDiskUsage, 0.1);

        UNIT_ASSERT_DOUBLES_EQUAL(evCheckSpaceResult->VDiskRawUsage, expectedVDiskRawUsage, 0.1);

        return evCheckSpaceResult;
    };

    Y_UNIT_TEST(ChangeExpectedSlotCount) {
        TActorTestContext testCtx({});
        const ui32 firstNodeId = testCtx.GetRuntime()->GetFirstNodeId();
        auto pdiskConfig = testCtx.GetPDiskConfig();
        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        pdiskConfig->SpaceColorBorder = TColor::ORANGE;
        testCtx.UpdateConfigRecreatePDisk(pdiskConfig);
        // The actual value of SharedQuota.HardLimit is initialized in TActorTestContext
        // with quite a complex formula. The value used here was obtained experimentally.
        // Feel free to update if some day it changes
        const ui32 sharedQuota = 778;

        // Setup receiving whiteboard state updates
        testCtx.GetRuntime()->SetDispatchTimeout(10 * TDuration::MilliSeconds(testCtx.GetPDiskConfig()->StatisticsUpdateIntervalMs));
        testCtx.GetRuntime()->RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(firstNodeId), testCtx.Sender);

        // State 1:
        // PDisk.ExpectedSlotCount: 0
        // Owners.GroupSizeInUnits: [0u, 2u]
        // Owners.Weight: [1, 2]
        Cerr << (TStringBuilder() << "- State 1" << Endl);

        TVDiskMock vdisk0(&testCtx);
        vdisk0.InitFull(1u);
        vdisk0.SendEvLogSync();
        CheckEvCheckSpace(testCtx, vdisk0, sharedQuota, sharedQuota, 0, 0.0, 0.0, 0.0, 1, 1, TColor::GREEN);

        TVDiskMock vdisk1(&testCtx);
        vdisk1.InitFull(2u);
        vdisk1.SendEvLogSync();
        CheckEvCheckSpace(testCtx, vdisk0, sharedQuota, sharedQuota/3*1, 0, 0.0, 0.0, 0.0, 2, 3, TColor::GREEN);
        CheckEvCheckSpace(testCtx, vdisk1, sharedQuota, sharedQuota/3*2, 0, 0.0, 0.0, 0.0, 2, 3, TColor::GREEN);

        // State 2:
        // PDisk.ExpectedSlotCount: 4
        Cerr << (TStringBuilder() << "- State 2" << Endl);

        testCtx.TestResponse<NPDisk::TEvChangeExpectedSlotCountResult>(
            new NPDisk::TEvChangeExpectedSlotCount(4, 0u),
            NKikimrProto::OK);
        pdiskConfig = testCtx.GetPDiskConfig();
        UNIT_ASSERT_VALUES_EQUAL(pdiskConfig->ExpectedSlotCount, 4);
        UNIT_ASSERT_VALUES_EQUAL(pdiskConfig->SlotSizeInUnits, 0u);
        AwaitAndCheckEvPDiskStateUpdate(testCtx, 0u, 3);

        ui32 fairQuota = sharedQuota / 4;
        UNIT_ASSERT_VALUES_EQUAL(fairQuota, 194);
        CheckEvCheckSpace(testCtx, vdisk0, sharedQuota, fairQuota, 0, 0.0, 0.0, 0.0, 2, 3, TColor::GREEN);
        CheckEvCheckSpace(testCtx, vdisk1, sharedQuota, fairQuota*2, 0, 0.0, 0.0, 0.0, 2, 3, TColor::GREEN);

        // State 3:
        // vdisk0 consumes all it's fair quota (1/4 pdisk)
        Cerr << (TStringBuilder() << "- State 3" << Endl);

        ui32 vdisk0Used = fairQuota;
        ui32 sharedFree = sharedQuota - vdisk0Used;
        UNIT_ASSERT_VALUES_EQUAL(vdisk0Used, 194);
        UNIT_ASSERT_VALUES_EQUAL(sharedFree, 584);
        for (ui32 i = 0; i < vdisk0Used; ++i) {
            vdisk0.ReserveChunk();
        }
        vdisk0.CommitReservedChunks();

        ui32 vdisk0LightYellowLimit = 168;
        double vdisk0SlotUtilization = 100. * ((double)vdisk0Used) / vdisk0LightYellowLimit;
        UNIT_ASSERT_DOUBLES_EQUAL(vdisk0SlotUtilization, 115.5, 0.1);
        CheckEvCheckSpace(testCtx, vdisk0, sharedFree, fairQuota, vdisk0Used, 0.97, vdisk0SlotUtilization, 25.0, 2, 3, TColor::ORANGE);
        CheckEvCheckSpace(testCtx, vdisk1, sharedFree, fairQuota*2, 0, 0.25, 0.0, 25.0, 2, 3, TColor::GREEN);

        // State 4:
        // PDisk.ExpectedSlotCount: 2
        // PDisk.SlotSizeInUnits: 2u
        // Owners.GroupSizeInUnits: [0u, 2u]
        // Owners.Weight: [1, 1]
        Cerr << (TStringBuilder() << "- State 4" << Endl);

        testCtx.TestResponse<NPDisk::TEvChangeExpectedSlotCountResult>(
            new NPDisk::TEvChangeExpectedSlotCount(2, 2u),
            NKikimrProto::OK);
        pdiskConfig = testCtx.GetPDiskConfig();
        UNIT_ASSERT_VALUES_EQUAL(pdiskConfig->ExpectedSlotCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(pdiskConfig->SlotSizeInUnits, 2u);
        AwaitAndCheckEvPDiskStateUpdate(testCtx, 2u, 2);

        fairQuota = sharedQuota / 2;
        UNIT_ASSERT_VALUES_EQUAL(fairQuota, 389);
        UNIT_ASSERT_VALUES_EQUAL(vdisk0Used, 194);
        UNIT_ASSERT_VALUES_EQUAL(vdisk0Used, fairQuota/2);
        vdisk0LightYellowLimit = 344;
        vdisk0SlotUtilization = 100. * ((double)vdisk0Used) / vdisk0LightYellowLimit;
        UNIT_ASSERT_DOUBLES_EQUAL(vdisk0SlotUtilization, 56.4, 0.1);
        CheckEvCheckSpace(testCtx, vdisk0, sharedFree, fairQuota, vdisk0Used, 0.5, vdisk0SlotUtilization, 25.0, 2, 2, TColor::GREEN);
        CheckEvCheckSpace(testCtx, vdisk1, sharedFree, fairQuota, 0, 0.25, 0.0, 25.0, 2, 2, TColor::GREEN);

        // State 5:
        // Owners.GroupSizeInUnits: [0u, 2u, 4u]
        // Owners.Weight: [1, 1, 2]
        Cerr << (TStringBuilder() << "- State 5" << Endl);

        TVDiskMock vdisk2(&testCtx);
        vdisk2.InitFull(4u);
        vdisk2.SendEvLogSync();

        fairQuota = sharedQuota / 4;
        UNIT_ASSERT_VALUES_EQUAL(fairQuota, 194);
        UNIT_ASSERT_VALUES_EQUAL(vdisk0Used, fairQuota);
        vdisk0LightYellowLimit = 168;
        vdisk0SlotUtilization = 100. * ((double)vdisk0Used) / vdisk0LightYellowLimit;
        UNIT_ASSERT_DOUBLES_EQUAL(vdisk0SlotUtilization, 115.5, 0.1);
        CheckEvCheckSpace(testCtx, vdisk0, sharedFree, fairQuota, vdisk0Used, 0.97, vdisk0SlotUtilization, 25.0, 3, 4, TColor::ORANGE);
        CheckEvCheckSpace(testCtx, vdisk1, sharedFree, fairQuota, 0, 0.25, 0.0, 25.0, 3, 4, TColor::GREEN);
        CheckEvCheckSpace(testCtx, vdisk2, sharedFree, fairQuota*2, 0, 0.25, 0.0, 25.0, 3, 4, TColor::GREEN);

        auto &icb = testCtx.GetRuntime()->GetAppData().Icb;
        TControlWrapper semiStrictSpaceIsolation(0, 0, 2);
        TControlBoard::RegisterSharedControl(semiStrictSpaceIsolation, icb->PDiskControls.SemiStrictSpaceIsolation);
        semiStrictSpaceIsolation = 1;
        CheckEvCheckSpace(testCtx, vdisk0, sharedFree, fairQuota, vdisk0Used, 0.90, vdisk0SlotUtilization, 25.0, 3, 4, TColor::LIGHT_YELLOW);
        semiStrictSpaceIsolation = 2;
        CheckEvCheckSpace(testCtx, vdisk0, sharedFree, fairQuota, vdisk0Used, 0.92, vdisk0SlotUtilization, 25.0, 3, 4, TColor::YELLOW);
        semiStrictSpaceIsolation = 0;

        // State 6:
        // Owners.GroupSizeInUnits: [0u, 2u, 1u]
        // Owners.Weight: [1, 1, 1]
        Cerr << (TStringBuilder() << "- State 6" << Endl);

        testCtx.TestResponse<NPDisk::TEvYardResizeResult>(
            new NPDisk::TEvYardResize(vdisk2.PDiskParams->Owner, vdisk2.PDiskParams->OwnerRound, 1u),
            NKikimrProto::OK);
        AwaitAndCheckEvPDiskStateUpdate(testCtx, 2u, 3);

        fairQuota = sharedQuota / 3;
        UNIT_ASSERT_VALUES_EQUAL(fairQuota, 259);
        vdisk0LightYellowLimit = 227;
        vdisk0SlotUtilization = 100. * ((double)vdisk0Used) / vdisk0LightYellowLimit;
        double vdisk0FairOccupancy = ((double)vdisk0Used) / fairQuota;
        UNIT_ASSERT_DOUBLES_EQUAL(vdisk0FairOccupancy, 0.749, 0.001);
        UNIT_ASSERT_DOUBLES_EQUAL(vdisk0SlotUtilization, 85.4, 0.1);
        CheckEvCheckSpace(testCtx, vdisk0, sharedFree, fairQuota, vdisk0Used, vdisk0FairOccupancy, vdisk0SlotUtilization, 25.0, 3, 3, TColor::GREEN);
        CheckEvCheckSpace(testCtx, vdisk1, sharedFree, fairQuota, 0, 0.25, 0.0, 25.0, 3, 3, TColor::GREEN);
        CheckEvCheckSpace(testCtx, vdisk2, sharedFree, fairQuota, 0, 0.25, 0.0, 25.0, 3, 3, TColor::GREEN);

        // State 7:
        // PDisk.ExpectedSlotCount: 8
        // PDisk.SlotSizeInUnits: 1u
        // Owners.GroupSizeInUnits: [0u, 2u, 1u]
        // Owners.Weight: [1, 2, 1]
        Cerr << (TStringBuilder() << "- State 7" << Endl);

        testCtx.TestResponse<NPDisk::TEvChangeExpectedSlotCountResult>(
            new NPDisk::TEvChangeExpectedSlotCount(8, 1u),
            NKikimrProto::OK);
        pdiskConfig = testCtx.GetPDiskConfig();
        UNIT_ASSERT_VALUES_EQUAL(pdiskConfig->ExpectedSlotCount, 8);
        UNIT_ASSERT_VALUES_EQUAL(pdiskConfig->SlotSizeInUnits, 1u);
        AwaitAndCheckEvPDiskStateUpdate(testCtx, 1u, 4);

        fairQuota = sharedQuota / 8;
        UNIT_ASSERT_VALUES_EQUAL(fairQuota, 97);
        UNIT_ASSERT_VALUES_EQUAL(vdisk0Used, 194);
        UNIT_ASSERT_VALUES_EQUAL(vdisk0Used, fairQuota*2);
        vdisk0LightYellowLimit = 81;
        vdisk0SlotUtilization = 100. * ((double)vdisk0Used) / vdisk0LightYellowLimit;
        UNIT_ASSERT_DOUBLES_EQUAL(vdisk0FairOccupancy, 0.749, 0.001);
        UNIT_ASSERT_DOUBLES_EQUAL(vdisk0SlotUtilization, 239.5, 0.1);
        CheckEvCheckSpace(testCtx, vdisk0, sharedFree, fairQuota, vdisk0Used, 0.965, vdisk0SlotUtilization, 25.0, 3, 4, TColor::ORANGE);
        CheckEvCheckSpace(testCtx, vdisk1, sharedFree, fairQuota*2, 0, 0.25, 0.0, 25.0, 3, 4, TColor::GREEN);
        CheckEvCheckSpace(testCtx, vdisk2, sharedFree, fairQuota, 0, 0.25, 0.0, 25.0, 3, 4, TColor::GREEN);

        // State 8:
        // vdisk1 makes the whole PDisk Red
        Cerr << (TStringBuilder() << "- State 8" << Endl);

        ui32 vdisk1Used = sharedFree - 3;
        UNIT_ASSERT_VALUES_EQUAL(vdisk1Used, 581);
        sharedFree = 3;
        for (ui32 i = 0; i < vdisk1Used; ++i) {
            vdisk1.ReserveChunk();
        }
        vdisk1.CommitReservedChunks();

        ui32 vdisk1LightYellowLimit = 168;
        UNIT_ASSERT_GE(vdisk1LightYellowLimit, vdisk0LightYellowLimit*2);
        double vdisk1SlotUtilization = 100. * ((double)vdisk1Used) / vdisk1LightYellowLimit;
        UNIT_ASSERT_DOUBLES_EQUAL(vdisk1SlotUtilization, 345.8, 0.1);

        UNIT_ASSERT_VALUES_EQUAL(vdisk0Used, fairQuota*2);
        UNIT_ASSERT_VALUES_EQUAL(vdisk1Used, fairQuota*6-1);
        UNIT_ASSERT_VALUES_EQUAL(vdisk0Used + vdisk1Used, sharedQuota - sharedFree);
        CheckEvCheckSpace(testCtx, vdisk0, sharedFree, fairQuota, vdisk0Used, 0.99, vdisk0SlotUtilization, 99.6, 3, 4, TColor::RED);
        CheckEvCheckSpace(testCtx, vdisk1, sharedFree, fairQuota*2, vdisk1Used, 0.99, vdisk1SlotUtilization, 99.6, 3, 4, TColor::RED);
        CheckEvCheckSpace(testCtx, vdisk2, sharedFree, fairQuota, 0, 0.99, 0.0, 99.6, 3, 4, TColor::RED);
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
        TActorTestContext::TSettings settings{};
        settings.IsBad = false;
        TActorTestContext testCtx(settings);
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

    void ChunkWriteDifferentOffsetAndSizeImpl(bool plainDataChunks, bool rdmaAlloc) {
        TActorTestContext testCtx({
            .PlainDataChunks = plainDataChunks,
            .UseRdmaAllocator = rdmaAlloc,
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
        for (ui32 i = 0; i <= 1; ++i) {
            ChunkWriteDifferentOffsetAndSizeImpl(i & 1, false);
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
        for (bool encryption: {true, false}) {
            TActorTestContext::TSettings settings{};
            settings.EnableFormatAndMetadataEncryption = encryption;
            settings.EnableSectorEncryption = encryption;
            TActorTestContext testCtx(settings);

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

    std::map<ui8, double> GetChunkOperationPriorities(bool read, const std::vector<ui8>& priorities) {
        TActorTestContext testCtx{{}};
        auto ioDuration = TDuration::MilliSeconds(5);
        Cerr << "ioDuration# " << ioDuration << Endl;
        testCtx.TestCtx.SectorMap->ImitateRandomWait = {ioDuration, ioDuration};

        auto cfg = testCtx.GetPDiskConfig();
        cfg->SeparateHugePriorities = true;
        testCtx.UpdateConfigRecreatePDisk(cfg);

        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();

        size_t chunksToReserve = priorities.size();
        for (size_t i = 0; i < chunksToReserve; ++i) {
            vdisk.ReserveChunk();
        }

        vdisk.CommitReservedChunks();
        UNIT_ASSERT(vdisk.Chunks[EChunkState::COMMITTED].size() == chunksToReserve);
        std::vector<TChunkIdx> chunks;
        for (const auto& chunk : vdisk.Chunks[EChunkState::COMMITTED]) {
            chunks.push_back(chunk);
        }

        auto seed = TInstant::Now().MicroSeconds();
        Cerr << "seed# " << seed << Endl;
        TReallyFastRng32 rng(seed);
        size_t size = 128_KB;
        size = size / vdisk.PDiskParams->AppendBlockSize * vdisk.PDiskParams->AppendBlockSize;

        TSimpleTimer t;
        const size_t eventsToSend = 1000 * priorities.size();
        const size_t eventsToWait = eventsToSend / 5;
        NPDisk::TEvChunkWrite::TPartsPtr parts = GenParts(rng, size);


        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
            new NPDisk::TEvYardControl(NPDisk::TEvYardControl::ActionPause, nullptr),
            NKikimrProto::OK);

        for (ui64 p = 0; p < priorities.size(); ++p) {
            for (size_t i = 0; i < eventsToSend; ++i) {
                if (read) {
                    testCtx.Send(new NPDisk::TEvChunkRead(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                            chunks[p], 0, size, priorities[p], (void*)p));
                } else {
                    testCtx.Send(new NPDisk::TEvChunkWrite(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                                chunks[p], 0, parts, (void*)p, false, priorities[p]));
                }
            }
        }
        Cout << (read ? "read" : "write") << " load. Time spent to send " << eventsToSend << " events# " << t.Get() << Endl;
        testCtx.TestResponse<NPDisk::TEvYardControlResult>(
            new NPDisk::TEvYardControl(NPDisk::TEvYardControl::ActionResume, nullptr),
            NKikimrProto::OK);

        std::vector<size_t> results(priorities.size(), 0);
        for (size_t i = 0; i < eventsToWait; ++i) {
            size_t cookie;
            if (read) {
                auto result = testCtx.Recv<NPDisk::TEvChunkReadResult>();
                cookie = (size_t)result->Cookie;
            } else {
                auto result = testCtx.Recv<NPDisk::TEvChunkWriteResult>();
                cookie = (size_t)result->Cookie;
            }
            results[cookie] += size;
        }
        auto max_v = *std::max_element(results.begin(), results.end());
        Cout << "bytes_max# " << max_v << " weights: " << Endl;
        std::map<ui8, double> shares;
        for (size_t i = 0; i < results.size(); ++i) {
            auto x = (double)results[i] / max_v;
            shares[priorities[i]] = x;
            Cout << "    " << Sprintf("%.3f", x) << " " << PriToString(priorities[i]) << Endl;
        }
        Cout << Endl;
        return shares;
    }

    Y_UNIT_TEST(CheckChunkReadOperationPriorities) {
        using namespace NPriRead;
        std::vector<ui8> PriRead = {SyncLog, HullComp, HullOnlineRt, HullOnlineOther, HullLoad, HullLow};
        auto shares = GetChunkOperationPriorities(true, PriRead);

        // compare with 10% tolerance
        UNIT_ASSERT(shares[SyncLog] * 0.90 > shares[HullLoad]);
        UNIT_ASSERT(shares[HullLoad] * 0.90 > shares[HullOnlineRt]);
        UNIT_ASSERT(shares[HullOnlineRt] * 0.90 > shares[HullComp]);
        // test equality with 20% tolerance
        UNIT_ASSERT(shares[HullOnlineRt] * 0.80 < shares[HullOnlineOther] &&
                    shares[HullOnlineOther] * 0.80 < shares[HullOnlineRt]);
        UNIT_ASSERT(shares[HullComp] * 0.90 > shares[HullLow]);
    }

    Y_UNIT_TEST(CheckChunkWriteOperationPriorities) {
        using namespace NPriWrite;
        std::vector<ui8> PriWrite = {SyncLog, HullFresh, HullHugeAsyncBlob, HullHugeUserData, HullComp};
        auto shares = GetChunkOperationPriorities(false, PriWrite);

        // compare with 10% tolerance
        UNIT_ASSERT(shares[SyncLog] * 0.90 > shares[HullHugeUserData]);
        UNIT_ASSERT(shares[HullHugeUserData] * 0.90 > shares[HullComp]);
        // test equality with 20% tolerance
        UNIT_ASSERT(shares[HullHugeUserData] * 0.80 < shares[HullHugeAsyncBlob] &&
                    shares[HullHugeAsyncBlob] * 0.80 < shares[HullHugeUserData]);
        UNIT_ASSERT(shares[HullComp] * 0.90 > shares[HullFresh]);
    }

    Y_UNIT_TEST(CompactionWriteShouldNotAffectRtWriteLatency) {
        using namespace NPriWrite;

        TActorTestContext testCtx{{}};
        auto ioDuration = TDuration::MilliSeconds(5);
        testCtx.TestCtx.SectorMap->ImitateRandomWait = {ioDuration, TDuration::MicroSeconds(1)};

        auto cfg = testCtx.GetPDiskConfig();
        cfg->SeparateHugePriorities = true;
        cfg->UseBytesFlightControl = true;
        testCtx.UpdateConfigRecreatePDisk(cfg);

        TVDiskMock compVDisk(&testCtx);
        compVDisk.InitFull();
        TVDiskMock rtVDisk(&testCtx);
        rtVDisk.InitFull();

        constexpr ui32 compChunkCount = 32;
        constexpr ui32 rtChunkCount = 8;
        compVDisk.ReserveChunk(compChunkCount);
        compVDisk.CommitReservedChunks();
        rtVDisk.ReserveChunk(rtChunkCount);
        rtVDisk.CommitReservedChunks();

        TVector<TChunkIdx> compChunks(compVDisk.Chunks[EChunkState::COMMITTED].begin(),
                compVDisk.Chunks[EChunkState::COMMITTED].end());
        TVector<TChunkIdx> rtChunks(rtVDisk.Chunks[EChunkState::COMMITTED].begin(),
                rtVDisk.Chunks[EChunkState::COMMITTED].end());

        UNIT_ASSERT_VALUES_EQUAL(compChunks.size(), compChunkCount);
        UNIT_ASSERT_VALUES_EQUAL(rtChunks.size(), rtChunkCount);

        auto alignToAppendBlock = [] (size_t size, ui32 appendBlockSize) {
            return (size + appendBlockSize - 1) / appendBlockSize * appendBlockSize;
        };
        const size_t compWriteSize = alignToAppendBlock(2_MB, compVDisk.PDiskParams->AppendBlockSize);
        const size_t rtWriteSize = alignToAppendBlock(500_KB, rtVDisk.PDiskParams->AppendBlockSize);

        auto seed = TInstant::Now().MicroSeconds();
        Cerr << "seed# " << seed << Endl;
        TReallyFastRng32 rng(seed);
        auto compParts = GenParts(rng, compWriteSize);
        auto rtParts = GenParts(rng, rtWriteSize);

        constexpr ui32 compInflight = 10;
        constexpr ui32 compTotalWrites = 1000;
        constexpr ui32 compWritesPerRtWrite = 20;
        const ui32 rtTotalWrites = Max<ui32>(1, compTotalWrites / compWritesPerRtWrite);

        void* compCookie = reinterpret_cast<void*>(1);
        void* rtCookie = reinterpret_cast<void*>(2);

        // statistics section
        ui32 compSent = 0;
        ui32 compDone = 0;
        ui32 rtSent = 0;
        ui32 rtDone = 0;
        ui32 nextRtAtCompDone = 0;
        std::optional<TInstant> rtWriteStartedAt;
        TVector<double> rtLatenciesMs;
        ui64 compBytesWritten = 0;
        ui64 rtBytesWritten = 0;
        TSimpleTimer totalWriteTimer;

        auto sendCompWrite = [&] {
            const TChunkIdx chunk = compChunks[compSent % compChunks.size()];
            testCtx.Send(new NPDisk::TEvChunkWrite(
                    compVDisk.PDiskParams->Owner, compVDisk.PDiskParams->OwnerRound,
                    chunk, 0, compParts, compCookie, false, HullComp));
            ++compSent;
        };

        auto sendRtWrite = [&] {
            const TChunkIdx chunk = rtChunks[rtSent % rtChunks.size()];
            rtWriteStartedAt = TInstant::Now();
            // Intentional: write request with NPriRead::HullOnlineRt to reproduce production traffic tagging.
            testCtx.Send(new NPDisk::TEvChunkWrite(
                    rtVDisk.PDiskParams->Owner, rtVDisk.PDiskParams->OwnerRound,
                    chunk, 0, rtParts, rtCookie, false, NPriRead::HullOnlineRt));
            ++rtSent;
        };

        auto trySendRtWrite = [&] {
            if (!rtWriteStartedAt && rtSent < rtTotalWrites && compDone >= nextRtAtCompDone) {
                sendRtWrite();
                nextRtAtCompDone += compWritesPerRtWrite;
            }
        };

        for (ui32 i = 0; i < compInflight && compSent < compTotalWrites; ++i) {
            sendCompWrite();
        }
        trySendRtWrite();

        while (compDone < compTotalWrites || rtDone < rtTotalWrites) {
            auto result = testCtx.Recv<NPDisk::TEvChunkWriteResult>();
            UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);

            if (result->Cookie == rtCookie) {
                UNIT_ASSERT(rtWriteStartedAt);
                const auto latency = TInstant::Now() - *rtWriteStartedAt;
                rtLatenciesMs.push_back((double)latency.MicroSeconds() / 1000.0);
                rtWriteStartedAt.reset();
                ++rtDone;
                rtBytesWritten += rtWriteSize;
            } else {
                UNIT_ASSERT_VALUES_EQUAL(result->Cookie, compCookie);
                ++compDone;
                compBytesWritten += compWriteSize;
                if (compSent < compTotalWrites) {
                    sendCompWrite();
                }
            }

            trySendRtWrite();
        }

        UNIT_ASSERT(!rtLatenciesMs.empty());
        UNIT_ASSERT_VALUES_EQUAL(rtDone, rtTotalWrites);

        double minLatencyMs = rtLatenciesMs.front();
        double maxLatencyMs = rtLatenciesMs.front();
        double avgLatencyMs = 0;
        for (double latencyMs : rtLatenciesMs) {
            minLatencyMs = Min(minLatencyMs, latencyMs);
            maxLatencyMs = Max(maxLatencyMs, latencyMs);
            avgLatencyMs += latencyMs;
        }
        avgLatencyMs /= rtLatenciesMs.size();

        Cout << "HullOnlineRt chunk write latency on vdisk2, ms:"
             << " min# " << Sprintf("%.3f", minLatencyMs)
             << " avg# " << Sprintf("%.3f", avgLatencyMs)
             << " max# " << Sprintf("%.3f", maxLatencyMs) << Endl;
        Cout << "HullOnlineRt chunk write latency samples on vdisk2, ms:";
        for (double latencyMs : rtLatenciesMs) {
            Cout << " " << Sprintf("%.3f", latencyMs);
        }
        Cout << Endl;

        const double durationSec = Max(totalWriteTimer.Get().SecondsFloat(), 1e-9);
        constexpr double bytesInMB = 1'000'000.0;
        constexpr double bytesInGB = 1'000'000'000.0;
        Cout << "Chunk write throughput:" << " duration_sec# " << Sprintf("%.3f", durationSec) << Endl
             << " comp_avg_speed# " << Sprintf("%.3f", (double)compBytesWritten / durationSec / bytesInMB) << "MB/s"
             << " rt_avg_speed# " << Sprintf("%.3f", (double)rtBytesWritten / durationSec / bytesInMB) << "MB/s" << Endl
             << " comp_written# " << Sprintf("%.3f", (double)compBytesWritten / bytesInGB) << "GB"
             << " rt_written# " << Sprintf("%.3f", (double)rtBytesWritten / bytesInGB) << "GB"
             << Endl;

        // All in-flight device requests are expected to complete before the new request starts.
        // This results in approximately 4x ioDuration latency. Additionally, there are
        // one request handled by the Submit thread and one by the PDisk main thread.
        //
        // In this test we observe ~35 ms, which is close to the theoretical expectation.
        // We use a ×10 margin in the assertion to avoid false positives due to timing noise.
        //
        // For comparison, the noop scheduler produces ~×60 latency in the same test.
        UNIT_ASSERT_LE(avgLatencyMs, ioDuration.MillisecondsFloat() * 10);
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
        const ui32 firstNodeId = testCtx.GetRuntime()->GetFirstNodeId();
        // The actual value of SharedQuota.HardLimit internally initialized in TActorTestContext
        // Feel free to update if some day it changes
        const ui32 sharedQuota = 708;

        // Setup receiving whiteboard state updates
        testCtx.GetRuntime()->SetDispatchTimeout(10 * TDuration::MilliSeconds(testCtx.GetPDiskConfig()->StatisticsUpdateIntervalMs));
        testCtx.GetRuntime()->RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(firstNodeId), testCtx.Sender);

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

        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        using NTestSuiteTPDiskTest::AwaitAndCheckEvPDiskStateUpdate;
        using NTestSuiteTPDiskTest::CheckEvCheckSpace;

        testCtx.TestResponse<NPDisk::TEvYardResizeResult>(
            new NPDisk::TEvYardResize(vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound, 4u),
            NKikimrProto::OK);

        AwaitAndCheckEvPDiskStateUpdate(testCtx, 0u, 4);
        CheckEvCheckSpace(testCtx, vdisk, sharedQuota, sharedQuota, 0, 0.0, 0.0, 0.0, 1, 4, TColor::GREEN);

        testCtx.TestResponse<NPDisk::TEvChangeExpectedSlotCountResult>(
            new NPDisk::TEvChangeExpectedSlotCount(8, 2u),
            NKikimrProto::OK);
        auto pdiskConfig = testCtx.GetPDiskConfig();
        UNIT_ASSERT_VALUES_EQUAL(pdiskConfig->ExpectedSlotCount, 8);
        UNIT_ASSERT_VALUES_EQUAL(pdiskConfig->SlotSizeInUnits, 2u);

        AwaitAndCheckEvPDiskStateUpdate(testCtx, 2u, 2);
        CheckEvCheckSpace(testCtx, vdisk, sharedQuota, sharedQuota/8*2, 0, 0.0, 0.0, 0.0, 1, 2, TColor::GREEN);
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


Y_UNIT_TEST_SUITE(TPDiskPrefailureDiskTest) {

    struct TSectorInfo {
        ui64 PatternNumber = 0;
        TInstant WriteTimestamp;
        bool LastReadSuccessful = false;
        bool WasWritten = false;
    };

    struct TChunkInfo {
        TChunkIdx ChunkIdx;
        std::vector<TSectorInfo> Sectors;

        TChunkInfo(TChunkIdx chunkIdx, ui32 sectorCount)
            : ChunkIdx(chunkIdx)
            , Sectors(sectorCount)
        {}
    };

    struct TDelayedReadRequest {
        TChunkIdx ChunkIdx;
        ui32 SectorOffset;
        ui32 SectorCount;
        TInstant ScheduledTime;
    };

    struct TPdt {
        TActorTestContext* TestCtx;
        THolder<TVDiskMock> VDisk;

        // Member variables moved from Run()
        ui64 ChunkSize;
        ui64 AppendBlockSize;
        std::vector<TChunkInfo> ChunkInfos;
        std::unordered_map<TChunkIdx, size_t> ChunkIndexMap;
        ui64 CurrentPattern;
        std::deque<TDelayedReadRequest> DelayedReads;
        TReallyFastRng32 Random;
        ui64 IterationCount;
        ui64 WriteCount;
        ui64 ReadCount;
        ui64 BytesWritten;
        ui64 BytesRead;
        ui64 AnomalyCount;
        ui64 UnreadableSectorCount;
        ui64 DirectReadMismatchCount;
        TInstant TestStartTime;

        TPdt()
            : ChunkSize(0)
            , AppendBlockSize(0)
            , CurrentPattern(1)
            , Random(TInstant::Now().MicroSeconds())
            , IterationCount(0)
            , WriteCount(0)
            , ReadCount(0)
            , BytesWritten(0)
            , BytesRead(0)
            , AnomalyCount(0)
            , UnreadableSectorCount(0)
            , DirectReadMismatchCount(0)
        {}

        TString GeneratePattern(ui32 size) {
            TString data(size, 0);
            ui64 pattern = CurrentPattern++;
            char* dataPtr = data.Detach();
            for (ui32 i = 0; i < size; i += sizeof(ui64)) {
                memcpy(dataPtr + i, &pattern, Min<ui32>(sizeof(ui64), size - i));
            }
            return data;
        }

        bool VerifyPattern(TRcBuf& data, ui64 expectedPattern) {
            size_t remainingSize = data.GetSize();
            const char* ptr = data.GetData();
            while (remainingSize >= sizeof(ui64)) {
                ui64 actual;
                memcpy(&actual, ptr, sizeof(ui64));
                if (actual != expectedPattern) {
                    Cerr << "CRITICAL ERROR: Pattern mismatch! Expected: " << expectedPattern
                         << " Actual: " << actual << " at offset " << (data.GetSize() - remainingSize)
                         << " inside the buffer!" << Endl;
                    return false;
                }
                ptr += sizeof(ui64);
                remainingSize -= sizeof(ui64);
            }
            return true;
        }

        bool PerformDirectIo(TChunkIdx chunkIdx, ui32 sectorOffset, TString& data, bool isWrite) {
            ui64 fileOffset = (ui64)chunkIdx * ChunkSize + (ui64)sectorOffset * 4096;

            try {
                TFile file(TestCtx->TestCtx.Path, OpenExisting | RdWr | DirectAligned | Sync);

                // Check file size
                i64 fileSize = file.GetLength();
                if ((i64)fileOffset + (i64)data.size() > fileSize) {
                    Cerr << "Direct I/O out of bounds: offset " << fileOffset << " + size " << data.size()
                         << " > file size " << fileSize << Endl;
                    return false;
                }

                // Allocate aligned buffer for direct I/O
                constexpr size_t alignment = 4096;
                void* alignedBuffer = nullptr;
                if (posix_memalign(&alignedBuffer, alignment, data.size()) != 0) {
                    Cerr << "Failed to allocate aligned buffer for direct I/O" << Endl;
                    return false;
                }

                if (isWrite) {
                    memcpy(alignedBuffer, data.data(), data.size());
                    file.Pwrite(alignedBuffer, data.size(), fileOffset);
                    free(alignedBuffer);
                    Cerr << "Direct write at chunk " << chunkIdx << " sector " << sectorOffset
                         << " offset " << fileOffset << " size " << data.size() << Endl;
                } else {
                    file.Pread(alignedBuffer, data.size(), fileOffset);
                    memcpy(data.Detach(), alignedBuffer, data.size());
                    free(alignedBuffer);
                    Cerr << "Direct read at chunk " << chunkIdx << " sector " << sectorOffset
                         << " offset " << fileOffset << " size " << data.size() << Endl;
                }
                return true;
            } catch (const TFileError& e) {
                Cerr << "Direct I/O ERROR at chunk " << chunkIdx << " sector " << sectorOffset
                     << " offset " << fileOffset << " size " << data.size()
                     << " operation: " << (isWrite ? "WRITE" : "READ")
                     << " error: " << e.what() << Endl;
                return false;
            } catch (...) {
                Cerr << "Direct I/O UNKNOWN ERROR at chunk " << chunkIdx << " sector " << sectorOffset
                     << " offset " << fileOffset << " size " << data.size()
                     << " operation: " << (isWrite ? "WRITE" : "READ") << Endl;
                return false;
            }
        }

        void Init() {
            VDisk = MakeHolder<TVDiskMock>(TestCtx);
            VDisk->InitFull();
            VDisk->SendEvLogSync();
            ChunkSize = VDisk->PDiskParams->ChunkSize;
            AppendBlockSize = VDisk->PDiskParams->AppendBlockSize;

            NPDisk::TDiskFormat format = TestCtx->SafeRunOnPDisk([&](NPDisk::TPDisk* pDisk) {
                NPDisk::TDiskFormat diskFormat = pDisk->Format;
                return diskFormat;
            });

            ui32 totalChunks = format.DiskSizeChunks();
            Cerr << "Total chunks: " << totalChunks << Endl;
            ui32 chunksToReserve = (totalChunks - 200) * 0.85;
            Cerr << "Reserving " << chunksToReserve << " chunks" << Endl;
            VDisk->ReserveChunk(chunksToReserve);
            VDisk->CommitReservedChunks();
            UNIT_ASSERT(VDisk->Chunks[EChunkState::COMMITTED].size() == chunksToReserve);

            ChunkInfos.reserve(chunksToReserve);
            for (const auto& chunk : VDisk->Chunks[EChunkState::COMMITTED]) {
                ChunkIndexMap[chunk] = ChunkInfos.size();
                ChunkInfos.emplace_back(chunk, ChunkSize / AppendBlockSize);
            }

            Cerr << "Chunk size: " << ChunkSize << ", AppendBlockSize: " << AppendBlockSize
                 << ", Sectors per chunk: " << (ChunkSize / AppendBlockSize) << Endl;

            TestStartTime = TInstant::Now();
        }

        void RecheckSector(TChunkIdx chunkIdx, ui32 sectorIdx) {
            // Find chunk info
            auto mapIt = ChunkIndexMap.find(chunkIdx);
            if (mapIt == ChunkIndexMap.end()) {
                Cerr << "CRITICAL ERROR in RecheckSector: Chunk not found in index map! Chunk: " << chunkIdx << Endl;
                Cerr << "Now: " << TInstant::Now() << Endl;
                return;
            }
            TChunkInfo& chunkInfo = ChunkInfos[mapIt->second];

            // Direct I/O test sequence: read - write - read
            Cerr << "  Performing direct I/O test..." << Endl;

            TString directRead1(4096, 0);
            bool directRead1Success = PerformDirectIo(chunkIdx, sectorIdx, directRead1, false);
            if (!directRead1Success) {
                Cerr << "  Direct read 1: FAILED (I/O error)" << Endl;
            } else {
                Cerr << "  Direct read 1 done." << Endl;
            }

            TString testData = GeneratePattern(4096);
            ui64 testPattern = CurrentPattern - 1;
            bool directWriteSuccess = PerformDirectIo(chunkIdx, sectorIdx, testData, true);
            if (!directWriteSuccess) {
                Cerr << "  Direct write: FAILED (I/O error)" << Endl;
            } else {
                Cerr << "  Direct write completed with test pattern " << testPattern << Endl;
            }

            TString directRead2(4096, 0);
            bool directRead2Success = PerformDirectIo(chunkIdx, sectorIdx, directRead2, false);
            if (!directRead2Success) {
                Cerr << "  Direct read 2: FAILED (I/O error)" << Endl;
            } else {
                TRcBuf directRead2Buf{TString(directRead2)};
                bool directRead2Valid = VerifyPattern(directRead2Buf, testPattern);
                Cerr << "  Direct read 2: " << (directRead2Valid ? "VALID" : "INVALID") << Endl;

                if (!directRead2Valid) {
                    ++DirectReadMismatchCount;
                }

                if (directRead1Success) {
                    bool directRead2MatchesPreWrite = true;
                    for (size_t i = 0; i < 4096; i++) {
                        if (directRead2[i] != directRead1[i]) {
                            directRead2MatchesPreWrite = false;
                            break;
                        }
                    }
                    Cerr << "  Direct read 2: " << (directRead2MatchesPreWrite ? "MATCHES PRE WRITE VALUE" : "DOES NOT MATCH PRE WRITE VALUE") << Endl;
                }
            }

            // PDisk write-read test
            Cerr << "  Performing PDisk I/O test..." << Endl;
            TString pdiskTestData = GeneratePattern(AppendBlockSize);
            ui64 pdiskTestPattern = CurrentPattern - 1;

            auto counter = MakeIntrusive<::NMonitoring::TCounterForPtr>();
            TMemoryConsumer consumer(counter);
            TTrackableBuffer pdiskWriteBuf(std::move(consumer), pdiskTestData.data(), pdiskTestData.size());
            auto evWrite = MakeHolder<NPDisk::TEvChunkWrite>(
                VDisk->PDiskParams->Owner, VDisk->PDiskParams->OwnerRound,
                chunkIdx, sectorIdx * AppendBlockSize,
                MakeIntrusive<NPDisk::TEvChunkWrite::TBufBackedUpParts>(std::move(pdiskWriteBuf)),
                nullptr, false, 0);
            TestCtx->TestResponse<NPDisk::TEvChunkWriteResult>(evWrite.Release(), NKikimrProto::OK);

            const auto evPDiskRead = TestCtx->TestResponse<NPDisk::TEvChunkReadResult>(
                new NPDisk::TEvChunkRead(VDisk->PDiskParams->Owner, VDisk->PDiskParams->OwnerRound,
                    chunkIdx, sectorIdx * AppendBlockSize, AppendBlockSize, 0, nullptr),
                NKikimrProto::OK);

            TBufferWithGaps& bwg = evPDiskRead->Data;

            if (!bwg.IsReadable(0, AppendBlockSize)) {
                ui64 physicalOffset = (ui64)chunkIdx * ChunkSize + (ui64)sectorIdx * 4096;
                Cerr << "  REREAD CRITICAL ERROR: Sector not readable!" << Endl;
                Cerr << "    Chunk: " << chunkIdx << " Sector: " << sectorIdx << Endl;
                Cerr << "    Physical offset: " << physicalOffset << " bytes (" << (physicalOffset / (1024*1024*1024)) << " GiB)" << Endl;
                Cerr << "    Pattern number: " << chunkInfo.Sectors[sectorIdx].PatternNumber << Endl;
                Cerr << "    Write timestamp: " << chunkInfo.Sectors[sectorIdx].WriteTimestamp << Endl;
                Cerr << "    Last read successful: " << (chunkInfo.Sectors[sectorIdx].LastReadSuccessful ? "true" : "false") << Endl;
                Cerr << "Now: " << TInstant::Now() << Endl;
            } else {
                TRcBuf sectorData = bwg.Substr(0, AppendBlockSize);

                bool pdiskReadValid = VerifyPattern(sectorData, pdiskTestPattern);
                Cerr << "  PDisk REREAD Chunk: " << chunkIdx << " Sector: " << sectorIdx << " after write: " << (pdiskReadValid ? "VALID" : "INVALID") << Endl;

                // Output first ui64
                if (sectorData.size() >= sizeof(ui64)) {
                    ui64 firstValue = 0;
                    memcpy(&firstValue, sectorData.data(), sizeof(ui64));
                    Cerr << "  First ui64 from reread: " << firstValue << Endl;
                }
                Cerr << "Now: " << TInstant::Now() << Endl;
            }
            chunkInfo.Sectors[sectorIdx].PatternNumber = pdiskTestPattern;
            chunkInfo.Sectors[sectorIdx].LastReadSuccessful = false;
        }

        void ReadWithPDisk(TChunkIdx chunkIdx, ui32 sectorOffset, ui32 sectorCount) {
            ++ReadCount;
            // Read from PDisk
            ui32 readSize = sectorCount * AppendBlockSize;
            ui64 offset = sectorOffset * AppendBlockSize;

            const auto evReadRes = TestCtx->TestResponse<NPDisk::TEvChunkReadResult>(
                new NPDisk::TEvChunkRead(VDisk->PDiskParams->Owner, VDisk->PDiskParams->OwnerRound,
                    chunkIdx, offset, readSize, 0, nullptr),
                std::nullopt);  // Don't assert status, check it manually

            if (evReadRes->Status != NKikimrProto::OK) {
                Cerr << "ERROR: Read failed with status " << NKikimrProto::EReplyStatus_Name(evReadRes->Status)
                     << " reason: " << evReadRes->ErrorReason << Endl;
                Cerr << "  Chunk: " << chunkIdx << " Offset: " << offset << " Size: " << readSize << Endl;
                Cerr << "  Read is terminating due to PDisk error" << Endl;
                Cerr << "Now: " << TInstant::Now() << Endl;
                return;
            }

            BytesRead += readSize;

            TBufferWithGaps& bwg = evReadRes->Data;
            //const_cast<TBufferWithGaps&>(bwg).Commit();

            // Find chunk info
            auto mapIt = ChunkIndexMap.find(chunkIdx);
            if (mapIt == ChunkIndexMap.end()) {
                Cerr << "CRITICAL ERROR: Chunk not found in index map! Chunk: " << chunkIdx << Endl;
                Cerr << "Read request had size: " << readSize << Endl;
                Cerr << "Read request had offset: " << offset << Endl;
                Cerr << "Now: " << TInstant::Now() << Endl;
                return;
            }
            TChunkInfo& chunkInfo = ChunkInfos[mapIt->second];

            for (ui32 i = 0; i < sectorCount; ++i) {
                ui32 sectorIdx = sectorOffset + i;
                if (sectorIdx < chunkInfo.Sectors.size() && chunkInfo.Sectors[sectorIdx].WasWritten) {
                    ui64 expectedPattern = chunkInfo.Sectors[sectorIdx].PatternNumber;

                    if (!bwg.IsReadable(i * AppendBlockSize, AppendBlockSize)) {
                        ++UnreadableSectorCount;
                        ui64 physicalOffset = (ui64)chunkIdx * ChunkSize + (ui64)sectorIdx * 4096;
                        Cerr << "ERROR: Sector not readable!" << Endl;
                        Cerr << "  Chunk: " << chunkIdx << " Sector: " << sectorIdx << Endl;
                        Cerr << "  Physical offset: " << physicalOffset << " bytes (" << (physicalOffset / (1024*1024*1024)) << " GiB)" << Endl;
                        Cerr << "  Pattern number: " << chunkInfo.Sectors[sectorIdx].PatternNumber << Endl;
                        Cerr << "  Write timestamp: " << chunkInfo.Sectors[sectorIdx].WriteTimestamp << Endl;
                        Cerr << "  Last read successful: " << (chunkInfo.Sectors[sectorIdx].LastReadSuccessful ? "true" : "false") << Endl;
                        Cerr << "  Time since write: " << (TInstant::Now() - chunkInfo.Sectors[sectorIdx].WriteTimestamp).Seconds() << " seconds" << Endl;
                        Cerr << "  Now: " << TInstant::Now() << Endl;

                        RecheckSector(chunkIdx, sectorIdx);
                        continue;
                    }

                    TRcBuf sectorData = bwg.Substr(i * AppendBlockSize, AppendBlockSize);

                    if (!VerifyPattern(sectorData, expectedPattern)) {
                        ++AnomalyCount;

                        Cerr << "ANOMALY DETECTED!" << Endl;
                        Cerr << "  Chunk: " << chunkIdx << " Sector: " << sectorIdx << Endl;
                        Cerr << "  Expected pattern: " << expectedPattern << Endl;
                        Cerr << "  Write time: " << chunkInfo.Sectors[sectorIdx].WriteTimestamp << Endl;
                        Cerr << "  Read time: " << TInstant::Now() << Endl;
                        Cerr << "  Now: " << TInstant::Now() << Endl;

                        // Output first ui64 from the sector
                        if (sectorData.size() >= sizeof(ui64)) {
                            ui64 firstValue = 0;
                            memcpy(&firstValue, sectorData.data(), sizeof(ui64));
                            Cerr << "  First ui64 read: " << firstValue << " (expected: " << expectedPattern << ")" << Endl;
                        }

                        RecheckSector(chunkIdx, sectorIdx);
                    } else {
                        chunkInfo.Sectors[sectorIdx].LastReadSuccessful = true;
                    }
                }
            }
        }

        void RandomReadWithPDisk() {

            ui32 chunkIdx = Random.GenRand() % ChunkInfos.size();
            TChunkInfo& chunkInfo = ChunkInfos[chunkIdx];

            ui32 sectorsPerChunk = chunkInfo.Sectors.size();
            ui32 sectorOffset = Random.GenRand() % sectorsPerChunk;
            ui32 initialSectorOffset = sectorOffset;
            while (!chunkInfo.Sectors[sectorOffset].WasWritten && sectorOffset < sectorsPerChunk) {
                sectorOffset++;
            }
            if (sectorOffset >= sectorsPerChunk) {
                for (sectorOffset = 0; sectorOffset < initialSectorOffset; sectorOffset++) {
                    if (chunkInfo.Sectors[sectorOffset].WasWritten) {
                        break;
                    }
                }
            }
            if (!chunkInfo.Sectors[sectorOffset].WasWritten) {
                return;
            }
            ui32 maxSectors = Min<ui32>(sectorsPerChunk - sectorOffset, 2048);
            ui32 sectorCount = 1 + Random.GenRand() % maxSectors;

            ReadWithPDisk(chunkInfo.ChunkIdx, sectorOffset, sectorCount);
        }

        void RandomWriteWithPDisk() {
            ++WriteCount;

            ui32 chunkIdx = Random.GenRand() % ChunkInfos.size();
            TChunkInfo& chunkInfo = ChunkInfos[chunkIdx];

            ui32 sectorsPerChunk = chunkInfo.Sectors.size();
            ui32 sectorOffset = Random.GenRand() % sectorsPerChunk;
            ui32 maxSectors = Min<ui32>(sectorsPerChunk - sectorOffset, 2048);
            ui32 sectorCount = 1 + Random.GenRand() % maxSectors;

            ui32 writeSize = sectorCount * AppendBlockSize;
            TString writeData = GeneratePattern(writeSize);
            ui64 pattern = CurrentPattern - 1;

            auto counter = MakeIntrusive<::NMonitoring::TCounterForPtr>();
            TMemoryConsumer consumer(counter);
            TTrackableBuffer writeBuf(std::move(consumer), writeData.data(), writeData.size());
            auto evWrite = MakeHolder<NPDisk::TEvChunkWrite>(
                VDisk->PDiskParams->Owner, VDisk->PDiskParams->OwnerRound,
                chunkInfo.ChunkIdx, sectorOffset * AppendBlockSize,
                MakeIntrusive<NPDisk::TEvChunkWrite::TBufBackedUpParts>(std::move(writeBuf)),
                nullptr, false, 0);

            const auto evWriteRes = TestCtx->TestResponse<NPDisk::TEvChunkWriteResult>(evWrite.Release(), std::nullopt);

            if (evWriteRes->Status != NKikimrProto::OK) {
                Cerr << "ERROR: Write failed with status " << NKikimrProto::EReplyStatus_Name(evWriteRes->Status)
                     << " reason: " << evWriteRes->ErrorReason << Endl;
                Cerr << "  Chunk: " << chunkInfo.ChunkIdx << " Offset: " << (sectorOffset * AppendBlockSize)
                     << " Size: " << writeSize << Endl;
                Cerr << "  Write is terminating due to PDisk error (likely out of space)" << Endl;
                return;
            }

            BytesWritten += writeSize;
            // Update sector info
            TInstant writeTime = TInstant::Now();
            for (ui32 i = 0; i < sectorCount; ++i) {
                chunkInfo.Sectors[sectorOffset + i].PatternNumber = pattern;
                chunkInfo.Sectors[sectorOffset + i].WriteTimestamp = writeTime;
                chunkInfo.Sectors[sectorOffset + i].WasWritten = true;
            }

            // 50% probability: read immediately or schedule delayed read
            if (Random.GenRand() % 2 == 0) {
                ReadWithPDisk(chunkInfo.ChunkIdx, sectorOffset, sectorCount);
            } else {
                // Schedule delayed read
                TDelayedReadRequest delayedReq;
                delayedReq.ChunkIdx = chunkInfo.ChunkIdx;
                delayedReq.SectorOffset = sectorOffset;
                delayedReq.SectorCount = sectorCount;
                delayedReq.ScheduledTime = TInstant::Now() + TDuration::Seconds(30);
                DelayedReads.push_back(delayedReq);
            }
        }

        void Run() {
            Init();

            Cerr << "Starting test loop..." << Endl;

            TInstant lastInfoTime;
            while (true) {
                ++IterationCount;

                if (IterationCount % 100 == 0) {
                    if (TInstant::Now() - lastInfoTime > TDuration::Seconds(60)) {
                        Cerr << "Time: " << TInstant::Now()
                            << " Duration: " << (TInstant::Now() - TestStartTime).Seconds() << " seconds"
                            << " Iteration: " << IterationCount
                            << " writes=" << WriteCount
                            << " reads=" << ReadCount
                            << " anomalies=" << AnomalyCount
                            << " unreadable_sectors=" << UnreadableSectorCount
                            << " direct_read_mismatches=" << DirectReadMismatchCount
                            << " bytes written=" << BytesWritten
                            << " bytes read=" << BytesRead
                            << Endl;
                        lastInfoTime = TInstant::Now();
                    }
                }

                // Process delayed reads
                TInstant now = TInstant::Now();
                while (!DelayedReads.empty() && DelayedReads.front().ScheduledTime <= now) {
                    const auto& req = DelayedReads.front();
                    ReadWithPDisk(req.ChunkIdx, req.SectorOffset, req.SectorCount);
                    DelayedReads.pop_front();
                }

                // Random write operation
                if (Random.GenRand() % 2 == 0) {
                    RandomWriteWithPDisk();
                }

                // Random read operation
                if (Random.GenRand() % 3 == 0 && ChunkInfos.size() > 0) {
                    RandomReadWithPDisk();
                }
            }

            Cerr << "Test completed!" << Endl;
            Cerr << "Total iterations: " << IterationCount << Endl;
            Cerr << "Total writes: " << WriteCount << Endl;
            Cerr << "Total reads: " << ReadCount << Endl;
            Cerr << "Total anomalies: " << AnomalyCount << Endl;
            Cerr << "Total unreadable sectors: " << UnreadableSectorCount << Endl;
            Cerr << "Total direct read mismatches: " << DirectReadMismatchCount << Endl;
            Cerr << "Total bytes written: " << BytesWritten << Endl;
            Cerr << "Total bytes read: " << BytesRead << Endl;
            Cerr << "Test duration: " << (TInstant::Now() - TestStartTime).Seconds() << " seconds" << Endl;
        }
    };

    Y_UNIT_TEST(TestWriteReadRepeat) {
        // Check for prefailure_path.txt file
        TString pathConfigFile = "prefailure_path.txt";

        if (!NFs::Exists(pathConfigFile)) {
            Cerr << "WARNING: " << pathConfigFile << " does not exist. Test skipped." << Endl;
            return;  // Test completes successfully
        }

        // Read path from config file
        TFileInput input(pathConfigFile);
        TString path = input.ReadAll();
        path = StripString(path);  // Remove whitespace and newlines

        if (path.empty()) {
            Cerr << "WARNING: " << pathConfigFile << " is empty. Test skipped." << Endl;
            return;  // Test completes successfully
        }

        Cerr << "Using path from " << pathConfigFile << ": " << path << Endl;

        // Detect disk size from existing file or block device
        ui64 diskSize = 0;
        bool isBlockDevice = false;

        if (NFs::Exists(path)) {
            // Use DetectFileParameters to handle both regular files and block devices
            ::NKikimr::DetectFileParameters(path, diskSize, isBlockDevice);
            Cerr << "Detected file type: " << (isBlockDevice ? "block device" : "regular file") << Endl;
            Cerr << "Detected size: " << diskSize << " bytes ("
                 << (diskSize / (1024*1024*1024)) << " GiB)" << Endl;
        } else {
            Cerr << "File does not exist, will be created with default size" << Endl;
        }

        TActorTestContext testCtx{
            {.DiskSize = diskSize,
             .UsePath = path,
             .UseSectorMap = false}};
        TPdt pdt;
        pdt.TestCtx = &testCtx;
        pdt.Run();
    }
}

// RDMA use ibverbs shared library which is part of hardware vendor provided drivers and can't be linked staticaly
// This library is not MSan-instrumented, so it causes msan fail if we run. So skip such run...
static bool IsMsanEnabled() {
#if defined(_msan_enabled_)
    return true;
#else
    return false;
#endif
}

Y_UNIT_TEST_SUITE(RDMA) {
    void TestChunkReadWithRdmaAllocator(bool plainDataChunks) {
        TActorTestContext testCtx({
            .PlainDataChunks = plainDataChunks,
            .UseRdmaAllocator = true,
        });
        TVDiskMock vdisk(&testCtx);

        vdisk.InitFull();
        vdisk.ReserveChunk();
        vdisk.CommitReservedChunks();
        auto chunk = *vdisk.Chunks[EChunkState::COMMITTED].begin();

        for (i64 writeSize: {1_KB - 123, 64_KB, 128_KB + 8765}) {
            for (i64 readOffset: {0_KB, 0_KB + 123, 4_KB, 8_KB + 123}) {
                for (i64 readSize: {writeSize, writeSize - 567, writeSize - i64(8_KB), writeSize - i64(8_KB) + 987}) {
                    if (readOffset < 0 || readSize < 0 || readOffset + readSize > writeSize) {
                        continue;
                    }
                    Cerr << "chunkBufSize: " << writeSize << ", readOffset: " << readOffset << ", readSize: " << readSize << Endl;
                    auto parts = MakeIntrusive<NPDisk::TEvChunkWrite::TAlignedParts>(PrepareData(writeSize));
                    testCtx.Send(new NPDisk::TEvChunkWrite(
                        vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                        chunk, 0, parts, nullptr, false, 0));
                    auto write = testCtx.Recv<NPDisk::TEvChunkWriteResult>();

                    testCtx.Send(new NPDisk::TEvChunkRead(
                        vdisk.PDiskParams->Owner, vdisk.PDiskParams->OwnerRound,
                        chunk, readOffset, readSize, 0, nullptr));
                    auto read = testCtx.Recv<NPDisk::TEvChunkReadResult>();
                    TRcBuf readBuf = read->Data.ToString();
                    NInterconnect::NRdma::TMemRegionSlice memReg = NInterconnect::NRdma::TryExtractFromRcBuf(readBuf);
                    UNIT_ASSERT_C(!memReg.Empty(), "Failed to extract RDMA memory region from RcBuf");
                    UNIT_ASSERT_VALUES_EQUAL_C(memReg.GetSize(), readSize, "Unexpected size of RDMA memory region");
                }
            }
        }
    }

    Y_UNIT_TEST(TestChunkReadWithRdmaAllocatorEncryptedChunks) {
        if (IsMsanEnabled())
            return;

        TestChunkReadWithRdmaAllocator(false);
    }

    Y_UNIT_TEST(TestChunkReadWithRdmaAllocatorPlainChunks) {
        if (IsMsanEnabled())
            return;

        TestChunkReadWithRdmaAllocator(true);
    }

    Y_UNIT_TEST(TestRcBuf) {
        if (IsMsanEnabled())
            return;

        ui32 size = 129961;
        ui32 offset = 123;
        ui32 tailRoom = 1111;
        ui32 totalSize = size + tailRoom;
        UNIT_ASSERT_VALUES_EQUAL(totalSize, 131072);

        auto alloc1 = [](ui32 size, ui32 headRoom, ui32 tailRoom) {
            TRcBuf buf = TRcBuf::UninitializedPageAligned(size + headRoom + tailRoom);
            buf.TrimFront(size + tailRoom);
            buf.TrimBack(size);
            Cerr << "alloc1: " << buf.Size() << " " << buf.Tailroom() << " " << buf.UnsafeTailroom() << Endl;
            return buf;
        };
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        auto alloc2 = [memPool](ui32 size, ui32 headRoom, ui32 tailRoom) -> TRcBuf {
            TRcBuf buf = memPool->AllocRcBuf(size + headRoom + tailRoom, NInterconnect::NRdma::IMemPool::EMPTY).value();
            buf.TrimFront(size + tailRoom);
            buf.TrimBack(size);
            Cerr << "alloc2: " << buf.Size() << " " << buf.Tailroom() << " " << buf.UnsafeTailroom() << Endl;
            return buf;
        };

        auto buf1 = TBufferWithGaps(offset, alloc1(size, 0, tailRoom));
        auto buf2 = TBufferWithGaps(offset, alloc2(size, 0, tailRoom));

        Cerr << "buf1: " << buf1.PrintState() << " " << buf1.Size() << " " << buf1.SizeWithTail() << Endl;
        Cerr << "buf2: " << buf2.PrintState() << " " << buf2.Size() << " " << buf2.SizeWithTail() << Endl;

        UNIT_ASSERT_VALUES_EQUAL_C(buf1.Size(), buf2.Size(), "Buffers should have the same size");

        buf1.RawDataPtr(0, totalSize);
        buf2.RawDataPtr(0, totalSize);
    }

    Y_UNIT_TEST(ChunkWriteDifferentOffsetAndSize) {
        if (IsMsanEnabled())
            return;

        for (ui32 i = 0; i <= 1; ++i) {
            NTestSuiteTPDiskTest::ChunkWriteDifferentOffsetAndSizeImpl(i & 1, true);
        }
    }
}

} // namespace NKikimr
