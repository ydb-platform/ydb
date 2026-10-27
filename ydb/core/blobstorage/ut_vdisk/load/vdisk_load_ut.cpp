#include "../defaults.h"
#include "../gen_restarts.h"
#include <ydb/core/blobstorage/ut_vdisk/lib/setup.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_dbstat.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_localrecovery.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_many.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_outofspace.h>

#include <library/cpp/testing/unittest/registar.h>

#define YDB_LOG_THIS_FILE_COMPONENT NActorsServices::TEST

#define SMALL_TEST
#ifdef SMALL_TEST
#define LARGE_MSG_NUM   500
#else
#define LARGE_MSG_NUM   5000
#endif

namespace NKikimr {
namespace NPDisk {
    extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;
}
}

template <class TTest, class TVDiskSetup = TDefaultVDiskSetup>
void TestRun(TTest *test,
             const TDuration &timeout,
             ui32 chunkSize = DefChunkSize,
             ui64 diskSize = DefDiskSize,
             ui32 domainsNum = 1,
             ui32 disksInDomain = 1,
             NKikimr::TErasureType::EErasureSpecies erasure = NKikimr::TBlobStorageGroupType::ErasureNone)
{
    TConfiguration Conf(TAllPDisksConfiguration::MkOneTmp(chunkSize, diskSize, "ROT"),
                        domainsNum,
                        disksInDomain,
                        erasure);
    TVDiskSetup vdiskSetup;
    Conf.Prepare(&vdiskSetup);
    bool success = Conf.Run<TTest>(test, timeout);
    Conf.Shutdown();

    UNIT_ASSERT(success);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// OUT OF SPACE
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsVDiskOutOfSpace) {
    Y_UNIT_TEST(WriteUntilOrangeZone) {
        return; // Test is ignored. FIX: KIKIMR-8019

        TWriteUntilOrangeZone test;
        const ui32 chunkSize = 512u << 10u;
        const ui64 diskSize = 500ull << 20ull;
        TestRun<TWriteUntilOrangeZone, TFastVDiskSetupCompacted>(&test, TIMEOUT, chunkSize, diskSize);
    }

    Y_UNIT_TEST(WriteUntilYellowZone) {
        TWriteUntilYellowZone test;
        const ui32 chunkSize = 512u << 10u;
        const ui64 diskSize = 700ull << 20ull;
        TestRun<TWriteUntilYellowZone, TFastVDiskSetupCompacted>(&test, TIMEOUT, chunkSize, diskSize);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Other
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsOther1) {

    Y_UNIT_TEST(PoisonPill) {
        TManyPutRangeGet2Channels test(false, true, LARGE_MSG_NUM, 100, UNK);
        TConfiguration Conf = CreateErasureNone();
        TFastVDiskSetup vdiskSetup;
        Conf.Prepare(&vdiskSetup);
        bool success = Conf.Run<TManyPutRangeGet2Channels>(&test, TDuration::Minutes(100));
        Conf.PoisonVDisks();
        Sleep(TDuration::Seconds(1)); // FIXME: we need to wait until vdisk is shut down
        Conf.Shutdown();
        UNIT_ASSERT(success);
    }

    template <class TSetup>
    void ChaoticParallelWriteGeneric(ui32 parallel, ui32 msgNum, ui32 msgSize,
                                     std::shared_ptr<IPutHandleClassGenerator> cls,
                                     TDuration workingTime, TDuration requestTimeout) {
        TConfiguration Conf = CreateErasureNone();
        TSetup vdiskSetup;
        Conf.Prepare(&vdiskSetup);

        TChaoticManyPutsTest w(parallel, msgNum, msgSize, cls, workingTime, requestTimeout);
        bool success1 = Conf.Run<TChaoticManyPutsTest>(&w, TDuration::Seconds(600));
        YDB_LOG_NOTICE_CTX(*Conf.ActorSystem1, "Chaotic write done");
        UNIT_ASSERT(success1);
        Conf.Shutdown();
    }

    Y_UNIT_TEST(ChaoticParallelWrite) {
        auto cls = std::make_shared<TPutHandleClassGenerator>(UNK);
        NTestSuiteTBsOther1::ChaoticParallelWriteGeneric<TFastVDiskSetup>(
                5000, 100, 10, cls, TDuration::Seconds(600), TDuration());
    }
}

Y_UNIT_TEST_SUITE(TBsOther2) {
    Y_UNIT_TEST(ChaoticParallelWrite_SkeletonFrontQueuesOverload) {
        // randomly generate put class
        class TRandomPutHandleClassGenerator : public IPutHandleClassGenerator {
        public:
            NKikimrBlobStorage::EPutHandleClass GetHandleClass() override {
                auto handleClass = rand() % 2 ? NKikimrBlobStorage::AsyncBlob : NKikimrBlobStorage::UserData;
                return handleClass;
            }
        };

        // According to TFastVDiskSetupSmallVDiskQueues we have very small queues at SkeletonFront
        // and get them overflow
        // vdisk must handle request in 0.1 sec or drop it, expect overload
        const auto requestTimeout = TDuration::MilliSeconds(100);
        auto cls = std::make_shared<TRandomPutHandleClassGenerator>();
        NTestSuiteTBsOther1::ChaoticParallelWriteGeneric<TFastVDiskSetupSmallVDiskQueues>(
                5000, 100, 10, cls, TDuration::Seconds(600), requestTimeout);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Other
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsDbStat) {
    Y_UNIT_TEST(ChaoticParallelWrite_DbStat) {
        auto cls = std::make_shared<TPutHandleClassGenerator>(UNK);
        const ui32 parallel = 50;
        const ui32 msgNum = 10000;
        const ui32 msgSize = 10;
        const TDuration workingTime = TDuration::Seconds(600);
        const TDuration requestTimeout = TDuration();

        TConfiguration Conf = CreateErasureNone();
        TFastVDiskSetup vdiskSetup;
        Conf.Prepare(&vdiskSetup);

        TChaoticManyPutsTest w(parallel, msgNum, msgSize, cls, workingTime, requestTimeout);
        bool success1 = Conf.Run<TChaoticManyPutsTest>(&w, TDuration::Seconds(600));
        YDB_LOG_NOTICE_CTX(*Conf.ActorSystem1, "Chaotic write done");
        UNIT_ASSERT(success1);
        // we have this tabletId in data
        ui64 tabletId = 30;
        Y_ABORT_UNLESS(tabletId < parallel);
        TDbStatTest stat(tabletId);
        bool success2 = Conf.Run<TDbStatTest>(&stat, TDuration::Seconds(10));
        UNIT_ASSERT(success2);
        Conf.Shutdown();
    }
}

