#include "defaults.h"
#include "gen_restarts.h"
#include <ydb/core/blobstorage/ut_vdisk/lib/setup.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_gc.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_dbstat.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_defrag.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_huge.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_localrecovery.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_many.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_outofspace.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_bad_blobid.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_brokendevice.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_repl.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_simplebs.h>
#include <ydb/core/blobstorage/ut_vdisk/lib/test_synclog.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/valgrind.h>

namespace NKikimr {
namespace NPDisk {
    extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;
}
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
template <class TTest, class TVDiskSetup = TDefaultVDiskSetup>
void TestRun(TTest *test,
             const TDuration &timeout,
             ui32 chunkSize = DefChunkSize,
             ui64 diskSize = DefDiskSize,
             ui32 domainsNum = DefDomainsNum,
             ui32 disksInDomain = DefDisksInDomain,
             NKikimr::TErasureType::EErasureSpecies erasure = DefErasure)
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

enum EDataSet {
    DG_3PUT,
    DG_3PUT_HUGE,
    DB_3PUT2HND,
    DB_3PUT2HND_HUGE,
    DB_1PUT2HND,
    DB_1PUT2HND_HUGE
};

IDataSetPtr DataSetSelector(EDataSet t, ui32 minHugeBlobSize) {
    switch (t) {
        case DG_3PUT:
            return new T3PutDataSet(NKikimrBlobStorage::EPutHandleClass::TabletLog, minHugeBlobSize, false);
        case DG_3PUT_HUGE:
            return new T3PutDataSet(NKikimrBlobStorage::EPutHandleClass::AsyncBlob, minHugeBlobSize, true);
        case DB_3PUT2HND:
            return new T3PutHandoff2DataSet(NKikimrBlobStorage::EPutHandleClass::TabletLog, minHugeBlobSize, false);
        case DB_3PUT2HND_HUGE:
            return new T3PutHandoff2DataSet(NKikimrBlobStorage::EPutHandleClass::AsyncBlob, minHugeBlobSize, true);
        case DB_1PUT2HND:
            return new T1PutHandoff2DataSet(NKikimrBlobStorage::EPutHandleClass::TabletLog, minHugeBlobSize, false);
        case DB_1PUT2HND_HUGE:
            return new T1PutHandoff2DataSet(NKikimrBlobStorage::EPutHandleClass::AsyncBlob, minHugeBlobSize, true);
    }
    return nullptr;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
#define Y_UNIT_TEST_COMP_DISK(TestName, ClassName, Setup, CompParam, VDiskNumParam, g)     \
Y_UNIT_TEST(TestName) {                                                                    \
    IDataSetPtr ds = DataSetSelector(g, 64u << 10u);                                            \
    ClassName test(ds, CompParam, VDiskNumParam);                                               \
    TestRun<ClassName, Setup>(&test, TIMEOUT);                                   \
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// SIMPLE BS
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsVDiskExtreme) {

    Y_UNIT_TEST(SimpleGetFromEmptyDB) {
        TSimpleGetFromEmptyDB test;
        TestRun<TSimpleGetFromEmptyDB, TFastVDiskSetup>(&test, TIMEOUT);
    }

    Y_UNIT_TEST_COMP_DISK(Simple3Put3GetFresh, TSimple3Put3Get, TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3Put3GetCompaction, TSimple3Put3Get, TFastVDiskSetupCompacted, true, 0, DG_3PUT)

    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqGetAllFresh, TSimple3Put1SeqGetAll, TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqGetAllCompaction, TSimple3Put1SeqGetAll, TFastVDiskSetupCompacted, true, 0, DG_3PUT)

    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqGet2Fresh, TSimple3Put1SeqGet2, TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqGet2Compaction, TSimple3Put1SeqGet2, TFastVDiskSetupCompacted, true, 0, DG_3PUT)

    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqSubsOkFresh, TSimple3Put1SeqSubsOk, TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqSubsOkCompaction, TSimple3Put1SeqSubsOk, TFastVDiskSetupCompacted, true, 0, DG_3PUT)

    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqSubsErrorFresh, TSimple3Put1SeqSubsError, TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqSubsErrorCompaction, TSimple3Put1SeqSubsError, TFastVDiskSetupCompacted, true, 0, DG_3PUT)

    Y_UNIT_TEST_COMP_DISK(Simple3Put1GetMissingKeyFresh, TSimple3Put1GetMissingKey, TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3Put1GetMissingKeyCompaction, TSimple3Put1GetMissingKey, TFastVDiskSetupCompacted, true, 0, DG_3PUT)

    Y_UNIT_TEST_COMP_DISK(Simple3Put1GetMissingPartFresh, TSimple3Put1GetMissingPart, TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3Put1GetMissingPartCompaction, TSimple3Put1GetMissingPart, TFastVDiskSetupCompacted, true, 0, DG_3PUT)
    // TODO: Make range queries
    // TODO: Block and read block
}


///////////////////////////////////////////////////////////////////////////////////////////////////////
// SIMPLE BS
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsVDiskExtremeHuge) {

    Y_UNIT_TEST_COMP_DISK(Simple3Put3GetFresh, TSimple3Put3Get, TFastVDiskSetup, false, 0, DG_3PUT_HUGE)
    Y_UNIT_TEST_COMP_DISK(Simple3Put3GetCompaction, TSimple3Put3Get, TFastVDiskSetupCompacted, true, 0, DG_3PUT_HUGE)

    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqGetAllFresh, TSimple3Put1SeqGetAll, TFastVDiskSetup, false, 0, DG_3PUT_HUGE)
    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqGetAllCompaction, TSimple3Put1SeqGetAll, TFastVDiskSetupCompacted, true, 0, DG_3PUT_HUGE)

    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqGet2Fresh, TSimple3Put1SeqGet2, TFastVDiskSetup, false, 0, DG_3PUT_HUGE)
    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqGet2Compaction, TSimple3Put1SeqGet2, TFastVDiskSetupCompacted, true, 0, DG_3PUT_HUGE)

    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqSubsOkFresh, TSimple3Put1SeqSubsOk, TFastVDiskSetup, false, 0, DG_3PUT_HUGE)
    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqSubsOkCompaction, TSimple3Put1SeqSubsOk, TFastVDiskSetupCompacted, true, 0, DG_3PUT_HUGE)

    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqSubsErrorFresh, TSimple3Put1SeqSubsError, TFastVDiskSetup, false, 0, DG_3PUT_HUGE)
    Y_UNIT_TEST_COMP_DISK(Simple3Put1SeqSubsErrorCompaction, TSimple3Put1SeqSubsError, TFastVDiskSetupCompacted, true, 0, DG_3PUT_HUGE)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// SIMPLE BS
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsVDiskExtremeHandoff) {
    Y_UNIT_TEST_COMP_DISK(SimpleHnd6Put1SeqGetFresh, TSimpleHnd6Put1SeqGet, TFastVDiskSetupHndOff, false, 7, DB_3PUT2HND)
    Y_UNIT_TEST_COMP_DISK(SimpleHnd6Put1SeqGetCompaction, TSimpleHnd6Put1SeqGet, TFastVDiskSetupCompactedHndOff, true, 7, DB_3PUT2HND)

    Y_UNIT_TEST_COMP_DISK(SimpleHnd2Put1GetFresh, TSimpleHnd2Put1Get, TFastVDiskSetupHndOff, false, 7, DB_1PUT2HND)
    Y_UNIT_TEST_COMP_DISK(SimpleHnd2Put1GetCompaction, TSimpleHnd2Put1Get, TFastVDiskSetupCompactedHndOff, true, 7, DB_1PUT2HND)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// SIMPLE BS
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsVDiskExtremeHandoffHuge) {

    Y_UNIT_TEST_COMP_DISK(SimpleHndPut1SeqGetFresh, TSimpleHnd6Put1SeqGet, TFastVDiskSetupHndOff, false, 7, DB_3PUT2HND_HUGE)
    // FIXME: turn this test on after implementing handoffs for huge blobs
    //Y_UNIT_TEST_COMP_DISK(SimpleHndPut1SeqGetCompaction, TSimpleHnd6Put1SeqGet, TFastVDiskSetupCompactedHndOff, true, 7, DB_3PUT2HND_HUGE)

    Y_UNIT_TEST_COMP_DISK(SimpleHnd2Put1GetFresh, TSimpleHnd2Put1Get, TFastVDiskSetupHndOff, false, 7, DB_1PUT2HND_HUGE)
    Y_UNIT_TEST_COMP_DISK(SimpleHnd2Put1GetCompaction, TSimpleHnd2Put1Get, TFastVDiskSetupCompactedHndOff, true, 7, DB_1PUT2HND_HUGE)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// RANGE BS
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsVDiskRange) {

    Y_UNIT_TEST(RangeGetFromEmptyDB) {
        TRangeGetFromEmptyDB test;
        TestRun<TRangeGetFromEmptyDB, TFastVDiskSetup>(&test, TIMEOUT);
    }

    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetAllForwardFresh, TSimple3PutRangeGetAllForward,
                               TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetAllForwardCompaction, TSimple3PutRangeGetAllForward,
                               TFastVDiskSetupCompacted, true, 0, DG_3PUT)

    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetAllBackwardFresh, TSimple3PutRangeGetAllBackward,
                               TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetAllBackwardCompaction, TSimple3PutRangeGetAllBackward,
                               TFastVDiskSetupCompacted, true, 0, DG_3PUT)

    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetNothingForwardFresh, TSimple3PutRangeGetNothingForward,
                               TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetNothingForwardCompaction, TSimple3PutRangeGetNothingForward,
                               TFastVDiskSetupCompacted, true, 0, DG_3PUT)

    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetNothingBackwardFresh, TSimple3PutRangeGetNothingBackward,
                               TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetNothingBackwardCompaction, TSimple3PutRangeGetNothingBackward,
                               TFastVDiskSetupCompacted, true, 0, DG_3PUT)

    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetMiddleForwardFresh, TSimple3PutRangeGetMiddleForward,
                               TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetMiddleForwardCompaction, TSimple3PutRangeGetMiddleForward,
                               TFastVDiskSetupCompacted, true, 0, DG_3PUT)

    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetMiddleBackwardFresh, TSimple3PutRangeGetMiddleBackward,
                               TFastVDiskSetup, false, 0, DG_3PUT)
    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetMiddleBackwardCompaction, TSimple3PutRangeGetMiddleBackward,
                               TFastVDiskSetupCompacted, true, 0, DG_3PUT)

    // FIXME: We need a test when we read from VDisk that got its data from other VDisk during syncing
    // FIXME: index only or with data
}


///////////////////////////////////////////////////////////////////////////////////////////////////////
// RANGE BS
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsVDiskRangeHuge) {

    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetAllForwardFresh, TSimple3PutRangeGetAllForward,
                               TFastVDiskSetup, false, 0, DG_3PUT_HUGE)
    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetAllForwardCompaction, TSimple3PutRangeGetAllForward,
                               TFastVDiskSetupCompacted, true, 0, DG_3PUT_HUGE)

    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetAllBackwardFresh, TSimple3PutRangeGetAllBackward,
                               TFastVDiskSetup, false, 0, DG_3PUT_HUGE)
    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetAllBackwardCompaction, TSimple3PutRangeGetAllBackward,
                               TFastVDiskSetupCompacted, true, 0, DG_3PUT_HUGE)

    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetNothingForwardFresh, TSimple3PutRangeGetNothingForward,
                               TFastVDiskSetup, false, 0, DG_3PUT_HUGE)
    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetNothingForwardCompaction, TSimple3PutRangeGetNothingForward,
                               TFastVDiskSetupCompacted, true, 0, DG_3PUT_HUGE)

    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetNothingBackwardFresh, TSimple3PutRangeGetNothingBackward,
                               TFastVDiskSetup, false, 0, DG_3PUT_HUGE)
    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetNothingBackwardCompaction, TSimple3PutRangeGetNothingBackward,
                               TFastVDiskSetupCompacted, true, 0, DG_3PUT_HUGE)

    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetMiddleForwardFresh, TSimple3PutRangeGetMiddleForward,
                               TFastVDiskSetup, false, 0, DG_3PUT_HUGE)
    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetMiddleForwardCompaction, TSimple3PutRangeGetMiddleForward,
                               TFastVDiskSetupCompacted, true, 0, DG_3PUT_HUGE)

    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetMiddleBackwardFresh, TSimple3PutRangeGetMiddleBackward,
                               TFastVDiskSetup, false, 0, DG_3PUT_HUGE)
    Y_UNIT_TEST_COMP_DISK(Simple3PutRangeGetMiddleBackwardCompaction, TSimple3PutRangeGetMiddleBackward,
                               TFastVDiskSetupCompacted, true, 0, DG_3PUT_HUGE)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// MANY REQUESTS BS
///////////////////////////////////////////////////////////////////////////////////////////////////////

#define SMALL_TEST

#ifdef SMALL_TEST
#define LARGE_MSG_NUM   500
#define MIDDLE_MSG_NUM  100
#define SMALL_MSG_NUM   10
#else
#define LARGE_MSG_NUM   5000
#define MIDDLE_MSG_NUM  1000
#define SMALL_MSG_NUM   100
#endif

Y_UNIT_TEST_SUITE(TBsVDiskManyPutGetCheckSize) {
    Y_UNIT_TEST(ManyPutGetCheckSize) {
        std::shared_ptr<TVector<TMsgPackInfo>> msgPacks(std::unique_ptr<TVector<TMsgPackInfo>>(new TVector<TMsgPackInfo>{
            TMsgPackInfo(100'000, 672),
            TMsgPackInfo(17'026, 1)
        }));
        TManyPutOneGet testOk(false, msgPacks, UNK, DefaultTestTabletId, 257, false);
        TestRun<TManyPutOneGet, TFastVDiskSetupHndOff>(&testOk, TDuration::Minutes(100), DefChunkSize, DefDiskSize,
                1, 1, NKikimr::TErasureType::ErasureNone);
        std::shared_ptr<TVector<TMsgPackInfo>> failMsgPacks(std::unique_ptr<TVector<TMsgPackInfo>>(new TVector<TMsgPackInfo>{
            TMsgPackInfo(100'000, 672),
            TMsgPackInfo(17'027, 1)
        }));
        TManyPutOneGet testError(false, failMsgPacks, UNK, DefaultTestTabletId, 257, true);
        TestRun<TManyPutOneGet, TFastVDiskSetupHndOff>(&testError, TDuration::Minutes(100), DefChunkSize, DefDiskSize,
                1, 1, NKikimr::TErasureType::ErasureNone);
    }
}
Y_UNIT_TEST_SUITE(TBsVDiskManyPutGet) {
    //// Group
    Y_UNIT_TEST(ManyPutGet) {
        TManyPutGet test(false, LARGE_MSG_NUM, 100, UNK);
        TestRun<TManyPutGet, TFastVDiskSetupHndOff>(&test, TDuration::Minutes(100));
    }

    Y_UNIT_TEST(ManyMultiSinglePutGet) {
        TManyMultiPutGet test(false, LARGE_MSG_NUM, 100, 1, UNK);
        TestRun<TManyMultiPutGet, TFastVDiskSetupHndOff>(&test, TDuration::Minutes(100));
    }

    Y_UNIT_TEST(ManyMultiPutGet) {
        TManyMultiPutGet test(false, LARGE_MSG_NUM, 100, 2, UNK);
        TestRun<TManyMultiPutGet, TFastVDiskSetupHndOff>(&test, TDuration::Minutes(100));
    }

    Y_UNIT_TEST(ManyMultiPutGetWithLargeBatch) {
        TManyMultiPutGet test(false, LARGE_MSG_NUM, 100, 16, UNK);
        TestRun<TManyMultiPutGet, TFastVDiskSetupHndOff>(&test, TDuration::Minutes(100));
    }

    Y_UNIT_TEST(ManyPutGetWaitCompaction) {
        TManyPutGet test(true, SMALL_MSG_NUM, 100, UNK);
        TestRun<TManyPutGet, TFastVDiskSetupHndOff>(&test, TDuration::Minutes(100));
    }

    //// Group
    Y_UNIT_TEST(ManyPutRangeGetFreshIndexOnly) {
        TManyPutRangeGet test(false, true, MIDDLE_MSG_NUM, 100, UNK);
        TestRun<TManyPutRangeGet, TFastVDiskSetup>(&test, TDuration::Minutes(100));
    }

    Y_UNIT_TEST(ManyPutRangeGetCompactionIndexOnly) {
        TManyPutRangeGet test(true, true, MIDDLE_MSG_NUM, 100, UNK);
        TestRun<TManyPutRangeGet, TFastVDiskSetupCompacted>(&test, TDuration::Minutes(100));
    }

    /// Group (read is mixed, fresh and compacted)
    Y_UNIT_TEST(ManyPutRangeGet2ChannelsIndexOnly) {
        TManyPutRangeGet2Channels test(false, true, LARGE_MSG_NUM, 100, UNK);
        TestRun<TManyPutRangeGet2Channels, TFastVDiskSetup>(&test, TDuration::Minutes(100));
    }
}


///////////////////////////////////////////////////////////////////////////////////////////////////////
// GC
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsVDiskGC) {
    Y_UNIT_TEST(GCPutKeepIntoEmptyDB) {
        TGCPutKeepIntoEmptyDB test;
        TestRun<TGCPutKeepIntoEmptyDB, TFastVDiskSetupCompacted>(&test, TIMEOUT);
    } // FIXME: Fresh???

    Y_UNIT_TEST(GCPutBarrierVDisk0NoSync) {
        TGCPutBarrierVDisk0 test;
        TestRun<TGCPutBarrierVDisk0, TFastCompactionGCNoSyncVDiskSetup>(&test, TIMEOUT);
    }

    Y_UNIT_TEST(GCPutBarrierSync) {
        TGCPutBarrier test;
        TestRun<TGCPutBarrier, TFastVDiskSetupCompacted>(&test, TIMEOUT);
    }

    Y_UNIT_TEST(GCPutKeepBarrierSync) {
        TGCPutKeepBarrier test;
        TestRun<TGCPutKeepBarrier, TFastVDiskSetupCompacted>(&test, TIMEOUT);
    }

    Y_UNIT_TEST(GCPutManyBarriersNoSync) {
        TGCPutManyBarriers test;
        TestRun<TGCPutManyBarriers, TFastCompactionGCNoSyncVDiskSetup>(&test, TIMEOUT);
    }

    Y_UNIT_TEST(TGCManyVPutsCompactGCAllTest) {
        TGCManyVPutsCompactGCAll test;
        TestRun<TGCManyVPutsCompactGCAll, TFastVDiskSetupCompacted>(&test, TIMEOUT);
    }

    Y_UNIT_TEST(TGCManyVPutsDelTabletTest) {
        TGCManyVPutsDelTablet test;
        TestRun<TGCManyVPutsDelTablet, TFastVDiskSetupCompacted>(&test, TIMEOUT);
    }

    // TODO: read barrier
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
// DEFRAG
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsVDiskDefrag) {
    Y_UNIT_TEST(DefragEmptyDB) {
        TDefragEmptyDB test;
        TestRun<TDefragEmptyDB, TDefragVDiskSetup>(&test, TIMEOUT);
    }

    Y_UNIT_TEST(Defrag50PercentGarbage) {
        TDefrag50PercentGarbage test;
        TestRun<TDefrag50PercentGarbage, TDefragVDiskSetup>(&test, TIMEOUT);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// OUT OF SPACE
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsVDiskBrokenPDisk) {
    // We test how VDisk handles death of device

    Y_UNIT_TEST(WriteUntilDeviceDeath) {
        TWriteUntilDeviceDeath test;
        TestRun<TWriteUntilDeviceDeath, TDefaultVDiskSetup>(&test, TIMEOUT);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Huge
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsHuge) {
    Y_UNIT_TEST(Simple) {
        THugeModuleTest test;
        TestRun<THugeModuleTest, TFastVDiskSetupCompacted>(&test, TIMEOUT);
    }

    Y_UNIT_TEST(SimpleErasureNone) {
        THugeModuleTest test;
        TestRun<THugeModuleTest, TFastVDiskSetupCompacted>(&test,
                                                           TIMEOUT,
                                                           DefChunkSize,
                                                           DefDiskSize,
                                                           DefDomainsNum,
                                                           DefDisksInDomain,
                                                           NKikimr::TErasureType::ErasureNone);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Local Recovery
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsLocalRecovery) {
    Y_UNIT_TEST(StartStopNotEmptyDB) {
        const ui32 numIterations = NValgrind::PlainOrUnderValgrind(10, 2);
        TConfiguration Conf;
        TFastVDiskSetup vdiskSetup;
        Conf.Prepare(&vdiskSetup);

        LOG_NOTICE(*Conf.ActorSystem1, NActorsServices::TEST, "====================== Initial step");
        TCheckDbIsEmptyManyPutGet test(true, false, MIDDLE_MSG_NUM, 100, UNK); // DB must be empty
        bool success1 = Conf.Run<TCheckDbIsEmptyManyPutGet>(&test, TIMEOUT);
        UNIT_ASSERT(success1);
        Conf.Shutdown();

        for (unsigned i = 0; i < numIterations; i++) {
            Conf.Prepare(&vdiskSetup, false);
            LOG_NOTICE(*Conf.ActorSystem1, NActorsServices::TEST, "====================== Iteration %u", i);
            TCheckDbIsEmptyManyPutGet test(false, false, 1000, 100, UNK); // DB must no be empty
            bool success2 = Conf.Run<TCheckDbIsEmptyManyPutGet>(&test, TIMEOUT);
            UNIT_ASSERT(success2);
            Conf.Shutdown();
        }
    }

    Y_UNIT_TEST(WriteRestartRead) {
        auto vdiskSetup = std::make_shared<TFastVDiskSetup>();
        auto settings = TWriteRestartReadSettings::OneSetup(1000, 10, UNK, vdiskSetup);
        WriteRestartRead(settings, TIMEOUT);
    }

    Y_UNIT_TEST(WriteRestartReadHuge) {
        auto vdiskSetup = std::make_shared<TFastVDiskSetup>();
        auto settings = TWriteRestartReadSettings::OneSetup(1000, 65u << 10u, HUGEB, vdiskSetup);
        WriteRestartRead(settings, TIMEOUT);
    }

    Y_UNIT_TEST(WriteRestartReadHugeIncreased) {
        auto vdiskWriteSetup = std::make_shared<TFastVDiskSetupMinHugeBlob>(8u << 10u);
        auto vdiskReadSetup = std::make_shared<TFastVDiskSetupMinHugeBlob>(60u << 10u);
        auto settings = TWriteRestartReadSettings(1000, 20u << 10u, HUGEB, vdiskWriteSetup, vdiskReadSetup);
        WriteRestartRead(settings, TIMEOUT);
    }

    Y_UNIT_TEST(WriteRestartReadHugeDecreased) {
        auto vdiskWriteSetup = std::make_shared<TFastVDiskSetupMinHugeBlob>(60 << 10u);
        auto vdiskReadSetup = std::make_shared<TFastVDiskSetupMinHugeBlob>(8u << 10u);
        auto settings = TWriteRestartReadSettings(1000, 20u << 10u, HUGEB, vdiskWriteSetup, vdiskReadSetup);
        WriteRestartRead(settings, TIMEOUT);
    }

    Y_UNIT_TEST(MultiPutWriteRestartRead) {
        auto vdiskSetup = std::make_shared<TFastVDiskSetup>();
        auto settings = TMultiPutWriteRestartReadSettings::OneSetup(1000, 10, 10, UNK, vdiskSetup);
        MultiPutWriteRestartRead(settings, TIMEOUT);
    }

    Y_UNIT_TEST(MultiPutWriteRestartReadHuge) {
        auto vdiskSetup = std::make_shared<TFastVDiskSetup>();
        auto settings = TMultiPutWriteRestartReadSettings::OneSetup(1000, 10, 65u << 10u, HUGEB, vdiskSetup);
        MultiPutWriteRestartRead(settings, TIMEOUT);
    }

    /* FIXME: working on new tests on changing huge blobs map
    struct TWriteVDiskSetup : public TFastVDiskSetup {
        bool SetUp(TAllVDisks::TVDiskInstance &vdisk, TAllPDisks *pdisks, ui32 id, ui32 d, ui32 j, ui32 pDiskID,
                ui32 slotId, bool runRepl, ui64 initOwnerRound) {
            TFastVDiskSetup::SetUp(vdisk, pdisks, id, d, j, pDiskID, slotId, runRepl, initOwnerRound);
            TIntrusivePtr<NKikimr::TVDiskConfig> &vDiskConfig = vdisk.Cfg;

            vDiskConfig->MaxLogoBlobDataSize = 128u << 10u;
            vDiskConfig->MinHugeBlobInBytes = 64u << 10u;
            vDiskConfig->MilestoneHugeBlobInBytes = 64u << 10u;
            return true;
        }
    };
    struct TReadVDiskSetup : public TFastVDiskSetup {
        bool SetUp(TAllVDisks::TVDiskInstance &vdisk, TAllPDisks *pdisks, ui32 id, ui32 d, ui32 j, ui32 pDiskID,
                ui32 slotId, bool runRepl, ui64 initOwnerRound) {
            TFastVDiskSetup::SetUp(vdisk, pdisks, id, d, j, pDiskID, slotId, runRepl, initOwnerRound);
            TIntrusivePtr<NKikimr::TVDiskConfig> &vDiskConfig = vdisk.Cfg;

            vDiskConfig->MaxLogoBlobDataSize = 128u << 10u;
            vDiskConfig->MinHugeBlobInBytes = 32u << 10u;
            vDiskConfig->MilestoneHugeBlobInBytes = 64u << 10u;
            return true;
        }
    };

    Y_UNIT_TEST(SmallerHugeBlobs) {
        auto vdiskWriteSetup = std::make_shared<TWriteVDiskSetup>();
        auto vdiskReadSetup = std::make_shared <TReadVDiskSetup>();
        TWriteRestartReadSettings settings(1000, 66u << 10u, HUGEB, vdiskWriteSetup, vdiskReadSetup);
        WriteRestartRead(settings, TIMEOUT);
    }
    */

    Y_UNIT_TEST(ChaoticWriteRestart) {
        auto vdiskSetup = std::make_shared<TFastVDiskSetup>();
        TChaoticWriteRestartWriteSettings settings(
            TWriteRestartReadSettings::OneSetup(100000, 10, UNK, vdiskSetup),
            10,
            TDuration::Seconds(10),
            TDuration::Seconds(0));
        ChaoticWriteRestartWrite(settings, TIMEOUT);
    }

    Y_UNIT_TEST(ChaoticWriteRestartHuge) {
        return; // KIKIMR-5314
        auto vdiskSetup = std::make_shared<TFastVDiskSetup>();
        TChaoticWriteRestartWriteSettings settings(
            TWriteRestartReadSettings::OneSetup(100000, 65u << 10u, HUGEB, vdiskSetup),
            10,
            TDuration::Seconds(10),
            TDuration::Seconds(0));
        ChaoticWriteRestartWrite(settings, TIMEOUT);
    }

    Y_UNIT_TEST(ChaoticWriteRestartHugeXXX) {
        auto vdiskSetup = std::make_shared<TFastVDiskSetup>();
        TChaoticWriteRestartWriteSettings settings(
            TWriteRestartReadSettings::OneSetup(300, 65u << 10u, HUGEB, vdiskSetup),
            500,
            TDuration::Seconds(10),
            TDuration::Seconds(0));
        ChaoticWriteRestartWrite(settings, TIMEOUT);
    }

    Y_UNIT_TEST(ChaoticWriteRestartHugeIncreased) {
        auto vdiskSetup = std::make_shared<TFastVDiskSetupMinHugeBlob>(8u << 10u);
        auto vdiskSecondSetup = std::make_shared<TFastVDiskSetupMinHugeBlob>(60u << 10u);
        TChaoticWriteRestartWriteSettings settings(
            TWriteRestartReadSettings::OneSetup(300, 20u << 10u, HUGEB, vdiskSetup),
            vdiskSecondSetup,
            500,
            TDuration::Seconds(10),
            TDuration::Seconds(0));
        ChaoticWriteRestartWrite(settings, TIMEOUT);
    }

    Y_UNIT_TEST(ChaoticWriteRestartHugeDecreased) {
        auto vdiskSetup = std::make_shared<TFastVDiskSetupMinHugeBlob>(60u << 10u);
        auto vdiskSecondSetup = std::make_shared<TFastVDiskSetupMinHugeBlob>(8u << 10u);
        TChaoticWriteRestartWriteSettings settings(
            TWriteRestartReadSettings::OneSetup(300, 20u << 10u, HUGEB, vdiskSetup),
            vdiskSecondSetup,
            500,
            TDuration::Seconds(10),
            TDuration::Seconds(0));
        ChaoticWriteRestartWrite(settings, TIMEOUT);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Other
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsOther1) {

    Y_UNIT_TEST(PoisonPill) {
        TManyPutRangeGet2Channels test(false, true, LARGE_MSG_NUM, 100, UNK);
        TConfiguration Conf;
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
        TConfiguration Conf;
        TSetup vdiskSetup;
        Conf.Prepare(&vdiskSetup);

        TChaoticManyPutsTest w(parallel, msgNum, msgSize, cls, workingTime, requestTimeout);
        bool success1 = Conf.Run<TChaoticManyPutsTest>(&w, TDuration::Seconds(600));
        LOG_NOTICE(*Conf.ActorSystem1, NActorsServices::TEST, "Chaotic write done");
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

        TConfiguration Conf;
        TFastVDiskSetup vdiskSetup;
        Conf.Prepare(&vdiskSetup);

        TChaoticManyPutsTest w(parallel, msgNum, msgSize, cls, workingTime, requestTimeout);
        bool success1 = Conf.Run<TChaoticManyPutsTest>(&w, TDuration::Seconds(600));
        LOG_NOTICE(*Conf.ActorSystem1, NActorsServices::TEST, "Chaotic write done");
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

///////////////////////////////////////////////////////////////////////////////////////////////////////
// Sync/Repl
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsVDiskRepl1) {
    Y_UNIT_TEST(ReplProxyData) {
        TTestReplProxyData test;
        TestRun<TTestReplProxyData, TFastVDiskSetupCompacted>(&test, TIMEOUT);
    }

    Y_UNIT_TEST(ReplProxyKeepBits) {
        TTestReplProxyKeepBits test;
        TestRun<TTestReplProxyKeepBits, TFastVDiskSetupCompacted>(&test, TIMEOUT);
    }


    Y_UNIT_TEST(ReplEraseDiskRestore) {
        TSmallCommonDataSet dataSet;
        ui32 domainsNum = 4u;
        ui32 disksInDomain = 2u;
        ui32 pDisksNum = domainsNum * disksInDomain;
        TConfiguration Conf(TAllPDisksConfiguration::MkManyTmp(pDisksNum, 512u << 10u, 16ull << 30ull, "ROT"),
                            domainsNum, disksInDomain);
        TFastVDiskSetup vdiskSetup;
        Conf.Prepare(&vdiskSetup);
        TTestReplDataWriteAndSync testLoad(&dataSet);
        bool success1 = Conf.Run<TTestReplDataWriteAndSync>(&testLoad, TIMEOUT);
        UNIT_ASSERT(success1);
        Conf.Shutdown();
        Conf.PDisks->EraseDisk(3, 678);
        Conf.Prepare(&vdiskSetup, false);
        TReadUntilSuccess testRead(&dataSet, 3, SMALL_TIMEOUT);
        bool success2 = Conf.Run<TReadUntilSuccess>(&testRead, TIMEOUT);
        Conf.Shutdown();
        UNIT_ASSERT(success2);
    }
}

Y_UNIT_TEST_SUITE(TBsVDiskRepl2) {
    Y_UNIT_TEST(ReplEraseDiskRestoreWOOneDisk) {
        TSmallCommonDataSet dataSet;
        ui32 domainsNum = 4u;
        ui32 disksInDomain = 2u;
        ui32 pDisksNum = domainsNum * disksInDomain;
        TConfiguration Conf(TAllPDisksConfiguration::MkManyTmp(pDisksNum, 512u << 10u, 16ull << 30ull, "ROT"),
                            domainsNum, disksInDomain);
        TFastVDiskSetup vdiskSetup;
        Conf.Prepare(&vdiskSetup);
        TTestReplDataWriteAndSync testLoad(&dataSet);
        bool success1 = Conf.Run<TTestReplDataWriteAndSync>(&testLoad, TIMEOUT);
        UNIT_ASSERT(success1);
        Conf.Shutdown();
        Conf.PDisks->EraseDisk(3, 678);
        TFastVDiskSetupWODisk2 vdiskSetupWODisk2;
        Conf.Prepare(&vdiskSetupWODisk2, false);
        TReadUntilSuccess testRead(&dataSet, 3, SMALL_TIMEOUT);
        bool success2 = Conf.Run<TReadUntilSuccess>(&testRead, TIMEOUT);
        Conf.Shutdown();
        UNIT_ASSERT(success2);
    }
}

Y_UNIT_TEST_SUITE(TBsVDiskRepl3) {
    Y_UNIT_TEST(ReplEraseDiskRestoreMultipart) {
        TSmallCommonDataSet dataSet;
        ui32 domainsNum = 4u;
        ui32 disksInDomain = 1u;
        ui32 pDisksNum = domainsNum * disksInDomain;
        TConfiguration Conf(TAllPDisksConfiguration::MkManyTmp(pDisksNum, 512u << 10u, 16ull << 30ull, "ROT"),
                            domainsNum, disksInDomain);
        TFastVDiskSetup vdiskSetup;
        Conf.Prepare(&vdiskSetup);
        TTestReplDataWriteAndSyncMultipart testLoad(&dataSet);
        bool success1 = Conf.Run<TTestReplDataWriteAndSyncMultipart>(&testLoad, TIMEOUT);
        UNIT_ASSERT(success1);
        Conf.Shutdown();
        Conf.PDisks->EraseDisk(3, 678);
        Conf.Prepare(&vdiskSetup, false);
        TReadUntilSuccess testRead(&dataSet, 3, SMALL_TIMEOUT, true);
        bool success2 = Conf.Run<TReadUntilSuccess>(&testRead, TIMEOUT);
        Conf.Shutdown();
        UNIT_ASSERT(success2);
    }

    Y_UNIT_TEST(AnubisTest) {
        // ignored
        return;

        const TSmallCommonDataSet dataSet;
        ui32 domainsNum = 4u;
        ui32 disksInDomain = 1u;
        ui32 pDisksNum = domainsNum * disksInDomain;
        TConfiguration Conf(TAllPDisksConfiguration::MkManyTmp(pDisksNum, 512u << 10u, 1ull << 30ull, "ROT"),
                            domainsNum, disksInDomain);
        // Write some data to each disk
        {
            TFastVDiskSetup vdiskSetup;
            Conf.Prepare(&vdiskSetup);
            TTestReplDataWriteAndSyncMultipart testLoad(&dataSet);
            bool success1 = Conf.Run<TTestReplDataWriteAndSyncMultipart>(&testLoad, TIMEOUT);
            UNIT_ASSERT(success1);
            Conf.Shutdown();
        }

        // Write GC commands to all disks except one (none-working)
        {
            TFastVDiskSetupWODisk2Compacted vdiskSetupWODisk2;
            Conf.Prepare(&vdiskSetupWODisk2, false);
            TTestCollectAllSimpleDataset testGC;
            bool success2 = Conf.Run<TTestCollectAllSimpleDataset>(&testGC, TIMEOUT);
            Conf.Shutdown();
            UNIT_ASSERT(success2);
        }

        // Restart group to see how Anubis works
        {
            TFastVDiskSetup vdiskSetup;
            Conf.Prepare(&vdiskSetup, false);
            TTestStub testLoad;
            bool success1 = Conf.Run<TTestStub>(&testLoad, TIMEOUT);
            UNIT_ASSERT(success1);
            Conf.Shutdown();
            // FIXME:
            // 1. Flush SyncLog for all alived disks
            // 2. Run ALL disks and see how Anubis works
        }
    }

    Y_UNIT_TEST(ReplPerf) {
        const ui64 maxDataSize = 10 << 20;
        const ui32 maxBlobs = 2000; // FIXME: set up -1
        const ui32 minBlobSize = 128;
        const ui32 maxBlobSize = 4096;

        TAutoPtr<IDataGenerator> generator;
        TAutoPtr<TGeneratedDataSet> dataSetPtr;

        ui32 domainsNum = 4u;
        ui32 disksInDomain = 2u;
        ui32 pDisksNum = domainsNum * disksInDomain;
        TConfiguration Conf(TAllPDisksConfiguration::MkManyTmp(pDisksNum, 16u << 20u, 16ull << 30ull, "ROT"),
                            domainsNum, disksInDomain);
        TFastVDiskSetupRepl vdiskSetup;
        Conf.Prepare(&vdiskSetup);

        const ui32 erasedPDiskID = 4;

        TVector<NKikimr::TVDiskID> vdisks;
        for (ui32 i = 0; i < Conf.VDisks->GetSize(); ++i) {
            auto& vDisk = Conf.VDisks->Get(i);
            if (vDisk.Cfg->BaseInfo.PDiskId == erasedPDiskID) {
                vdisks.push_back(vDisk.VDiskID);
            }
        }

        LOG_NOTICE(*Conf.ActorSystem1, NActorsServices::TEST, "starting writer");
        generator.Reset(CreateBlobGenerator(maxDataSize, maxBlobs, minBlobSize, maxBlobSize, 0, 1,
                Conf.GroupInfo, vdisks));
        dataSetPtr.Reset(new TGeneratedDataSet(generator));
        TTestReplDataWriteAndSync testLoad(dataSetPtr.Get());
        bool success1 = Conf.Run<TTestReplDataWriteAndSync>(&testLoad, TIMEOUT);
        UNIT_ASSERT(success1);
        LOG_NOTICE(*Conf.ActorSystem1, NActorsServices::TEST, "stopped writer");
        Conf.Shutdown();

        Conf.PDisks->EraseDisk(3, 678);

        Conf.Prepare(&vdiskSetup, false);

        generator.Reset(CreateBlobGenerator(maxDataSize, maxBlobs, minBlobSize, maxBlobSize, 0, 1,
                Conf.GroupInfo, vdisks));
        dataSetPtr.Reset(new TGeneratedDataSet(generator));
        LOG_NOTICE(*Conf.ActorSystem1, NActorsServices::TEST, "starting first read pass");
        TReadUntilSuccess testRead(dataSetPtr.Get(), 3, SMALL_TIMEOUT);
        TInstant begin = Now();
        bool success2 = Conf.Run<TReadUntilSuccess>(&testRead, TIMEOUT);
        TInstant end = Now();
        TDuration timedelta = end - begin;
        Conf.Shutdown();
        UNIT_ASSERT(success2);

        Conf.Prepare(&vdiskSetup, false, false); // do not run replication here, all data must already be in VDisk

        generator.Reset(CreateBlobGenerator(maxDataSize, maxBlobs, minBlobSize, maxBlobSize, 0, 1,
                Conf.GroupInfo, vdisks));
        dataSetPtr.Reset(new TGeneratedDataSet(generator));

        LOG_NOTICE_S(*Conf.ActorSystem1, NActorsServices::TEST, "first read pass w/repl took " << timedelta.ToString().data());
        LOG_NOTICE(*Conf.ActorSystem1, NActorsServices::TEST, "starting second read pass");
        TReadUntilSuccess verifyRead(dataSetPtr.Get(), 3, SMALL_TIMEOUT);
        begin = Now();
        bool success3 = Conf.Run<TReadUntilSuccess>(&verifyRead, TIMEOUT);
        end = Now();
        timedelta = end - begin;
        Conf.Shutdown();
        UNIT_ASSERT(success3);
    }

    Y_UNIT_TEST(SyncLogTest) {
        TConfiguration Conf;
        TNoVDiskSetup vdiskSetup;
        Conf.Prepare(&vdiskSetup);
        TSyncLogTestWrite test;
        bool success1 = Conf.Run<TSyncLogTestWrite>(&test, TIMEOUT, 1, false);
        Conf.Shutdown();
        UNIT_ASSERT(success1);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////
// BAD BLOBID
///////////////////////////////////////////////////////////////////////////////////////////////////////
Y_UNIT_TEST_SUITE(TBsVDiskBadBlobId) {
    // Ensure that putting blob with bad id results in ERROR status

    Y_UNIT_TEST(PutBlobWithBadId) {
        TWriteAndExpectError test;
        TestRun<TWriteAndExpectError, TDefaultVDiskSetup>(&test, TIMEOUT);
    }
}
