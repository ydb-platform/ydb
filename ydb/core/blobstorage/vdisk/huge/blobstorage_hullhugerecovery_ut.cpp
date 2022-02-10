#include "blobstorage_hullhugerecovery.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>


// change to Cerr if you want logging
#define STR Cnull


namespace NKikimr {

    using namespace NHuge;

    Y_UNIT_TEST_SUITE(TBlobStorageHullHugeRecovery) {

        Y_UNIT_TEST(LogPos) {
            ui64 chunkAllocationLsn =       48265416;
            ui64 chunkFreeingLsn =          11198354;
            ui64 hugeBlobLoggedLsn =        48767829;
            ui64 logoBlobsDbSlotDelLsn =    48613932;
            ui64 blocksDbSlotDelLsn =       45042322;
            ui64 barriersDbSlotDelLsn =     45043017;
            ui64 entryPointLsn =            48767829;
            THullHugeRecoveryLogPos logPos(
                chunkAllocationLsn,
                chunkFreeingLsn,
                hugeBlobLoggedLsn,
                logoBlobsDbSlotDelLsn,
                blocksDbSlotDelLsn,
                barriersDbSlotDelLsn,
                entryPointLsn);
            UNIT_ASSERT(logPos.FirstLsnToKeep() == 45042322);
        }

        Y_UNIT_TEST(LogTracker) {
            TLogTracker lt;

            TLogTracker::TPosition prev;
            prev.EntryPointLsn = 100;
            prev.HugeBlobLoggedLsn = 100;
            lt.EntryPointFromRecoveryLog(prev);

            prev.EntryPointLsn = 200;
            prev.HugeBlobLoggedLsn = 200;
            lt.EntryPointFromRecoveryLog(prev);

            TLogTracker::TPosition cur;
            cur.EntryPointLsn = 300;
            cur.HugeBlobLoggedLsn = 300;
            lt.EntryPointFromRecoveryLog(cur);


            lt.FinishRecovery(300);
            UNIT_ASSERT(lt.FirstLsnToKeep() == 200);



            TLogTracker::TPosition newPos;
            newPos.EntryPointLsn = 400;
            newPos.HugeBlobLoggedLsn = 400;
            lt.InitiateNewEntryPointCommit(newPos);
            lt.EntryPointCommitted(400);
            UNIT_ASSERT(lt.FirstLsnToKeep() == 300);
        }
    }

} // NKikimr
