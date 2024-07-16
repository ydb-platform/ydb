#include "blobstorage_synclogdata.h"
#include <ydb/core/blobstorage/vdisk/common/memusage.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

#define STR Cnull

namespace NKikimr {

    using namespace NSyncLog;

    Y_UNIT_TEST_SUITE(TBlobStorageSyncLogData) {

        TEntryPointParser MakeParser(const TString &serializedData) {
            ui64 pDiskGuid = 1234;
            ui64 chunkSize = 64 << 20;
            ui32 appendBlockSize = 4 << 10;
            ui32 syncLogAdvisedIndexedBlockSize = 4 << 10;

            ::NMonitoring::TDynamicCounters::TCounterPtr cntr(new NMonitoring::TCounterForPtr);
            TMemoryConsumer memConsumer(cntr);
            TSyncLogParams params(pDiskGuid, chunkSize, appendBlockSize, syncLogAdvisedIndexedBlockSize, memConsumer);
            TEntryPointParser parser(std::move(params));
            TString explanation;
            bool needsInitialCommit;
            bool success = parser.Parse(serializedData, needsInitialCommit, explanation);
            UNIT_ASSERT(success);
            return parser;
        }

        TSyncLogPtr CreateEmpty() {
            TEntryPointParser parser = MakeParser(TString());
            UNIT_ASSERT(parser.GetRecoveryLogConfirmedLsn() == 0);
            UNIT_ASSERT(parser.GetChunksToDelete().empty());
            return parser.GetSyncLogPtr();
        }

        TSyncLogPtr Parse(const TString &serializedData) {
            TEntryPointParser parser = MakeParser(serializedData);
            return parser.GetSyncLogPtr();
        }


        void TestEmptySyncLog(const TVector<ui32> &chunksToDeleteDelayed,
                ui64 recoveryLogConfirmedLsn)
        {
            TSyncLogPtr syncLog = CreateEmpty();

            auto syncLogSnap = syncLog->GetSnapshot();
            TVector<ui32> copyOfChunksToDeleteDelayed = chunksToDeleteDelayed;
            TEntryPointSerializer serializer(syncLogSnap, std::move(copyOfChunksToDeleteDelayed),
                recoveryLogConfirmedLsn);
            TDeltaToDiskRecLog delta(10);
            serializer.Serialize(delta);
            TString serializedData = serializer.GetSerializedData();

            auto recoveredSyncLog = Parse(serializedData);
            TEntryPointParser parser = MakeParser(serializedData);
            UNIT_ASSERT(parser.GetChunksToDelete() == chunksToDeleteDelayed);
            UNIT_ASSERT(parser.GetRecoveryLogConfirmedLsn() == recoveryLogConfirmedLsn);
        }

        Y_UNIT_TEST(SerializeParseEmpty1_Proto) {
            TVector<ui32> chunksToDeleteDelayed;
            ui64 recoveryLogConfirmedLsn = 0;
            TestEmptySyncLog(chunksToDeleteDelayed, recoveryLogConfirmedLsn);
        }

        Y_UNIT_TEST(SerializeParseEmpty2_Proto) {
            TVector<ui32> chunksToDeleteDelayed = {78, 3};
            ui64 recoveryLogConfirmedLsn = 783246;
            TestEmptySyncLog(chunksToDeleteDelayed, recoveryLogConfirmedLsn);
        }
    }

} // NKikimr
