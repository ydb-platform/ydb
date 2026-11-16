#include "blobstorage_synclogdata.h"
#include <ydb/core/blobstorage/vdisk/common/memusage.h>
#include <ydb/core/blobstorage/vdisk/synclog/phantom_flag_storage/phantom_flag_storage_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>
#include <util/system/unaligned_mem.h>

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

        Y_UNIT_TEST(EmptyDiskSpaceStat) {
            const TSyncLogDiskSpaceStat stat = CreateEmpty()->GetDiskSpaceStat();
            UNIT_ASSERT_VALUES_EQUAL(stat.ChunkSizeBytes, 64ull << 20);
            UNIT_ASSERT_VALUES_EQUAL(stat.ActiveChunkCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(stat.UsedBytes, 0);
            UNIT_ASSERT_VALUES_EQUAL(stat.FreeBytes, 0);
        }

        Y_UNIT_TEST(PhantomFlagStorageItemFraming) {
            const TLogoBlobID key(123, 4, 5, 6, 4096, 0);
            const TLogoBlobRec record(key, 0);
            const TPhantomFlagStorageItem expectedFlag = TPhantomFlagStorageItem::CreateFlag(&record);
            TString encoded;
            expectedFlag.Serialize(&encoded);
            TPhantomFlagStorageItem::CreateSkip(1).Serialize(&encoded);

            constexpr ui32 typeSize = sizeof(EPhantomFlagStorageItem);
            constexpr ui32 skipHeaderSize = typeSize + sizeof(ui32);
            constexpr ui32 unknownSize = skipHeaderSize + 2;
            TString unknown(unknownSize, 'x');
            unknown[0] = '\x7f';
            WriteUnaligned<ui32>(unknown.Detach() + typeSize, unknownSize);
            encoded.append(unknown);
            TPhantomFlagStorageItem::CreateThreshold(2, key).Serialize(&encoded);

            TStringBuf remaining(encoded);
            const std::optional<TPhantomFlagStorageItem> flag = TPhantomFlagStorageItem::ReadFrom(remaining);
            UNIT_ASSERT(flag);
            UNIT_ASSERT(flag->GetType() == EPhantomFlagStorageItem::Flag);
            UNIT_ASSERT_VALUES_EQUAL(flag->GetFlag().Record.LogoBlobID(), key);

            const std::optional<TPhantomFlagStorageItem> padding = TPhantomFlagStorageItem::ReadFrom(remaining);
            UNIT_ASSERT(padding);
            UNIT_ASSERT(padding->GetType() == EPhantomFlagStorageItem::Skip);
            UNIT_ASSERT_VALUES_EQUAL(padding->GetSkip().Size, 1);

            const std::optional<TPhantomFlagStorageItem> skipped = TPhantomFlagStorageItem::ReadFrom(remaining);
            UNIT_ASSERT(skipped);
            UNIT_ASSERT(skipped->GetType() == EPhantomFlagStorageItem::Skip);
            UNIT_ASSERT_VALUES_EQUAL(skipped->GetSkip().Size, unknownSize);

            const std::optional<TPhantomFlagStorageItem> decodedThreshold =
                TPhantomFlagStorageItem::ReadFrom(remaining);
            UNIT_ASSERT(decodedThreshold);
            UNIT_ASSERT(decodedThreshold->GetType() == EPhantomFlagStorageItem::Threshold);
            const TPhantomFlagStorageItem::TThreshold threshold = decodedThreshold->GetThreshold();
            UNIT_ASSERT_VALUES_EQUAL(threshold.TabletId, key.TabletID());
            UNIT_ASSERT_VALUES_EQUAL(threshold.Channel, key.Channel());
            UNIT_ASSERT_VALUES_EQUAL(threshold.Generation, key.Generation());
            UNIT_ASSERT_VALUES_EQUAL(threshold.Step, key.Step());
            UNIT_ASSERT_VALUES_EQUAL(threshold.OrderNumber, 2);
            UNIT_ASSERT(remaining.empty());

            const auto assertRejected = [](TStringBuf bytes) {
                TStringBuf input = bytes;
                UNIT_ASSERT(!TPhantomFlagStorageItem::ReadFrom(input));
                UNIT_ASSERT(input.data() == bytes.data());
                UNIT_ASSERT_VALUES_EQUAL(input.size(), bytes.size());
            };
            assertRejected(remaining);
            assertRejected(TStringBuf(encoded).SubStr(0, expectedFlag.SerializedSize() - 1));
            assertRejected(TStringBuf(unknown).SubStr(0, skipHeaderSize - 1));
            assertRejected(TStringBuf(unknown).SubStr(0, unknownSize - 1));

            unknown[0] = static_cast<char>(EPhantomFlagStorageItem::Skip);
            const ui32 invalidSize = skipHeaderSize - 1;
            WriteUnaligned<ui32>(unknown.Detach() + typeSize, invalidSize);
            assertRejected(unknown);
        }

        Y_UNIT_TEST(PhantomFlagStorageKeyPages) {
            const TBlobStorageGroupType groupType(TBlobStorageGroupType::Erasure4Plus2Block);
            TIngress doNotKeep;
            doNotKeep.SetKeep(TIngress::IngressMode(groupType), CollectModeDoNotKeep);
            TIngress keep;
            keep.SetKeep(TIngress::IngressMode(groupType), CollectModeKeep);

            const TLogoBlobID firstKey(123, 4, 5, 6, 4096, 1);
            const TLogoBlobID ignoredKey(123, 4, 5, 6, 4096, 2);
            const TLogoBlobID nextKey(123, 4, 5, 6, 4096, 3);
            const TLogoBlobRec first(firstKey, doNotKeep.Raw());
            const TLogoBlobRec ignored(ignoredKey, keep.Raw());
            const TLogoBlobRec next(nextKey, doNotKeep.Raw());

            TString encoded;
            TPhantomFlagStorageItem::CreateFlag(&first).Serialize(&encoded);
            TPhantomFlagStorageItem::CreateThreshold(2, firstKey).Serialize(&encoded);
            TPhantomFlagStorageItem::CreateFlag(&ignored).Serialize(&encoded);
            TPhantomFlagStorageItem::CreateSkip(1).Serialize(&encoded);
            const size_t nextOffset = encoded.size();
            TPhantomFlagStorageItem::CreateFlag(&next).Serialize(&encoded);
            TPhantomFlagStorageItem::AlignWriteBlock(&encoded, 4096, 4096);

            constexpr size_t pageKeys = 1;
            TStringBuf remaining(encoded);
            TString firstPage;
            UNIT_ASSERT(ReadPhantomFlagStorageKeys(remaining, groupType, pageKeys, firstPage));
            UNIT_ASSERT_VALUES_EQUAL(firstPage, firstKey.AsBinaryString());
            UNIT_ASSERT_VALUES_EQUAL(encoded.size() - remaining.size(), nextOffset);

            TString lastPage;
            UNIT_ASSERT(ReadPhantomFlagStorageKeys(remaining, groupType, pageKeys, lastPage));
            UNIT_ASSERT_VALUES_EQUAL(lastPage, nextKey.AsBinaryString());
            UNIT_ASSERT(remaining.empty());
        }
    }

} // NKikimr
