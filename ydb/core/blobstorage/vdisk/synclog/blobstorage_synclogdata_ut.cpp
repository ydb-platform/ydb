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

        Y_UNIT_TEST(PhantomFlagStorageChunkGenerations) {
            const auto restoreSnapshot = [](const TSyncLogSnapshotPtr& snapshot) {
                TEntryPointSerializer serializer(snapshot, {}, 0);
                TDeltaToDiskRecLog delta(10);
                serializer.Serialize(delta);
                return Parse(serializer.GetSerializedData());
            };

            constexpr ui32 chunkIndex = 17;
            TPhantomFlagStorageDataProto legacyManifest;
            legacyManifest.SetChunkSize(32_MB);
            auto* legacyChunk = legacyManifest.AddChunks();
            legacyChunk->SetChunkIdx(chunkIndex);
            legacyChunk->SetDataSize(4096);

            TPhantomFlagStorageData data;
            data.Deserialize(legacyManifest);
            UNIT_ASSERT_VALUES_EQUAL(data.Chunks.at(chunkIndex).Generation, 0);
            UNIT_ASSERT_VALUES_EQUAL(data.LastChunkGeneration, 0);

            constexpr ui64 generation = 4;
            constexpr ui64 lastGeneration = 8;
            data.Chunks.at(chunkIndex).Generation = generation;
            data.LastChunkGeneration = lastGeneration;
            TPhantomFlagStorageDataProto manifest;
            data.Serialize(&manifest);
            UNIT_ASSERT_VALUES_EQUAL(manifest.GetGeneration(), lastGeneration);
            TPhantomFlagStorageData recovered;
            recovered.Deserialize(manifest);
            UNIT_ASSERT_VALUES_EQUAL(recovered.Chunks.at(chunkIndex).Generation, generation);
            UNIT_ASSERT_VALUES_EQUAL(recovered.LastChunkGeneration, lastGeneration);

            legacyChunk->SetGeneration(generation);
            TPhantomFlagStorageData beforeGlobalData;
            beforeGlobalData.Deserialize(legacyManifest);
            TSyncLogPtr syncLog = CreateEmpty();
            syncLog->UpdatePhantomFlagStorageData(std::move(beforeGlobalData));
            const TSyncLogPtr beforeGlobalGeneration = restoreSnapshot(syncLog->GetSnapshot());
            UNIT_ASSERT_VALUES_EQUAL(beforeGlobalGeneration->GetPhantomFlagStorageData().LastChunkGeneration, 0);
            UNIT_ASSERT_VALUES_EQUAL(
                beforeGlobalGeneration->GetPhantomFlagStorageData().Chunks.at(chunkIndex).Generation, generation);

            syncLog->UpdatePhantomFlagStorageData(std::move(recovered));
            const TSyncLogSnapshotPtr originalSnapshot = syncLog->GetSnapshot();
            const TSyncLogPtr original = restoreSnapshot(originalSnapshot);
            UNIT_ASSERT_VALUES_EQUAL(original->GetPhantomFlagStorageData().LastChunkGeneration, lastGeneration);
            UNIT_ASSERT_VALUES_EQUAL(original->GetPhantomFlagStorageData().Chunks.at(chunkIndex).Generation, generation);

            // Removing the manifest must preserve the global generation in the entry point.
            syncLog->UpdatePhantomFlagStorageData(std::nullopt);
            const TSyncLogSnapshotPtr withoutManifest = syncLog->GetSnapshot();
            UNIT_ASSERT(!withoutManifest->PhantomFlagStorageData);
            TSyncLogPtr afterDrop = restoreSnapshot(withoutManifest);
            UNIT_ASSERT(afterDrop->GetPhantomFlagStorageData().Chunks.empty());
            UNIT_ASSERT_VALUES_EQUAL(afterDrop->GetPhantomFlagStorageData().LastChunkGeneration, lastGeneration);
            UNIT_ASSERT_VALUES_EQUAL(afterDrop->GetPhantomFlagStorageData().ChunkSize, 64_MB);

            // Rebuilding from an older manifest cannot roll back the saved global generation.
            TPhantomFlagStorageData rebuilt = beforeGlobalGeneration->GetPhantomFlagStorageData();
            afterDrop->UpdatePhantomFlagStorageData(std::move(rebuilt));
            const TSyncLogPtr afterRebuild = restoreSnapshot(afterDrop->GetSnapshot());
            UNIT_ASSERT_VALUES_EQUAL(afterRebuild->GetPhantomFlagStorageData().LastChunkGeneration, lastGeneration);
            UNIT_ASSERT_VALUES_EQUAL(
                afterRebuild->GetPhantomFlagStorageData().Chunks.at(chunkIndex).Generation, generation);

            constexpr ui64 nextGeneration = lastGeneration + 1;
            TPhantomFlagStorageData next = afterDrop->GetPhantomFlagStorageData();
            next.LastChunkGeneration = nextGeneration;
            next.Chunks.at(chunkIndex).Generation = nextGeneration;
            syncLog->UpdatePhantomFlagStorageData(std::move(next));
            const TSyncLogPtr afterAllocation = restoreSnapshot(syncLog->GetSnapshot());
            UNIT_ASSERT_VALUES_EQUAL(afterAllocation->GetPhantomFlagStorageData().LastChunkGeneration, nextGeneration);
            const TSyncLogPtr unchangedSnapshot = restoreSnapshot(withoutManifest);
            UNIT_ASSERT_VALUES_EQUAL(unchangedSnapshot->GetPhantomFlagStorageData().LastChunkGeneration, lastGeneration);
            UNIT_ASSERT(unchangedSnapshot->GetPhantomFlagStorageData().Chunks.empty());
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

            const TPhantomFlagStorageItem firstFlag = TPhantomFlagStorageItem::CreateFlag(&first);
            const ui32 firstFlagSize = firstFlag.SerializedSize();
            TString encoded;
            firstFlag.Serialize(&encoded);
            TPhantomFlagStorageItem::CreateThreshold(2, firstKey).Serialize(&encoded);
            TPhantomFlagStorageItem::CreateFlag(&ignored).Serialize(&encoded);
            const ui32 paddingOffset = static_cast<ui32>(encoded.size());
            constexpr ui32 blockSize = 4096;
            TPhantomFlagStorageItem::AlignWriteBlock(&encoded, blockSize, blockSize);
            const ui32 nextOffset = static_cast<ui32>(encoded.size());
            TPhantomFlagStorageItem::CreateFlag(&next).Serialize(&encoded);
            const ui32 committedSize = static_cast<ui32>(encoded.size());
            const TStringBuf bytes(encoded);

            constexpr ui32 singleByte = 1;
            using TProtoKeys = google::protobuf::RepeatedPtrField<NKikimrProto::TLogoBlobID>;
            TProtoKeys firstPage;
            const std::optional<ui32> firstRead = ReadPhantomFlagStorageKeys(
                bytes.SubStr(0, firstFlagSize), singleByte, committedSize, groupType, firstPage);
            UNIT_ASSERT(firstRead);
            UNIT_ASSERT_VALUES_EQUAL(*firstRead, firstFlagSize);
            UNIT_ASSERT_VALUES_EQUAL(firstPage.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(firstPage.Get(0)), firstKey);

            // The lookahead contains the padding length but only part of the padding.
            const ui32 middleReadSize = paddingOffset - firstFlagSize + singleByte;
            const TStringBuf middleBytes = bytes.SubStr(firstFlagSize, middleReadSize + sizeof(ui32));
            const ui32 middleCommittedSize = committedSize - firstFlagSize;
            TProtoKeys middlePage;
            const std::optional<ui32> middleRead = ReadPhantomFlagStorageKeys(
                middleBytes, middleReadSize, middleCommittedSize, groupType, middlePage);
            UNIT_ASSERT(middleRead);
            UNIT_ASSERT_VALUES_EQUAL(*middleRead, nextOffset - firstFlagSize);
            UNIT_ASSERT(*middleRead > middleBytes.size());
            UNIT_ASSERT(middlePage.empty());

            TProtoKeys lastPage;
            const ui32 lastCommittedSize = committedSize - nextOffset;
            const std::optional<ui32> lastRead = ReadPhantomFlagStorageKeys(
                bytes.SubStr(nextOffset), singleByte, lastCommittedSize, groupType, lastPage);
            UNIT_ASSERT(lastRead);
            UNIT_ASSERT_VALUES_EQUAL(*lastRead, lastCommittedSize);
            UNIT_ASSERT_VALUES_EQUAL(lastPage.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(lastPage.Get(0)), nextKey);

            TProtoKeys badPage;
            const ui32 truncatedSize = nextOffset - firstFlagSize - 1;
            UNIT_ASSERT(!ReadPhantomFlagStorageKeys(
                middleBytes, middleReadSize, truncatedSize, groupType, badPage));
        }
    }

} // NKikimr
