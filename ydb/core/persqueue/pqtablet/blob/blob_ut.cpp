#include "blob_int.h"
#include "blob.h"
#include "header.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

namespace {

constexpr ui32 TEST_LEGACY_MAX_HEADER_SIZE = 32;

void SetTestMaxHeaderSize(bool enableExtendedBatchHeader) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableTopicWriteOffsetDeltaInKeys(enableExtendedBatchHeader);
    InitMaxHeaderSize(featureFlags);
}

class TTestMaxHeaderSizeGuard {
public:
    explicit TTestMaxHeaderSizeGuard(bool enableExtendedBatchHeader)
        : PreviousMaxHeaderSize(GetMaxHeaderSize())
    {
        SetTestMaxHeaderSize(enableExtendedBatchHeader);
    }

    ~TTestMaxHeaderSizeGuard() {
        SetTestMaxHeaderSize(PreviousMaxHeaderSize == 64);
    }

private:
    const ui32 PreviousMaxHeaderSize;
};

} // namespace

Y_UNIT_TEST_SUITE(BlobTest) {
    Y_UNIT_TEST(Flags_HasPartData) {
        TMessageFlags flags;

        flags.F.HasPartData = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 1);
    }

    Y_UNIT_TEST(Flags_HasWriteTimestamp) {
        TMessageFlags flags;

        flags.F.HasWriteTimestamp = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 2);
    }

    Y_UNIT_TEST(Flags_HasCreateTimestamp) {
        TMessageFlags flags;

        flags.F.HasCreateTimestamp = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 4);
    }

    Y_UNIT_TEST(Flags_HasUncompressedSize) {
        TMessageFlags flags;

        flags.F.HasUncompressedSize = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 8);
    }

    Y_UNIT_TEST(Flags_HasKinesisData) {
        TMessageFlags flags;

        flags.F.HasKinesisData = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 16);
    }
}

Y_UNIT_TEST_SUITE(BatchMemory) {
    Y_UNIT_TEST(UnpackFreesPackedData) {
        TBatch batch(0, 0);
        auto ts = TInstant::Seconds(100);
        for (ui32 i = 0; i < 10; ++i) {
            TString data(1_KB, 'a' + i);
            batch.AddBlob(TClientBlob(
                TString("src"), i + 1, std::move(data), TMaybe<TPartData>(),
                ts, ts, 0, "", ""
            ));
        }

        UNIT_ASSERT(!batch.Packed);
        UNIT_ASSERT(!batch.Header.HasClientBlobCount());
        UNIT_ASSERT(batch.GetUnpackedSize() > 0);

        batch.Pack();
        UNIT_ASSERT(batch.Packed);
        UNIT_ASSERT(!batch.Header.HasClientBlobCount());
        UNIT_ASSERT(batch.PackedData.Size() > 0);
        UNIT_ASSERT(batch.PackedData.Capacity() > 0);

        batch.Unpack();
        UNIT_ASSERT(!batch.Packed);
        UNIT_ASSERT_VALUES_EQUAL(batch.PackedData.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(batch.PackedData.Capacity(), 0);
        UNIT_ASSERT(!batch.Blobs.empty());
    }

    Y_UNIT_TEST(LegacyMaxHeaderSizePackInvariant) {
        TTestMaxHeaderSizeGuard maxHeaderSizeGuard(false);

        TBatch batch(Max<ui64>(), 0);
        auto ts = TInstant::Seconds(100);
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString(8_KB, 'a'), TMaybe<TPartData>(),
            ts, ts, 0, "", ""
        ));

        batch.Pack();
        UNIT_ASSERT_LE(batch.GetPackedSize(), batch.GetUnpackedSize() + TEST_LEGACY_MAX_HEADER_SIZE);

        batch.Unpack();
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch.GetCount(), 1u);
        UNIT_ASSERT(!batch.Header.HasClientBlobCount());
        UNIT_ASSERT(!batch.Header.HasOffsetDelta());
    }

    Y_UNIT_TEST(LegacyMaxHeaderSizePackInvariantWithKinesis) {
        TTestMaxHeaderSizeGuard maxHeaderSizeGuard(false);

        TBatch batch(Max<ui64>(), 0);
        auto ts = TInstant::Seconds(100);
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString(8_KB, 'a'), TMaybe<TPartData>(),
            ts, ts, 0, TString("partition-key"), TString("explicit-hash-key")
        ));

        batch.Pack();
        UNIT_ASSERT_LE(batch.GetPackedSize(), batch.GetUnpackedSize() + TEST_LEGACY_MAX_HEADER_SIZE);
        UNIT_ASSERT(batch.Header.HasHasKinesis());
        UNIT_ASSERT(batch.Header.GetHasKinesis());

        batch.Unpack();
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch.GetCount(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].PartitionKey, "partition-key");
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].ExplicitHashKey, "explicit-hash-key");
        UNIT_ASSERT(!batch.Header.HasClientBlobCount());
        UNIT_ASSERT(!batch.Header.HasOffsetDelta());
    }

    Y_UNIT_TEST(BatchSizePackUnpack) {
        TBatch batch(100, 0);
        auto ts = TInstant::Seconds(100);
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("data"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 5
        ));
        UNIT_ASSERT_VALUES_EQUAL(batch.GetCount(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Header.GetClientBlobCount(), 1u);

        batch.Pack();
        batch.Unpack();

        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].LogicalMessageCount, 5u);
        UNIT_ASSERT_VALUES_EQUAL(batch.GetCount(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Header.GetClientBlobCount(), 1u);
    }

    Y_UNIT_TEST(BatchSizePackUnpackWithoutUncompressedSize) {
        TBatch batch(100, 0);
        auto ts = TInstant::Seconds(100);
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString(8_KB, 'a'), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 5
        ));

        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].UncompressedSize, 0u);

        batch.Pack();
        batch.Unpack();

        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].UncompressedSize, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].LogicalMessageCount, 5u);
    }


    Y_UNIT_TEST(MessageMetadataPackUnpack) {
        TBatch batch(100, 0);
        auto ts = TInstant::Seconds(100);
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("data"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 5
        ));

        batch.Pack();
        batch.Unpack();

        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].LogicalMessageCount, 5u);
    }

    Y_UNIT_TEST(BatchHeaderOffsetDeltaRoundtrip) {
        TBatch batch(100, 0);
        const auto ts = TInstant::Seconds(100);
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("data"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 5
        ));
        batch.SetOffsetDelta(42);

        UNIT_ASSERT(batch.HasOffsetDelta());
        UNIT_ASSERT_VALUES_EQUAL(batch.GetOffsetDelta(), 42u);

        batch.Pack();
        TString serialized;
        batch.SerializeTo(serialized);

        const auto header = ExtractHeader(serialized.data(), serialized.size());
        UNIT_ASSERT(header.HasOffsetDelta());
        UNIT_ASSERT_VALUES_EQUAL(header.GetOffsetDelta(), 42u);

        batch.Unpack();
        UNIT_ASSERT(batch.HasOffsetDelta());
        UNIT_ASSERT_VALUES_EQUAL(batch.GetOffsetDelta(), 42u);
        UNIT_ASSERT(!batch.Header.HasClientBlobCount() || batch.Header.GetClientBlobCount() == 1u);
    }

    Y_UNIT_TEST(BatchFindPosWithBatchSize) {
        TBatch batch(0, 0);
        const auto ts = TInstant::Seconds(100);

        auto makeBlob = [&](ui64 seqNo, ui32 logicalMessageCount = 1) {
            return TClientBlob(
                TString("src"), seqNo, TString("data"), TMaybe<TPartData>(),
                ts, ts, 0, "", "", logicalMessageCount);
        };

        batch.AddBlob(makeBlob(1, 5));
        batch.AddBlob(makeBlob(2, 3));
        batch.AddBlob(makeBlob(3, 1));

        UNIT_ASSERT_VALUES_EQUAL(batch.GetCount(), 9u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs.size(), 3u);

        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 0).BlobIdx, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(5, 0).BlobIdx, 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(8, 0).BlobIdx, 2u);

        // Offset inside a batched slot is not a valid message boundary.
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(3, 0).BlobIdx, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(9, 0).BlobIdx, Max<ui32>());
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 1).BlobIdx, Max<ui32>());
    }

    Y_UNIT_TEST(BatchFindPosWithBatchSizeNonZeroStart) {
        TBatch batch(10, 0);
        const auto ts = TInstant::Seconds(100);

        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("data"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 5));

        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(9, 0).BlobIdx, Max<ui32>());
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(10, 0).BlobIdx, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(15, 0).BlobIdx, Max<ui32>());
    }

    Y_UNIT_TEST(BatchFindPosMultipart) {
        TBatch batch(0, 0);
        const auto ts = TInstant::Seconds(100);
        constexpr ui32 totalSize = 100;

        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("p0"), TPartData{0, 3, totalSize},
            ts, ts, totalSize, "", ""));
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("p1"), TPartData{1, 3, totalSize},
            ts, ts, totalSize, "", ""));
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("p2"), TPartData{2, 3, totalSize},
            ts, ts, totalSize, "", ""));
        batch.AddBlob(TClientBlob(
            TString("src"), 2, TString("next"), TMaybe<TPartData>(),
            ts, ts, 0, "", ""));

        UNIT_ASSERT_VALUES_EQUAL(batch.GetCount(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 0).BlobIdx, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 1).BlobIdx, 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 2).BlobIdx, 2u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(1, 0).BlobIdx, 3u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 3).BlobIdx, Max<ui32>());
    }

    Y_UNIT_TEST(BatchFindPosWithBatchSizeAndMultipart) {
        TBatch batch(0, 0);
        const auto ts = TInstant::Seconds(100);
        constexpr ui32 totalSize = 100;

        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("batch"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 5));
        batch.AddBlob(TClientBlob(
            TString("src"), 6, TString("p0"), TPartData{0, 2, totalSize},
            ts, ts, totalSize, "", ""));
        batch.AddBlob(TClientBlob(
            TString("src"), 6, TString("p1"), TPartData{1, 2, totalSize},
            ts, ts, totalSize, "", ""));
        batch.AddBlob(TClientBlob(
            TString("src"), 7, TString("next"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 3));

        UNIT_ASSERT(batch.HasOffsetDelta());
        UNIT_ASSERT_VALUES_EQUAL(batch.GetCount(), 9u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 0).BlobIdx, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(4, 0).BlobIdx, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(5, 0).BlobIdx, 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(5, 1).BlobIdx, 2u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(6, 0).BlobIdx, 3u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(8, 0).BlobIdx, 3u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(5, 2).BlobIdx, Max<ui32>());
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(9, 0).BlobIdx, Max<ui32>());
    }

    Y_UNIT_TEST(BatchFindPosSurvivesPackUnpack) {
        TBatch batch(0, 0);
        const auto ts = TInstant::Seconds(100);

        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("a"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 5));

        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 0).BlobIdx, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(1, 0).BlobIdx, 0u);

        batch.Pack();
        batch.Unpack();

        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].LogicalMessageCount, 5u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 0).BlobIdx, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(1, 0).BlobIdx, 0u);
    }
}

bool operator ==(const TClientBlob &lhs, const TClientBlob &rhs) {
    TPartData defaultPartData{0, 0, 0};

    return lhs.SourceId == rhs.SourceId &&
        lhs.SeqNo == rhs.SeqNo &&
        lhs.Data == rhs.Data &&
        lhs.GetPartNo() == rhs.GetPartNo() &&
        lhs.GetTotalParts() == rhs.GetTotalParts() &&
        lhs.GetTotalSize() == rhs.GetTotalSize() &&
        lhs.WriteTimestamp == rhs.WriteTimestamp &&
        lhs.CreateTimestamp == rhs.CreateTimestamp &&
        lhs.UncompressedSize == rhs.UncompressedSize &&
        lhs.PartitionKey == rhs.PartitionKey &&
        lhs.ExplicitHashKey == rhs.ExplicitHashKey &&
        lhs.LogicalMessageCount == rhs.LogicalMessageCount;
}

Y_UNIT_TEST_SUITE(ClientBlobSerialization) {

    TClientBlob MakeClientBlob(ui64 size, ui64 seqNo, ui64 totalParts,
                               ui16 partNo, ui64 totalSize, ui64 partKeySize,
                               ui64 explisitHashKeySize, TString sourceId) {
        Cerr << "payloadSize " << size << " totalSize " << totalSize << " partsCount " << totalParts
             << " partKeySize " << partKeySize << " hashSize " << explisitHashKeySize << " partNo " << partNo << "\n===\n";

        TString data = NUnitTest::RandomString(size);
        TMaybe<TPartData> partData;
        auto ts = TInstant::Now().Seconds();
        if (totalParts) {
          partData = TPartData{static_cast<ui16>(partNo),
                               static_cast<ui16>(totalParts),
                               static_cast<ui32>(totalSize)};
        }
        return TClientBlob{std::move(sourceId),
                           seqNo,
                           std::move(data),
                           partData,
                           TInstant::Seconds(ts),
                           TInstant::Seconds(ts + 5),
                           size ? size * 2 : 0,
                           partKeySize ? NUnitTest::RandomString(partKeySize) : TString(),
                           explisitHashKeySize
                               ? NUnitTest::RandomString(explisitHashKeySize)
                               : TString()};
    }

    Y_UNIT_TEST(EmptyPayload) {
        TBuffer buffer;
        buffer.Reserve(8_MB);

        auto blob = MakeClientBlob(0, 1, 100, 1, 100, 0, 0, "SomeSrcId");

        buffer.Clear();
        Serialize(blob, buffer);

        TClientBlob deserialized = DeserializeClientBlob(buffer.data(), buffer.size());
        UNIT_ASSERT(blob == deserialized);
    }


    Y_UNIT_TEST(MessageMetadataStoresCountInUpperBits) {
        TBuffer buffer;
        buffer.Reserve(8_MB);

        auto ts = TInstant::Seconds(100);
        TClientBlob blob(
            TString("src"), 42, TString("payload"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 7);

        Serialize(blob, buffer);

        const char* data = buffer.data();
        data += sizeof(ui32); // total size
        data += sizeof(ui64); // seqNo
        data += sizeof(ui8);  // flags
        const ui32 messageMetadata = ReadUnaligned<ui32>(data);

        UNIT_ASSERT_VALUES_EQUAL(messageMetadata, 7u << MESSAGE_METADATA_RESERVED_BITS);
    }

    Y_UNIT_TEST(SerializeAndDeserializeMessageMetadata) {
        TBuffer buffer;
        buffer.Reserve(8_MB);

        auto ts = TInstant::Seconds(100);
        TClientBlob blob(
            TString("src"), 42, TString("payload"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 7);

        Serialize(blob, buffer);
        TClientBlob deserialized = DeserializeClientBlob(buffer.data(), buffer.size());

        UNIT_ASSERT(blob == deserialized);
        UNIT_ASSERT_VALUES_EQUAL(deserialized.LogicalMessageCount, 7u);
    }

    Y_UNIT_TEST(SerializeAndDeserializeAllScenarios) {
        TVector<ui64> payloadSizes = {10, 0, 100, 512_KB};
        TVector<ui64> partsCount = {0, 1, 10, 40};
        TVector<ui64> totalSizes = {1, 0, 100, 512_KB, 1_MB, 10_MB};
        TVector<ui64> partKeySizes = {0, 100, 256};
        TVector<ui64> hashSizes = {1, 0, 100, 255};
        TVector<ui16> partNos = {0, 1, 10};
        TVector<TString> sourceIds = {"", "someSrcId"};

        ui64 seqNo = 1;
        TBuffer buffer;
        buffer.Reserve(8_MB);

        for (ui64 payloadSize : payloadSizes) {
            for (ui64 totalSize : totalSizes) {
                if (payloadSize > totalSize) {
                    continue;
                }
                for (ui64 partsCount : partsCount) {
                    if (partsCount < 2 && payloadSize < totalSize) {
                        continue;
                    }
                    for (ui16 partNo : partNos) {
                        if (partNo >= partsCount) {
                            continue;
                        }
                        for (ui64 partKeySize : partKeySizes) {
                            for (ui64 hashSize : hashSizes) {
                                if (hashSize && !partKeySize) {
                                    continue;
                                }
                                for (const auto& srcId : sourceIds) {

                                    auto blob = MakeClientBlob(payloadSize, seqNo++, partsCount, partNo, totalSize,
                                                            partKeySize, hashSize, srcId);

                                    buffer.Clear();
                                    Serialize(blob, buffer);

                                    TClientBlob deserialized = DeserializeClientBlob(buffer.data(), buffer.size());
                                    UNIT_ASSERT(blob == deserialized);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
}
<<<<<<< HEAD
=======

Y_UNIT_TEST_SUITE(Head) {
    Y_UNIT_TEST(EmptyHead) {
        THead head;
        UNIT_ASSERT_VALUES_EQUAL(head.GetCount(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(head.GetNextOffset(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(head.GetOffsetDelta(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(head.GetInternalPartsCount(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(head.FindPos(0, 0), Max<ui32>());
        UNIT_ASSERT(head.GetBatches().empty());
    }

    Y_UNIT_TEST(MultiBatchFindPosAndCount) {
        THead head;
        head.Offset = 100;

        TBatch batch0(100, 0);
        batch0.AddBlob(MakeSimpleBlob("a", 1, "d1"));
        batch0.AddBlob(MakeSimpleBlob("a", 2, "d2"));
        head.AddBatch(batch0);

        TBatch batch1(102, 0);
        batch1.AddBlob(MakeSimpleBlob("b", 1, "d3", 5));
        head.AddBatch(batch1);

        UNIT_ASSERT_VALUES_EQUAL(head.GetCount(), 7u);
        UNIT_ASSERT_VALUES_EQUAL(head.GetNextOffset(), 107u);
        UNIT_ASSERT(head.GetOffsetDelta() >= 7u);
        UNIT_ASSERT_VALUES_EQUAL(head.FindPos(99, 0), Max<ui32>());
        UNIT_ASSERT_VALUES_EQUAL(head.FindPos(100, 0), 0u);
        UNIT_ASSERT_VALUES_EQUAL(head.FindPos(101, 0), 0u);
        UNIT_ASSERT_VALUES_EQUAL(head.FindPos(102, 0), 1u);
        UNIT_ASSERT_VALUES_EQUAL(head.FindPos(106, 0), 1u);
    }

    Y_UNIT_TEST(AddBlobUpdatesInternalParts) {
        THead head;
        head.Offset = 0;
        head.AddBatch(TBatch(0, 0));

        const auto ts = TInstant::Seconds(1);
        head.AddBlob(TClientBlob(
            TString("src"), 1, TString("p0"), TPartData{0, 2, 10},
            ts, ts, 10, "", ""));
        UNIT_ASSERT_VALUES_EQUAL(head.GetInternalPartsCount(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(head.GetCount(), 0u);

        head.AddBlob(TClientBlob(
            TString("src"), 1, TString("p1"), TPartData{1, 2, 10},
            ts, ts, 10, "", ""));
        // Non-last parts remain counted as internal even after the message completes.
        UNIT_ASSERT_VALUES_EQUAL(head.GetInternalPartsCount(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(head.GetCount(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(head.GetLastBatch().Blobs.size(), 2u);
    }

    Y_UNIT_TEST(ExtractFirstBatch) {
        THead head;
        head.Offset = 10;

        TBatch batch0(10, 0);
        batch0.AddBlob(MakeSimpleBlob("a", 1, "x", 2));
        head.AddBatch(batch0);

        TBatch batch1(12, 0);
        batch1.AddBlob(MakeSimpleBlob("b", 1, "y"));
        head.AddBatch(batch1);

        UNIT_ASSERT_VALUES_EQUAL(head.GetBatches().size(), 2u);
        auto extracted = head.ExtractFirstBatch();
        UNIT_ASSERT_VALUES_EQUAL(extracted.GetOffset(), 10u);
        UNIT_ASSERT_VALUES_EQUAL(extracted.GetCount(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(head.GetBatches().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(head.GetLastBatch().GetOffset(), 12u);
        // GetCount still uses head.Offset == first batch offset invariant.
        head.Offset = 12;
        UNIT_ASSERT_VALUES_EQUAL(head.GetCount(), 1u);
    }

    Y_UNIT_TEST(ClearResetsState) {
        THead head;
        head.Offset = 5;
        head.PartNo = 1;
        head.PackedSize = 100;
        head.AddBatch(TBatch(5, 1));
        head.AddBlob(MakeSimpleBlob("a", 1, "d"));

        head.Clear();
        UNIT_ASSERT_VALUES_EQUAL(head.Offset, 0u);
        UNIT_ASSERT_VALUES_EQUAL(head.PartNo, 0u);
        UNIT_ASSERT_VALUES_EQUAL(head.PackedSize, 0u);
        UNIT_ASSERT(head.GetBatches().empty());
        UNIT_ASSERT_VALUES_EQUAL(head.GetInternalPartsCount(), 0u);
    }

    Y_UNIT_TEST(MutableBatchPackUnpack) {
        THead head;
        head.Offset = 0;
        head.AddBatch(TBatch(0, 0));
        head.AddBlob(MakeSimpleBlob("src", 1, "payload", 3, true));

        head.MutableLastBatch().Pack();
        UNIT_ASSERT(head.GetLastBatch().Packed);
        UNIT_ASSERT(head.GetLastBatch().Header.GetFormat() == NKikimrPQ::TBatchHeader::ECompressed);

        head.MutableLastBatch().Unpack();
        UNIT_ASSERT(!head.GetLastBatch().Packed);
        UNIT_ASSERT_VALUES_EQUAL(head.GetLastBatch().Blobs[0].LogicalMessageCount, 3u);
        UNIT_ASSERT(head.GetLastBatch().Blobs[0].IsBatch);
    }

    Y_UNIT_TEST(GetOffsetDeltaWithLMC) {
        THead head;
        head.Offset = 100;
        TBatch batch(100, 0);
        batch.AddBlob(MakeSimpleBlob("a", 1, "x", 5));
        head.AddBatch(batch);

        UNIT_ASSERT_VALUES_EQUAL(head.GetOffsetDelta(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(head.GetCount(), 5u);
    }
}

Y_UNIT_TEST_SUITE(PartitionedBlob) {
    // Moved from ydb/core/persqueue/ut/internals_ut.cpp
    Y_UNIT_TEST(TestPartitionedBlobSimpleTest) {
        THead head;
        THead newHead;

        TPartitionedBlob blob(TPartitionId(0), 0, "sourceId", 1, 1, 10, head, newHead, false, false, 8_MB);
        TClientBlob clientBlob("sourceId", 1, "valuevalue", TMaybe<TPartData>(), TInstant::MilliSeconds(1), TInstant::MilliSeconds(1), 0, "123", "123");
        UNIT_ASSERT(blob.IsInited());
        TString error;
        UNIT_ASSERT(blob.IsNextPart("sourceId", 1, 0, &error));

        blob.Add(std::move(clientBlob));
        UNIT_ASSERT(blob.IsComplete());
        UNIT_ASSERT(blob.GetFormedBlobs().empty());
        UNIT_ASSERT(blob.GetClientBlobs().size() == 1);
    }

    void TestPartitionedBlobCompaction(bool headCompacted, ui32 parts, ui32 partSize, ui32 leftInHead)
    {
        TVector<TClientBlob> all;

        THead head;
        head.Offset = 100;
        head.AddBatch(TBatch(head.Offset, 0));
        for (ui32 i = 0; i < 50; ++i) {
            TString value(100_KB, 'a');
            head.AddBlob(TClientBlob(
                "sourceId" + TString(1,'a' + rand() % 26), i + 1, std::move(value), TMaybe<TPartData>(),
                TInstant::MilliSeconds(i + 1),  TInstant::MilliSeconds(i + 1), 1, "", ""
            ));
            if (!headCompacted)
                all.push_back(head.GetLastBatch().Blobs.back());
        }
        head.MutableLastBatch().Pack();
        UNIT_ASSERT(head.GetLastBatch().Header.GetFormat() == NKikimrPQ::TBatchHeader::ECompressed);
        head.MutableLastBatch().Unpack();
        head.MutableLastBatch().Pack();
        TString str;
        head.GetLastBatch().SerializeTo(str);
        auto header = ExtractHeader(str.c_str(), str.size());
        TBatch batch(header, str.c_str() + header.ByteSize() + sizeof(ui16));
        batch.Unpack();

        head.PackedSize = head.GetLastBatch().GetPackedSize();
        UNIT_ASSERT(head.GetLastBatch().GetUnpackedSize() + GetMaxHeaderSize() >= head.GetLastBatch().GetPackedSize());
        THead newHead;
        newHead.Offset = head.GetNextOffset();
        newHead.AddBatch(TBatch(newHead.Offset, 0));
        for (ui32 i = 0; i < 10; ++i) {
            TString value(100_KB, 'a');
            newHead.AddBlob(TClientBlob(
                "sourceId2", i + 1, std::move(value), TMaybe<TPartData>(),
                TInstant::MilliSeconds(i + 1000), TInstant::MilliSeconds(i + 1000), 1, "", ""
            ));
            all.push_back(newHead.GetLastBatch().Blobs.back()); //newHead always glued
        }
        newHead.PackedSize = newHead.GetLastBatch().GetUnpackedSize();
        TString value2(partSize, 'b');
        ui32 maxBlobSize = 8 << 20;
        TPartitionedBlob blob(TPartitionId(0), newHead.GetNextOffset(), "sourceId3", 1, parts, parts * value2.size(), head, newHead, headCompacted, false, maxBlobSize);

        TVector<TPartitionedBlob::TFormedBlobInfo> formed;

        TString error;
        for (ui32 i = 0; i < parts; ++i) {
            UNIT_ASSERT(!blob.IsComplete());
            UNIT_ASSERT(blob.IsNextPart("sourceId3", 1, i, &error));
            TMaybe<TPartData> partData = TPartData(i, parts, value2.size());
            TString v = value2;
            TClientBlob clientBlob(
                "soruceId3", 1, std::move(v), std::move(partData),
                TInstant::MilliSeconds(1), TInstant::MilliSeconds(1), 1, "", ""
            );
            all.push_back(clientBlob);
            auto res = blob.Add(std::move(clientBlob));
            if (res && !res->Value.empty())
                formed.emplace_back(*res);
        }
        UNIT_ASSERT(blob.IsComplete());
        UNIT_ASSERT(formed.size() == blob.GetFormedBlobs().size());
        for (ui32 i = 0; i < formed.size(); ++i) {
            UNIT_ASSERT(formed[i].Key == blob.GetFormedBlobs()[i].OldKey);
            UNIT_ASSERT(formed[i].Value.size() == blob.GetFormedBlobs()[i].Size);
            UNIT_ASSERT(formed[i].Value.size() <= 8_MB);
            UNIT_ASSERT(formed[i].Value.size() > 6_MB);
        }
        TVector<TClientBlob> real;
        ui32 nextOffset = headCompacted ? newHead.Offset : head.Offset;
        for (auto& p : formed) {
            const char* data = p.Value.c_str();
            const char* end = data + p.Value.size();
            ui64 offset = p.Key.GetOffset();
            UNIT_ASSERT(offset == nextOffset);
            while(data < end) {
                auto header = ExtractHeader(data, end - data);
                UNIT_ASSERT(header.GetOffset() == nextOffset);
                nextOffset += header.GetCount();
                data += header.ByteSize() + sizeof(ui16);
                TBatch batch(header, data);
                data += header.GetPayloadSize();
                batch.Unpack();
                for (auto& b: batch.Blobs) {
                    real.push_back(b);
                }
            }
        }
        ui32 s = 0;
        ui32 c = 0;

        if (formed.empty()) { //nothing compacted - newHead must be here

            if (!headCompacted) {
                for (ui32 pp = 0; pp < head.GetBatches().size(); ++pp) {
                    head.MutableBatch(pp).Unpack();
                    for (const auto& b : head.GetBatch(pp).Blobs)
                        real.push_back(b);
                }
            }

            for (ui32 pp = 0; pp < newHead.GetBatches().size(); ++pp) {
                newHead.MutableBatch(pp).Unpack();
                for (const auto& b : newHead.GetBatch(pp).Blobs)
                    real.push_back(b);
            }
        }

        for (const auto& p : blob.GetClientBlobs()) {
            real.push_back(p);
            c++;
            s += p.GetSerializedSize();
        }

        UNIT_ASSERT(c == leftInHead);
        UNIT_ASSERT(s + GetMaxHeaderSize() <= maxBlobSize);
        UNIT_ASSERT(real.size() == all.size());
        for (ui32 i = 0; i < all.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(all[i].SourceId, real[i].SourceId);
            UNIT_ASSERT_VALUES_EQUAL(all[i].SeqNo, real[i].SeqNo);
            UNIT_ASSERT_VALUES_EQUAL(all[i].Data, real[i].Data);
            UNIT_ASSERT_VALUES_EQUAL(all[i].PartData.Defined(), real[i].PartData.Defined());
            if (all[i].PartData.Defined()) {
                UNIT_ASSERT_VALUES_EQUAL(all[i].PartData->PartNo, real[i].PartData->PartNo);
                UNIT_ASSERT_VALUES_EQUAL(all[i].PartData->TotalParts, real[i].PartData->TotalParts);
                UNIT_ASSERT_VALUES_EQUAL(all[i].PartData->TotalSize, real[i].PartData->TotalSize);
            }
        }
    }

    Y_UNIT_TEST(TestPartitionedBigTest) {
        TestPartitionedBlobCompaction(true, 100, 400_KB, 3);
        TestPartitionedBlobCompaction(false, 100, 512_KB - 9 - sizeof(ui64) - sizeof(ui16) - 100, 16);
        TestPartitionedBlobCompaction(false, 101, 512_KB - 9 - sizeof(ui64) - sizeof(ui16) - 100, 1);
        TestPartitionedBlobCompaction(false, 1, 512_KB - 9 - sizeof(ui64) - sizeof(ui16) - 100, 1);
        TestPartitionedBlobCompaction(true, 1, 512_KB - 9 - sizeof(ui64) - sizeof(ui16) - 100, 1);
        TestPartitionedBlobCompaction(true, 101, 512_KB - 9 - sizeof(ui64) - sizeof(ui16) - 100, 7);
    }

    Y_UNIT_TEST(IsNextPartRejectsMismatch) {
        THead head;
        THead newHead;
        TPartitionedBlob blob(
            TPartitionId(0), 0, "sourceId", 5, 3, 100, head, newHead, false, false, 8_MB);

        TString error;
        UNIT_ASSERT(!blob.IsNextPart("other", 5, 0, &error));
        UNIT_ASSERT(error.find("waited sourceId") != TString::npos);

        error.clear();
        UNIT_ASSERT(!blob.IsNextPart("sourceId", 6, 0, &error));
        UNIT_ASSERT(error.find("seqNo") != TString::npos);

        error.clear();
        UNIT_ASSERT(!blob.IsNextPart("sourceId", 5, 1, &error));
        UNIT_ASSERT(error.find("partNo") != TString::npos);

        error.clear();
        UNIT_ASSERT(blob.IsNextPart("sourceId", 5, 0, &error));
    }

    Y_UNIT_TEST(AddKeyRenamePath) {
        THead head;
        THead newHead;
        newHead.Offset = 100;

        TPartitionedBlob blob(
            TPartitionId(3), 100, "src", 1, 1, 10, head, newHead, true, false, 8_MB, 0, false);

        auto oldKey = TKey::ForBody(
            TKeyPrefix::TypeData, TPartitionId(3), 50, 0, 5, 0);
        auto ts = TInstant::Seconds(42);
        auto res = blob.Add(oldKey, 12345, ts, false);
        UNIT_ASSERT(!res.has_value());
        UNIT_ASSERT(blob.HasFormedBlobs());
        UNIT_ASSERT_VALUES_EQUAL(blob.GetFormedBlobs().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(blob.GetFormedBlobs()[0].OldKey.ToString(), oldKey.ToString());
        UNIT_ASSERT_VALUES_EQUAL(blob.GetFormedBlobs()[0].NewKey.GetOffset(), 100u);
        UNIT_ASSERT_VALUES_EQUAL(blob.GetFormedBlobs()[0].Size, 12345u);
        UNIT_ASSERT_VALUES_EQUAL(blob.GetFormedBlobs()[0].CreationUnixTime, ts);
    }

    Y_UNIT_TEST(GetOffsetDeltaCountsPendingBlobs) {
        THead head;
        THead newHead;
        newHead.Offset = 0;

        TPartitionedBlob blob(
            TPartitionId(0), 0, "src", 1, 2, 20, head, newHead, true, false, 8_MB);

        blob.Add(MakeSimpleBlob("src", 1, "p0")); // treated as complete part count via LogicalMessageCount
        // After first of two parts: not complete yet; offset delta from pending blobs with PartNo==0 / no PartData
        UNIT_ASSERT_VALUES_EQUAL(blob.GetOffsetDelta(), 1u);
    }
}

Y_UNIT_TEST_SUITE(FindFirstOffsetAtOrAfterTimestamp) {
    Y_UNIT_TEST(FindsMessageInTheMiddleOfABlob) {
        const TVector<TClientBlob> blobs = {
            MakeSimpleBlob("a", 1, "m1"),
            MakeSimpleBlob("a", 2, "m2"),
            MakeSimpleBlob("a", 3, "m3"),
        };
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(TInstant::Seconds(101), 10, blobs), 10u);
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(TInstant::Seconds(102), 10, blobs), 11u);
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(TInstant::Seconds(102) + TDuration::MilliSeconds(1), 10, blobs), 12u);
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(TInstant::Seconds(103), 10, blobs), 12u);
        UNIT_ASSERT(!FindFirstOffsetAtOrAfterTimestamp(TInstant::Seconds(104), 10, blobs).Defined());
    }

    Y_UNIT_TEST(KafkaBatchIsAtomic) {
        const TVector<TClientBlob> blobs = {
            MakeSimpleBlob("a", 1, "before"),
            MakeSimpleBlob("k", 2, "batch", 3, true),
            MakeSimpleBlob("a", 3, "after"),
        };
        // Batch occupies offsets 11,12,13. Do not unpack: either first of the batch or first after it.
        // Interior offsets 12/13 are never returned.
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(TInstant::Seconds(102), 10, blobs), 11u);
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(TInstant::Seconds(102) + TDuration::MilliSeconds(1), 10, blobs), 14u);
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(TInstant::Seconds(103), 10, blobs), 14u);
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(TInstant::Seconds(101), 10, blobs), 10u); // before first
        UNIT_ASSERT(!FindFirstOffsetAtOrAfterTimestamp(TInstant::Seconds(104), 10, blobs).Defined()); // after last
    }

    Y_UNIT_TEST(BeforeFirstAndAfterLastMessage) {
        const TVector<TClientBlob> blobs = {
            MakeSimpleBlob("a", 1, "m1"),
            MakeSimpleBlob("a", 2, "m2"),
        };
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(TInstant::Seconds(50), 10, blobs), 10u);
        UNIT_ASSERT(!FindFirstOffsetAtOrAfterTimestamp(TInstant::Seconds(200), 10, blobs).Defined());
    }

    Y_UNIT_TEST(MultipartMessageUsesFirstPartTimestamp) {
        auto ts = TInstant::Seconds(50);
        TClientBlob part0(TString("s"), 1, TString("p0"), TPartData(0, 2, 10), ts, ts, 0, TString(), TString());
        TClientBlob part1(TString("s"), 1, TString("p1"), TPartData(1, 2, 10), ts, ts, 0, TString(), TString());
        TClientBlob next = MakeSimpleBlob("s", 2, "next");
        const TVector<TClientBlob> blobs = {part0, part1, next};
        UNIT_ASSERT_VALUES_EQUAL(*FindFirstOffsetAtOrAfterTimestamp(ts, 0, blobs), 0u);
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(ts + TDuration::MilliSeconds(1), 0, blobs), 1u);
    }

    TClientBlob MakeBlobAt(TString data, ui64 seqNo, TInstant ts) {
        return TClientBlob(
            TString("s"), seqNo, std::move(data), TMaybe<TPartData>(),
            ts, ts, 0, TString(), TString());
    }

    void AppendPackedBatch(TString& blob, TBatch& batch) {
        batch.Pack();
        batch.SerializeTo(blob);
    }

    Y_UNIT_TEST(OffsetGapsBetweenBatches) {
        const auto tsEarly = TInstant::Seconds(100);
        const auto tsLate = TInstant::Seconds(200);

        TBatch first(10, 0);
        first.AddBlob(MakeBlobAt("a", 1, tsEarly));
        TBatch second(20, 0);
        second.AddBlob(MakeBlobAt("b", 2, tsLate));

        const TVector<TBatch> batches = {first, second};

        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(tsEarly, 10, batches), 10u);
        // Gap 11..19 must not be treated as a message at offset 11.
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(tsEarly + TDuration::MilliSeconds(1), 10, batches), 20u);
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(tsLate, 10, batches), 20u);
        UNIT_ASSERT(!FindFirstOffsetAtOrAfterTimestamp(tsLate + TDuration::MilliSeconds(1), 10, batches).Defined());
    }

    Y_UNIT_TEST(OffsetGapsBetweenPackedBatches) {
        const auto tsEarly = TInstant::Seconds(100);
        const auto tsLate = TInstant::Seconds(200);

        TString blob;
        TBatch first(10, 0);
        first.AddBlob(MakeBlobAt("a", 1, tsEarly));
        AppendPackedBatch(blob, first);

        TBatch second(20, 0);
        second.AddBlob(MakeBlobAt("b", 2, tsLate));
        AppendPackedBatch(blob, second);

        auto key = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId(0), 10, 0, 2, 0);
        auto batches = GetUnpackedBatches(key, blob);

        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(tsEarly + TDuration::MilliSeconds(1), 10, batches), 20u);
        UNIT_ASSERT_VALUES_EQUAL(
            *FindFirstOffsetAtOrAfterTimestamp(tsLate, 10, batches), 20u);
    }
}

Y_UNIT_TEST_SUITE(BatchUnpackMalformed) {
    TBatch MakePackedBatch(ui32 blobCount) {
        TBatch batch(0, 0);
        for (ui32 i = 0; i < blobCount; ++i) {
            batch.AddBlob(MakeSimpleBlob("src", i + 1, TString(1 + i, 'a' + i)));
        }
        batch.Pack();
        return batch;
    }

    size_t SkipChunks(const TBuffer& data, ui32 count) {
        size_t pos = 0;
        for (ui32 i = 0; i < count; ++i) {
            pos += sizeof(ui32) + ReadUnaligned<ui32>(data.Data() + pos);
        }
        return pos;
    }

    // YDBBUGS-1116: a corrupted trailing cumulative-offset word of a VarLen
    // chunk used to produce a wild offsets pointer and a SEGV inside
    // TVarLenDecoder::TChunkIterator::Next.
    Y_UNIT_TEST(UnpackThrowsOnCorruptedVarLenDataSize) {
        auto batch = MakePackedBatch(5);

        // Chunks: [order][SourceId][SeqNo][Data]...
        // The last word of the SourceId chunk is its trailing data size.
        const size_t sourceIdChunkPos = SkipChunks(batch.PackedData, 1);
        const ui32 sourceIdChunkSize = ReadUnaligned<ui32>(batch.PackedData.Data() + sourceIdChunkPos);
        WriteUnaligned<ui32>(
            batch.PackedData.Data() + sourceIdChunkPos + sizeof(ui32) + sourceIdChunkSize - sizeof(ui32),
            Max<ui32>() - 3);

        TVector<TClientBlob> result;
        UNIT_ASSERT_EXCEPTION(batch.UnpackTo(&result), yexception);
    }

    // YDBBUGS-1116: non-monotonic cumulative offsets used to underflow the
    // value size in TVarLenDecoder::TChunkIterator::Next.
    Y_UNIT_TEST(UnpackThrowsOnCorruptedDataChunkOffsets) {
        auto batch = MakePackedBatch(4);
        const ui32 blobCount = 4;

        // Chunks: [order][SourceId][SeqNo][Data]...
        // The Data chunk ends with blobCount cumulative offsets.
        const size_t dataChunkPos = SkipChunks(batch.PackedData, 3);
        const ui32 dataChunkSize = ReadUnaligned<ui32>(batch.PackedData.Data() + dataChunkPos);
        const size_t offsetsPos = dataChunkPos + sizeof(ui32) + dataChunkSize - blobCount * sizeof(ui32);
        // Decrease the third cumulative offset: offsets become non-monotonic.
        WriteUnaligned<ui32>(batch.PackedData.Data() + offsetsPos + 2 * sizeof(ui32), 0);

        TVector<TClientBlob> result;
        UNIT_ASSERT_EXCEPTION(batch.UnpackTo(&result), yexception);
    }

    // YDBBUGS-1116: a header claiming more client blobs than the chunks
    // encode used to read all chunk iterators past their ends.
    Y_UNIT_TEST(UnpackThrowsOnHeaderBlobCountMismatch) {
        auto batch = MakePackedBatch(3);

        batch.Header.SetClientBlobCount(1'000'000);

        TVector<TClientBlob> result;
        UNIT_ASSERT_EXCEPTION(batch.UnpackTo(&result), yexception);
    }
}

} // namespace NKikimr::NPQ
>>>>>>> 68a78bb0d67 (YDBBUGS-1116 Fix PersQueue blob decoding crash in chunk decoders)
