#include "blob_int.h"
#include "blob.h"
#include "blob_offset.h"
#include "header.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>
#include <util/system/unaligned_mem.h>

#include <deque>

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

    Y_UNIT_TEST(Flags_HasBatchInfo) {
        TMessageFlags flags;

        flags.F.HasBatchInfo = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 32);
    }

    Y_UNIT_TEST(PartData_IsLastPart) {
        UNIT_ASSERT(TPartData(2, 3, 100).IsLastPart());
        UNIT_ASSERT(!TPartData(0, 3, 100).IsLastPart());
        UNIT_ASSERT(TPartData(0, 1, 10).IsLastPart());
    }

    Y_UNIT_TEST(ClientBlob_PartHelpers) {
        auto ts = TInstant::Seconds(1);
        TClientBlob simple(TString("s"), 1, TString("d"), TMaybe<TPartData>(), ts, ts, 42, "", "");
        UNIT_ASSERT_VALUES_EQUAL(simple.GetPartNo(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(simple.GetTotalParts(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(simple.GetTotalSize(), 42u);
        UNIT_ASSERT(simple.IsLastPart());

        TClientBlob part(TString("s"), 1, TString("d"), TPartData{1, 3, 1000}, ts, ts, 1000, "", "");
        UNIT_ASSERT_VALUES_EQUAL(part.GetPartNo(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(part.GetTotalParts(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(part.GetTotalSize(), 1000u);
        UNIT_ASSERT(!part.IsLastPart());
    }

    Y_UNIT_TEST(LogicalMessageCountConstants) {
        UNIT_ASSERT_VALUES_EQUAL(MESSAGE_METADATA_RESERVED_BITS, 1u);
        UNIT_ASSERT_VALUES_EQUAL(LOGICAL_MESSAGE_COUNT_BITS, 31u);
        UNIT_ASSERT_VALUES_EQUAL(MAX_LOGICAL_MESSAGE_COUNT, (1u << 31) - 1);
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
        lhs.LogicalMessageCount == rhs.LogicalMessageCount &&
        lhs.IsBatch == rhs.IsBatch;
}

void AssertClientBlobsEqual(const TVector<TClientBlob>& expected, const TVector<TClientBlob>& actual) {
    UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        UNIT_ASSERT(expected[i] == actual[i]);
    }
}

TClientBlob MakeSimpleBlob(
    TString sourceId, ui64 seqNo, TString data,
    ui32 logicalMessageCount = 1, bool isBatch = false,
    ui32 uncompressedSize = 0,
    TString partitionKey = "", TString explicitHashKey = "")
{
    auto ts = TInstant::Seconds(100 + seqNo);
    return TClientBlob(
        std::move(sourceId), seqNo, std::move(data), TMaybe<TPartData>(),
        ts, ts + TDuration::Seconds(1), uncompressedSize,
        std::move(partitionKey), std::move(explicitHashKey),
        logicalMessageCount, isBatch);
}

Y_UNIT_TEST_SUITE(ClientBlobSerialization) {

    // Serialize() sets optional wire flags from InitFlags:
    //   HasPartData          ← PartData defined
    //   HasWriteTimestamp    ← always 1
    //   HasCreateTimestamp   ← always 1
    //   HasUncompressedSize  ← UncompressedSize != 0
    //   HasKinesisData       ← PartitionKey non-empty (ExplicitHash alone is NOT written)
    //   HasBatchInfo         ← LogicalMessageCount != 1 || IsBatch
    TClientBlob MakeClientBlob(
        ui64 seqNo,
        TString sourceId,
        TString data,
        TMaybe<TPartData> partData,
        ui32 uncompressedSize,
        TString partitionKey,
        TString explicitHashKey,
        ui32 logicalMessageCount,
        bool isBatch)
    {
        auto ts = TInstant::Seconds(100 + (seqNo % 1000));
        return TClientBlob{
            std::move(sourceId),
            seqNo,
            std::move(data),
            partData,
            ts,
            ts + TDuration::Seconds(5),
            uncompressedSize,
            std::move(partitionKey),
            std::move(explicitHashKey),
            logicalMessageCount,
            isBatch};
    }

    void RoundTrip(const TClientBlob& blob) {
        TBuffer buffer;
        buffer.Reserve(blob.GetSerializedSize() + 64);
        Serialize(blob, buffer);
        UNIT_ASSERT_VALUES_EQUAL(blob.GetSerializedSize(), buffer.Size());
        TClientBlob deserialized = DeserializeClientBlob(buffer.Data(), buffer.Size());
        UNIT_ASSERT(blob == deserialized);
    }

    Y_UNIT_TEST(MessageMetadataStoresCountInUpperBits) {
        TBuffer buffer;
        auto blob = MakeClientBlob(42, "src", "payload", Nothing(), 0, "", "", 7, false);
        Serialize(blob, buffer);

        const char* data = buffer.Data();
        data += sizeof(ui32); // total size
        data += sizeof(ui64); // seqNo
        data += sizeof(ui8);  // flags
        const ui32 messageMetadata = ReadUnaligned<ui32>(data);

        UNIT_ASSERT_VALUES_EQUAL(messageMetadata, 7u << MESSAGE_METADATA_RESERVED_BITS);
    }

    Y_UNIT_TEST(IsBatchMetadataLowBit) {
        TBuffer buffer;
        auto blob = MakeClientBlob(42, "src", "payload", Nothing(), 0, "", "", 1, true);
        Serialize(blob, buffer);

        const char* data = buffer.Data();
        data += sizeof(ui32);
        data += sizeof(ui64);
        TMessageFlags flags(ReadUnaligned<ui8>(data));
        UNIT_ASSERT(flags.F.HasBatchInfo);
        data += sizeof(ui8);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadUnaligned<ui32>(data),
            (1u << MESSAGE_METADATA_RESERVED_BITS) | 1u);
        RoundTrip(blob);
    }

    Y_UNIT_TEST(PartitionKey256RestoredFromZeroSizeByte) {
        TString partitionKey(256, 'p');
        auto blob = MakeClientBlob(1, "src", "data", Nothing(), 0, partitionKey, TString(10, 'h'), 1, false);
        RoundTrip(blob);
        UNIT_ASSERT_VALUES_EQUAL(blob.PartitionKey.size(), 256u);
    }

    Y_UNIT_TEST(ExplicitHashWithoutPartitionKeyIsNotSerialized) {
        // InitFlags gates Kinesis on PartitionKey only; ExplicitHash alone must not round-trip.
        auto blob = MakeClientBlob(1, "src", "data", Nothing(), 0, "", "hash-only", 1, false);
        TBuffer buffer;
        Serialize(blob, buffer);
        TClientBlob deserialized = DeserializeClientBlob(buffer.Data(), buffer.Size());
        UNIT_ASSERT(deserialized.PartitionKey.empty());
        UNIT_ASSERT(deserialized.ExplicitHashKey.empty());
        UNIT_ASSERT_VALUES_EQUAL(deserialized.Data, blob.Data);
    }

    Y_UNIT_TEST(SerializeAndDeserializeAllFlagCombinations) {
        // Axes cover every InitFlags branch that Serialize can emit.
        const TVector<TMaybe<TPartData>> partCases = {
            Nothing(),
            TPartData{0, 1, 50},
            TPartData{0, 3, 300},
            TPartData{1, 3, 300},
            TPartData{2, 3, 300},
        };
        const TVector<ui32> uncompressedCases = {0, 1, 42};
        struct TKinesisCase {
            TString PartitionKey;
            TString ExplicitHashKey;
        };
        const TVector<TKinesisCase> kinesisCases = {
            {"", ""},
            {"pk", ""},
            {"pk", "hk"},
            {TString(1, 'x'), TString(255, 'h')},
            {TString(256, 'p'), TString(10, 'h')},
        };
        struct TBatchCase {
            ui32 LogicalMessageCount;
            bool IsBatch;
        };
        const TVector<TBatchCase> batchCases = {
            {1, false},
            {2, false},
            {1, true},
            {5, true},
            {MAX_LOGICAL_MESSAGE_COUNT, false},
            {MAX_LOGICAL_MESSAGE_COUNT, true},
        };
        const TVector<TString> payloads = {
            "",
            "x",
            TString(100, 'a'),
            TString(1_KB, 'b'),
        };
        const TVector<TString> sourceIds = {"", "src", TString(200, 's')};

        ui64 seqNo = 1;
        ui64 cases = 0;
        for (const auto& partData : partCases) {
            for (ui32 uncompressedSize : uncompressedCases) {
                for (const auto& kinesis : kinesisCases) {
                    for (const auto& batch : batchCases) {
                        for (const auto& payload : payloads) {
                            for (const auto& sourceId : sourceIds) {
                                auto blob = MakeClientBlob(
                                    seqNo++,
                                    sourceId,
                                    payload,
                                    partData,
                                    uncompressedSize,
                                    kinesis.PartitionKey,
                                    kinesis.ExplicitHashKey,
                                    batch.LogicalMessageCount,
                                    batch.IsBatch);
                                RoundTrip(blob);
                                ++cases;
                            }
                        }
                    }
                }
            }
        }
        // 5 * 3 * 5 * 6 * 4 * 3 = 5400
        UNIT_ASSERT_VALUES_EQUAL(cases, 5400u);
    }

    Y_UNIT_TEST(SerializeAndDeserializeLargePayloadAndPartEdges) {
        // Large payloads and extreme PartData values — orthogonal to the flag matrix above.
        struct TCase {
            ui64 PayloadSize;
            TMaybe<TPartData> PartData;
            ui32 UncompressedSize;
            ui64 PartKeySize;
            ui64 HashSize;
            ui32 LogicalMessageCount;
            bool IsBatch;
            TString SourceId;
        };
        const TVector<TCase> cases = {
            {0, Nothing(), 0, 0, 0, 1, false, "src"},
            {0, TPartData{1, 100, 100}, 0, 0, 0, 1, false, ""},
            {10, TPartData{0, 1, 10}, 20, 0, 0, 1, false, "src"},
            {100, TPartData{0, 2, 100}, 0, 100, 0, 3, true, "src"},
            {100, TPartData{1, 2, 100}, 200, 100, 255, 1, true, ""},
            {512_KB, Nothing(), 0, 0, 0, 1, false, "src"},
            {512_KB, Nothing(), 512_KB * 2, 256, 10, 5, true, "src"},
            {512_KB, TPartData{0, 2, 1_MB}, 0, 256, 100, 1, false, ""},
            {512_KB, TPartData{1, 2, 1_MB}, 1, 0, 0, 2, false, "src"},
            {100, TPartData{9, 40, 10_MB}, 0, 100, 1, 1, false, "src"},
            {100, TPartData{39, 40, 10_MB}, 50, 256, 255, MAX_LOGICAL_MESSAGE_COUNT, true, TString(200, 's')},
            {1_MB, Nothing(), 0, 256, 255, 1, false, "big"},
        };

        ui64 seqNo = 1;
        TBuffer buffer;
        buffer.Reserve(8_MB);
        for (const auto& c : cases) {
            TString data = c.PayloadSize ? NUnitTest::RandomString(c.PayloadSize) : TString();
            TString pk = c.PartKeySize ? NUnitTest::RandomString(c.PartKeySize) : TString();
            TString hk = c.HashSize ? NUnitTest::RandomString(c.HashSize) : TString();
            auto blob = MakeClientBlob(
                seqNo++, c.SourceId, std::move(data), c.PartData,
                c.UncompressedSize, std::move(pk), std::move(hk),
                c.LogicalMessageCount, c.IsBatch);

            buffer.Clear();
            Serialize(blob, buffer);
            UNIT_ASSERT_VALUES_EQUAL(blob.GetSerializedSize(), buffer.Size());
            UNIT_ASSERT(blob == DeserializeClientBlob(buffer.Data(), buffer.Size()));
        }
    }
}

Y_UNIT_TEST_SUITE(BlobOffset) {
    Y_UNIT_TEST(IdentityWhenSpacesAgree) {
        constexpr ui64 offset = 100;
        UNIT_ASSERT_VALUES_EQUAL(HeaderOffsetToKeySpace(offset, offset, 105), 105u);
        UNIT_ASSERT_VALUES_EQUAL(KeyOffsetToHeaderSpace(offset, offset, 105), 105u);
    }

    Y_UNIT_TEST(RoundTripAfterTxKeyRename) {
        // Production-like: parent Key offset vs supportive first header.
        constexpr ui64 blobKeyOffset = 547'189'849;
        constexpr ui64 firstHeaderOffset = 391;
        constexpr ui64 midInBlob = 2;

        const ui64 keySpace = blobKeyOffset + midInBlob;
        const ui64 headerSpace = firstHeaderOffset + midInBlob;

        UNIT_ASSERT_VALUES_EQUAL(
            HeaderOffsetToKeySpace(blobKeyOffset, firstHeaderOffset, headerSpace),
            keySpace);
        UNIT_ASSERT_VALUES_EQUAL(
            KeyOffsetToHeaderSpace(blobKeyOffset, firstHeaderOffset, keySpace),
            headerSpace);

        UNIT_ASSERT_VALUES_EQUAL(
            HeaderOffsetToKeySpace(
                blobKeyOffset,
                firstHeaderOffset,
                KeyOffsetToHeaderSpace(blobKeyOffset, firstHeaderOffset, keySpace)),
            keySpace);
    }

    Y_UNIT_TEST(SupportiveStartsAtZero) {
        constexpr ui64 blobKeyOffset = 1'000'000;
        constexpr ui64 firstHeaderOffset = 0;

        for (ui64 mid : {0ull, 1ull, 17ull, 999ull}) {
            const ui64 keySpace = blobKeyOffset + mid;
            const ui64 headerSpace = firstHeaderOffset + mid;
            UNIT_ASSERT_VALUES_EQUAL(
                HeaderOffsetToKeySpace(blobKeyOffset, firstHeaderOffset, headerSpace),
                keySpace);
            UNIT_ASSERT_VALUES_EQUAL(
                KeyOffsetToHeaderSpace(blobKeyOffset, firstHeaderOffset, keySpace),
                headerSpace);
        }
    }

    Y_UNIT_TEST(FirstHeaderGreaterThanKey) {
        // Unusual but mathematically valid: header coords above key coords.
        constexpr ui64 blobKeyOffset = 100;
        constexpr ui64 firstHeaderOffset = 500;

        for (ui64 mid : {0ull, 3ull, 50ull}) {
            const ui64 keySpace = blobKeyOffset + mid;
            const ui64 headerSpace = firstHeaderOffset + mid;
            UNIT_ASSERT_VALUES_EQUAL(
                HeaderOffsetToKeySpace(blobKeyOffset, firstHeaderOffset, headerSpace),
                keySpace);
            UNIT_ASSERT_VALUES_EQUAL(
                KeyOffsetToHeaderSpace(blobKeyOffset, firstHeaderOffset, keySpace),
                headerSpace);
        }
    }
}

Y_UNIT_TEST_SUITE(BatchPacking) {
    // Compressed pack chooses optional chunks from OR of blob fields:
    //   hasUncompressed ← any UncompressedSize > 0
    //   hasKinesis      ← any PartitionKey OR ExplicitHashKey non-empty
    //   hasBatchInfo    ← any LogicalMessageCount != 1 || IsBatch
    // Plus PartData map and multi-SourceId reorder.
    Y_UNIT_TEST(PackUnpackAllFormatCombinations) {
        auto ts = TInstant::Seconds(100);
        ui64 cases = 0;

        for (bool withPartData : {false, true}) {
            for (bool withUncompressed : {false, true}) {
                for (bool withKinesis : {false, true}) {
                    for (bool withBatchInfo : {false, true}) {
                        for (bool withIsBatch : {false, true}) {
                            if (withIsBatch && !withBatchInfo) {
                                continue; // IsBatch implies HasBatchInfo chunk
                            }
                            for (bool multiSourceId : {false, true}) {
                                TBatch batch(0, 0);
                                TVector<TClientBlob> expected;

                                auto add = [&](TString sid, ui64 seqNo, TString data,
                                               TMaybe<TPartData> part, ui32 unc,
                                               TString pk, TString hk, ui32 lmc, bool isBatch) {
                                    TClientBlob blob(
                                        std::move(sid), seqNo, std::move(data), part,
                                        ts, ts + TDuration::Seconds(1), unc,
                                        std::move(pk), std::move(hk), lmc, isBatch);
                                    expected.push_back(blob);
                                    batch.AddBlob(blob);
                                };

                                const ui32 lmc = withBatchInfo ? (withIsBatch ? 1u : 3u) : 1u;
                                const bool isBatch = withIsBatch;
                                const ui32 unc = withUncompressed ? 17u : 0u;
                                const TString pk = withKinesis ? "partition-key" : "";
                                const TString hk = withKinesis ? "hash-key" : "";

                                if (withPartData) {
                                    add(multiSourceId ? "sidA" : "sid", 1, "p0",
                                        TPartData{0, 2, 20}, unc, pk, hk, 1, false);
                                    add(multiSourceId ? "sidA" : "sid", 1, "p1",
                                        TPartData{1, 2, 20}, unc, pk, hk, 1, false);
                                    add(multiSourceId ? "sidB" : "sid", 2, "next",
                                        Nothing(), unc, pk, hk, lmc, isBatch);
                                } else {
                                    add(multiSourceId ? "sidA" : "sid", 1, "one",
                                        Nothing(), unc, pk, hk, lmc, isBatch);
                                    add(multiSourceId ? "sidB" : "sid", 2, "two",
                                        Nothing(), unc, pk, hk, 1, false);
                                    if (multiSourceId) {
                                        add("sidA", 3, "three",
                                            Nothing(), unc, pk, hk, 1, false);
                                    }
                                }

                                batch.Pack();
                                UNIT_ASSERT(batch.Packed);
                                UNIT_ASSERT(
                                    batch.Header.GetFormat() == NKikimrPQ::TBatchHeader::ECompressed ||
                                    batch.Header.GetFormat() == NKikimrPQ::TBatchHeader::EUncompressed);
                                if (withKinesis &&
                                    batch.Header.GetFormat() == NKikimrPQ::TBatchHeader::ECompressed)
                                {
                                    UNIT_ASSERT(batch.Header.GetHasKinesis());
                                }

                                batch.Unpack();
                                AssertClientBlobsEqual(expected, batch.Blobs);
                                ++cases;
                            }
                        }
                    }
                }
            }
        }
        // 2*2*2 * (1+2) * 2 = 48  (batchInfo/isBatch: 3 valid pairs)
        UNIT_ASSERT_VALUES_EQUAL(cases, 48u);
    }

    Y_UNIT_TEST(CompressedPackPreservesExplicitHashWithoutPartitionKey) {
        // Unlike Serialize(), compressed batch packing emits Kinesis columns when
        // ExplicitHashKey is set even if PartitionKey is empty.
        TBatch batch(0, 0);
        TVector<TClientBlob> expected;
        auto ts = TInstant::Seconds(100);
        for (ui32 i = 0; i < 50; ++i) {
            TClientBlob blob(
                TString("sid"), i + 1, TString(10, 'a'), Nothing(),
                ts, ts, 0, "", TString("hash-only"), 1, false);
            expected.push_back(blob);
            batch.AddBlob(blob);
        }
        batch.Pack();
        UNIT_ASSERT(batch.Header.GetFormat() == NKikimrPQ::TBatchHeader::ECompressed);
        UNIT_ASSERT(batch.Header.GetHasKinesis());
        batch.Unpack();
        AssertClientBlobsEqual(expected, batch.Blobs);
    }

    // Moved from ydb/core/persqueue/ut/internals_ut.cpp
    Y_UNIT_TEST(TestBatchPacking) {
        TBatch batch;
        for (ui32 i = 0; i < 100; ++i) {
            TString value(10, 'a');
            batch.AddBlob(TClientBlob(
                "sourceId1", i + 1, std::move(value), TMaybe<TPartData>(),
                TInstant::MilliSeconds(1), TInstant::MilliSeconds(1), 0, "", ""
            ));
        }
        batch.Pack();
        TBuffer b = batch.PackedData;
        UNIT_ASSERT(batch.Header.GetFormat() == NKikimrPQ::TBatchHeader::ECompressed);
        batch.Unpack();
        batch.Pack();
        UNIT_ASSERT(batch.PackedData == b);
        TString str;
        batch.SerializeTo(str);
        auto header = ExtractHeader(str.c_str(), str.size());
        TBatch batch2(header, str.c_str() + header.ByteSize() + sizeof(ui16));
        batch2.Unpack();
        UNIT_ASSERT_VALUES_EQUAL(batch2.Blobs.size(), 100u);

        TBatch batch3;
        TString value;
        value.reserve(64_KB);
        ui32 rnd = 0x12345678;
        for (ui32 i = 0; i < 64_KB; ++i) {
            rnd ^= rnd << 13;
            rnd ^= rnd >> 17;
            rnd ^= rnd << 5;
            value.push_back(static_cast<char>(rnd));
        }
        const TString expectedValue = value;
        batch3.AddBlob(TClientBlob(
            "sourceId", 999'999'999'999'999ll, std::move(value), TPartData{33, 66, 4'000'000'000u},
            TInstant::MilliSeconds(999'999'999'999ll), TInstant::MilliSeconds(1000), 0, "", ""
        ));
        batch3.Pack();
        batch3.Unpack();
        UNIT_ASSERT_VALUES_EQUAL(batch3.Blobs.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch3.Blobs[0].SourceId, "sourceId");
        UNIT_ASSERT_VALUES_EQUAL(batch3.Blobs[0].SeqNo, 999'999'999'999'999ull);
        UNIT_ASSERT(batch3.Blobs[0].PartData.Defined());
        UNIT_ASSERT_VALUES_EQUAL(batch3.Blobs[0].PartData->PartNo, 33u);
        UNIT_ASSERT_VALUES_EQUAL(batch3.Blobs[0].PartData->TotalParts, 66u);
        UNIT_ASSERT_VALUES_EQUAL(batch3.Blobs[0].PartData->TotalSize, 4'000'000'000u);
        UNIT_ASSERT_VALUES_EQUAL(batch3.Blobs[0].Data, expectedValue);
    }

    Y_UNIT_TEST(CompressedMultiSourceIdRoundtrip) {
        TBatch batch(10, 0);
        TVector<TClientBlob> expected;
        expected.push_back(MakeSimpleBlob("sidA", 1, "data-a1"));
        expected.push_back(MakeSimpleBlob("sidB", 1, "data-b1"));
        expected.push_back(MakeSimpleBlob("sidA", 2, "data-a2"));
        expected.push_back(MakeSimpleBlob("sidC", 5, "data-c1", 3, true, 100, "pk", "hk"));
        expected.push_back(MakeSimpleBlob("sidB", 2, "data-b2", 2));

        for (const auto& b : expected) {
            batch.AddBlob(b);
        }

        batch.Pack();
        UNIT_ASSERT(batch.Packed);
        UNIT_ASSERT(batch.Header.GetFormat() == NKikimrPQ::TBatchHeader::ECompressed);
        UNIT_ASSERT(batch.Header.GetHasKinesis());

        TBuffer packedSnapshot = batch.PackedData;
        batch.Unpack();
        AssertClientBlobsEqual(expected, batch.Blobs);

        batch.Pack();
        UNIT_ASSERT(batch.PackedData == packedSnapshot);
    }

    Y_UNIT_TEST(CompressedMultipartWithBatchInfo) {
        TBatch batch(0, 0);
        const auto ts = TInstant::Seconds(50);
        constexpr ui32 totalSize = 300;

        TVector<TClientBlob> expected;
        expected.push_back(TClientBlob(
            TString("src"), 1, TString("p0"), TPartData{0, 2, totalSize},
            ts, ts, totalSize, TString("pk"), TString("hk"), 1, false));
        expected.push_back(TClientBlob(
            TString("src"), 1, TString("p1"), TPartData{1, 2, totalSize},
            ts, ts, totalSize, TString("pk"), TString("hk"), 1, false));
        expected.push_back(TClientBlob(
            TString("src"), 2, TString("batch"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 4, true));

        for (const auto& b : expected) {
            batch.AddBlob(b);
        }

        UNIT_ASSERT_VALUES_EQUAL(batch.GetCount(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(batch.GetInternalPartsCount(), 1u);

        batch.Pack();
        batch.Unpack();
        AssertClientBlobsEqual(expected, batch.Blobs);
        UNIT_ASSERT_VALUES_EQUAL(batch.GetInternalPartsCount(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 0).BlobIdx, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 1).BlobIdx, 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(1, 0).BlobIdx, 2u);
    }

    Y_UNIT_TEST(UnpackToDoesNotMutateBatch) {
        TBatch batch(0, 0);
        batch.AddBlob(MakeSimpleBlob("src", 1, "data", 2));
        batch.Pack();

        UNIT_ASSERT(batch.Packed);
        UNIT_ASSERT(batch.Blobs.empty());

        TVector<TClientBlob> result;
        batch.UnpackTo(&result);
        UNIT_ASSERT(batch.Packed);
        UNIT_ASSERT(batch.Blobs.empty());
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result[0].LogicalMessageCount, 2u);
        UNIT_ASSERT_VALUES_EQUAL(result[0].Data, "data");
    }

    Y_UNIT_TEST(UncompressedFallbackRoundtrip) {
        // Columnar packing has fixed per-chunk overhead; for a tiny batch it exceeds
        // raw serialization, so Pack() falls back to EUncompressed.
        TBatch batch(0, 0);
        TVector<TClientBlob> expected;
        expected.push_back(MakeSimpleBlob("src", 1, "x", 1, true, 0, "pk", "hk"));
        batch.AddBlob(expected.back());

        batch.Pack();
        UNIT_ASSERT(batch.Packed);
        UNIT_ASSERT(batch.Header.GetFormat() == NKikimrPQ::TBatchHeader::EUncompressed);

        batch.Unpack();
        AssertClientBlobsEqual(expected, batch.Blobs);
        UNIT_ASSERT(batch.Blobs[0].IsBatch);
    }

    Y_UNIT_TEST(SerializeToHeaderInvariants) {
        TBatch batch(42, 0);
        batch.AddBlob(MakeSimpleBlob("src", 1, "data", 5, true));
        batch.Pack();

        TString serialized;
        batch.SerializeTo(serialized);

        const auto header = ExtractHeader(serialized.data(), serialized.size());
        UNIT_ASSERT_VALUES_EQUAL(header.GetOffset(), 42u);
        UNIT_ASSERT_VALUES_EQUAL(header.GetCount(), 5u);
        UNIT_ASSERT(header.HasOffsetDelta());
        UNIT_ASSERT_VALUES_EQUAL(header.GetOffsetDelta(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(
            sizeof(ui16) + header.ByteSize() + header.GetPayloadSize(),
            serialized.size());
        UNIT_ASSERT(static_cast<ui32>(header.ByteSize()) <= GetMaxHeaderSize());
    }

    Y_UNIT_TEST(ClearOffsetDeltaUsesNonDeltaFindPos) {
        TBatch batch(0, 0);
        batch.AddBlob(MakeSimpleBlob("src", 1, "a", 5));
        batch.AddBlob(MakeSimpleBlob("src", 2, "b"));
        UNIT_ASSERT(batch.HasOffsetDelta());

        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 0).BlobIdx, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(5, 0).BlobIdx, 1u);

        batch.ClearOffsetDelta();
        UNIT_ASSERT(!batch.HasOffsetDelta());
        // Without OffsetDelta, FindPos treats each blob as one offset slot —
        // offset 1 is the second blob, not an interior LMC slot.
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(0, 0).BlobIdx, 0u);
        UNIT_ASSERT_VALUES_EQUAL(batch.FindPos(1, 0).BlobIdx, 1u);
    }

    Y_UNIT_TEST(FindPosReturnsOffsetAndPartNo) {
        TBatch batch(10, 0);
        const auto ts = TInstant::Seconds(1);
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("p0"), TPartData{0, 2, 100},
            ts, ts, 100, "", ""));
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("p1"), TPartData{1, 2, 100},
            ts, ts, 100, "", ""));

        auto pos = batch.FindPos(10, 0);
        UNIT_ASSERT_VALUES_EQUAL(pos.BlobIdx, 0u);
        UNIT_ASSERT_VALUES_EQUAL(pos.Offset, 10u);
        UNIT_ASSERT_VALUES_EQUAL(pos.PartNo, 0u);

        pos = batch.FindPos(10, 1);
        UNIT_ASSERT_VALUES_EQUAL(pos.BlobIdx, 1u);
        UNIT_ASSERT_VALUES_EQUAL(pos.Offset, 10u);
        UNIT_ASSERT_VALUES_EQUAL(pos.PartNo, 1u);
    }

    Y_UNIT_TEST(FromBlobsPreservesOrderAndCount) {
        std::deque<TClientBlob> blobs;
        blobs.push_back(MakeSimpleBlob("a", 1, "x", 2));
        blobs.push_back(MakeSimpleBlob("b", 1, "y"));

        auto batch = TBatch::FromBlobs(100, std::move(blobs));
        UNIT_ASSERT_VALUES_EQUAL(batch.GetOffset(), 100u);
        UNIT_ASSERT_VALUES_EQUAL(batch.GetCount(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].SourceId, "a");
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[1].SourceId, "b");
    }

    Y_UNIT_TEST(IsGreaterThanAndEmpty) {
        TBatch empty;
        UNIT_ASSERT(empty.Empty());

        TBatch batch(10, 2);
        UNIT_ASSERT(batch.Empty());
        UNIT_ASSERT(batch.IsGreaterThan(9, 0));
        UNIT_ASSERT(batch.IsGreaterThan(10, 1));
        UNIT_ASSERT(!batch.IsGreaterThan(10, 2));
        UNIT_ASSERT(!batch.IsGreaterThan(11, 0));
    }

    Y_UNIT_TEST(SingleMessageDoesNotSetOffsetDelta) {
        TBatch batch(0, 0);
        batch.AddBlob(MakeSimpleBlob("src", 1, "data"));
        UNIT_ASSERT(!batch.HasOffsetDelta());
        UNIT_ASSERT_VALUES_EQUAL(batch.GetCount(), 1u);
        UNIT_ASSERT(!batch.Header.HasClientBlobCount());
    }
}

Y_UNIT_TEST_SUITE(BlobIterator) {
    TString AppendPackedBatch(TString& blob, TBatch& batch) {
        batch.Pack();
        batch.SerializeTo(blob);
        return blob;
    }

    Y_UNIT_TEST(MultiBatchRoundtrip) {
        TString blob;
        ui32 totalCount = 0;
        ui16 totalInternal = 0;

        {
            TBatch batch(100, 0);
            batch.AddBlob(MakeSimpleBlob("a", 1, "d1"));
            batch.AddBlob(MakeSimpleBlob("b", 1, "d2", 3, true));
            totalCount += batch.GetCount();
            totalInternal += batch.GetInternalPartsCount();
            AppendPackedBatch(blob, batch);
        }
        {
            TBatch batch(100 + totalCount, 0);
            const auto ts = TInstant::Seconds(1);
            batch.AddBlob(TClientBlob(
                TString("c"), 1, TString("p0"), TPartData{0, 2, 20},
                ts, ts, 20, "", ""));
            batch.AddBlob(TClientBlob(
                TString("c"), 1, TString("p1"), TPartData{1, 2, 20},
                ts, ts, 20, "", ""));
            totalCount += batch.GetCount();
            totalInternal += batch.GetInternalPartsCount();
            AppendPackedBatch(blob, batch);
        }

        auto key = TKey::ForBody(
            TKeyPrefix::TypeData, TPartitionId(0), 100, 0, totalCount, totalInternal);

        TClientBlob::CheckBlob(key, blob);

        auto batches = GetUnpackedBatches(key, blob);
        UNIT_ASSERT_VALUES_EQUAL(batches.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(batches[0].GetOffset(), 100u);
        UNIT_ASSERT_VALUES_EQUAL(batches[0].Blobs.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(batches[0].Blobs[0].Data, "d1");
        UNIT_ASSERT_VALUES_EQUAL(batches[0].Blobs[1].LogicalMessageCount, 3u);
        UNIT_ASSERT(batches[0].Blobs[1].IsBatch);
        UNIT_ASSERT_VALUES_EQUAL(batches[1].Blobs.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(batches[1].Blobs[0].GetPartNo(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(batches[1].Blobs[1].GetPartNo(), 1u);
    }

    Y_UNIT_TEST(IteratorWalksAllBatches) {
        TString blob;
        TBatch batch1(0, 0);
        batch1.AddBlob(MakeSimpleBlob("s", 1, "one"));
        AppendPackedBatch(blob, batch1);

        TBatch batch2(1, 0);
        batch2.AddBlob(MakeSimpleBlob("s", 2, "two"));
        AppendPackedBatch(blob, batch2);

        auto key = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId(7), 0, 0, 2, 0);

        ui32 seen = 0;
        for (TBlobIterator it(key, blob); it.IsValid(); it.Next()) {
            auto batch = it.GetBatch();
            UNIT_ASSERT(batch.Packed);
            batch.Unpack();
            UNIT_ASSERT_VALUES_EQUAL(batch.Blobs.size(), 1u);
            ++seen;
        }
        UNIT_ASSERT_VALUES_EQUAL(seen, 2u);
    }

    Y_UNIT_TEST(SingleBatchWithNonZeroPartNo) {
        TString blob;
        const auto ts = TInstant::Seconds(1);
        TBatch batch(50, 1);
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("mid"), TPartData{1, 3, 90},
            ts, ts, 90, "", ""));
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("last"), TPartData{2, 3, 90},
            ts, ts, 90, "", ""));
        AppendPackedBatch(blob, batch);

        auto key = TKey::ForBody(
            TKeyPrefix::TypeData, TPartitionId(0), 50, 1,
            batch.GetCount(), batch.GetInternalPartsCount());

        auto batches = GetUnpackedBatches(key, blob);
        UNIT_ASSERT_VALUES_EQUAL(batches.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(batches[0].GetPartNo(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(batches[0].Blobs.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(batches[0].GetCount(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(batches[0].GetInternalPartsCount(), 1u);
    }
}

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

} // namespace NKikimr::NPQ
