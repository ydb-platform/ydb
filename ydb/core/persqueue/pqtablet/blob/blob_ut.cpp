#include "blob_int.h"
#include "blob.h"
#include "header.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

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
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].MessageCount, 5u);
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
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].MessageCount, 5u);
    }


    Y_UNIT_TEST(MessageMetadataPackUnpack) {
        TBatch batch(100, 0);
        auto ts = TInstant::Seconds(100);
        batch.AddBlob(TClientBlob(
            TString("src"), 1, TString("data"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 5, EMessageFormat::KAFKA_BATCH
        ));

        batch.Pack();
        batch.Unpack();

        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].MessageCount, 5u);
        UNIT_ASSERT(batch.Blobs[0].MessageFormat == EMessageFormat::KAFKA_BATCH);
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

        auto makeBlob = [&](ui64 seqNo, ui32 messageCount = 1) {
            return TClientBlob(
                TString("src"), seqNo, TString("data"), TMaybe<TPartData>(),
                ts, ts, 0, "", "", messageCount);
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

        UNIT_ASSERT_VALUES_EQUAL(batch.Blobs[0].MessageCount, 5u);
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
        lhs.MessageCount == rhs.MessageCount &&
        lhs.MessageFormat == rhs.MessageFormat;
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


    Y_UNIT_TEST(MessageMetadataStoresFormatInLowerBits) {
        TBuffer buffer;
        buffer.Reserve(8_MB);

        auto ts = TInstant::Seconds(100);
        TClientBlob blob(
            TString("src"), 42, TString("payload"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 7, EMessageFormat::KAFKA_BATCH);

        Serialize(blob, buffer);

        const char* data = buffer.data();
        data += sizeof(ui32); // total size
        data += sizeof(ui64); // seqNo
        data += sizeof(ui8);  // flags
        const ui32 messageMetadata = ReadUnaligned<ui32>(data);

        UNIT_ASSERT_VALUES_EQUAL(messageMetadata, (7u << MESSAGE_FORMAT_BITS) | static_cast<ui32>(EMessageFormat::KAFKA_BATCH));
    }

    Y_UNIT_TEST(SerializeAndDeserializeMessageMetadata) {
        TBuffer buffer;
        buffer.Reserve(8_MB);

        auto ts = TInstant::Seconds(100);
        TClientBlob blob(
            TString("src"), 42, TString("payload"), TMaybe<TPartData>(),
            ts, ts, 0, "", "", 7, EMessageFormat::KAFKA_BATCH);

        Serialize(blob, buffer);
        TClientBlob deserialized = DeserializeClientBlob(buffer.data(), buffer.size());

        UNIT_ASSERT(blob == deserialized);
        UNIT_ASSERT_VALUES_EQUAL(deserialized.MessageCount, 7u);
        UNIT_ASSERT(deserialized.MessageFormat == EMessageFormat::KAFKA_BATCH);
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
