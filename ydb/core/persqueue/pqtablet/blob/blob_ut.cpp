#include "blob_int.h"
#include "blob.h"

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
        lhs.ExplicitHashKey == rhs.ExplicitHashKey;
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
