#include "blob_int.h"
#include "blob.h"

#include <library/cpp/testing/unittest/registar.h>

#include <type_traits>

namespace NKikimr::NPQ {
namespace {

template <class T, class = void>
struct THasDataOwnership : std::false_type {
};

template <class T>
struct THasDataOwnership<T, std::void_t<typename T::EDataOwnership>> : std::true_type {
};

template <class T = TBlobIterator>
std::enable_if_t<THasDataOwnership<T>::value, T> MakeInitPathIterator(const TKey& key, const TString& blob)
{
    return T(key, blob, T::EDataOwnership::Shared);
}

template <class T = TBlobIterator>
std::enable_if_t<!THasDataOwnership<T>::value, T> MakeInitPathIterator(const TKey& key, const TString& blob)
{
    return T(key, blob);
}

template <class T, class = void>
struct THasIsShared : std::false_type {
};

template <class T>
struct THasIsShared<T, std::void_t<decltype(std::declval<const T&>().IsShared())>> : std::true_type {
};

template <class T>
bool IsPackedDataShared(const T& packedData, std::true_type)
{
    return packedData.IsShared();
}

template <class T>
bool IsPackedDataShared(const T&, std::false_type)
{
    return false;
}

bool IsBatchPayloadShared(const TBatch& batch)
{
    return IsPackedDataShared(batch.PackedData, THasIsShared<decltype(batch.PackedData)>());
}

TString MakePayload(ui32 blobNo, ui32 batchNo, size_t size)
{
    TString payload;
    payload.reserve(size);
    for (size_t i = 0; i < size; ++i) {
        payload.push_back(static_cast<char>(1 + (i * 131 + blobNo * 17 + batchNo * 29) % 251));
    }
    return payload;
}

TString MakeDataHeadValue(ui64 firstOffset, ui32 blobNo, ui32 batchCount, size_t payloadSize)
{
    TString value;
    value.reserve(batchCount * (payloadSize + 128));
    const auto ts = TInstant::Seconds(1000 + blobNo);

    for (ui32 batchNo = 0; batchNo < batchCount; ++batchNo) {
        TBatch batch(firstOffset + batchNo, 0);
        batch.AddBlob(TClientBlob(
            TString("sourceId"),
            batchNo + 1,
            MakePayload(blobNo, batchNo, payloadSize),
            TMaybe<TPartData>(),
            ts,
            ts,
            0,
            "",
            ""
        ));
        batch.Pack();
        batch.SerializeTo(value);
    }

    return value;
}

void LoadHeadFromDataHeadBlob(THead& head, const TKey& key, const TString& value)
{
    head.Offset = key.GetOffset();
    head.PartNo = key.GetPartNo();

    for (auto it = MakeInitPathIterator(key, value); it.IsValid(); it.Next()) {
        head.AddBatch(it.GetBatch());
    }
    head.PackedSize += value.size();
}

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
        UNIT_ASSERT(batch.GetUnpackedSize() > 0);

        batch.Pack();
        UNIT_ASSERT(batch.Packed);
        UNIT_ASSERT(batch.PackedData.Size() > 0);
        UNIT_ASSERT(batch.PackedData.Capacity() > 0);

        batch.Unpack();
        UNIT_ASSERT(!batch.Packed);
        UNIT_ASSERT_VALUES_EQUAL(batch.PackedData.Size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(batch.PackedData.Capacity(), 0);
        UNIT_ASSERT(!batch.Blobs.empty());
    }

    Y_UNIT_TEST(DroppedHeadBatchesDoNotKeepOriginalDataHeadBlobs) {
        constexpr ui32 DataHeadBlobCount = 64;
        constexpr ui32 BatchesPerDataHeadBlob = 64;
        constexpr size_t PayloadSize = 1_KB;

        TVector<THead> heads;
        heads.reserve(DataHeadBlobCount);

        ui64 nextOffset = 1000;
        for (ui32 blobNo = 0; blobNo < DataHeadBlobCount; ++blobNo) {
            const auto key = TKey::ForHead(
                TKeyPrefix::TypeData,
                TPartitionId(0),
                nextOffset,
                0,
                BatchesPerDataHeadBlob,
                0
            );
            const TString value = MakeDataHeadValue(nextOffset, blobNo, BatchesPerDataHeadBlob, PayloadSize);

            heads.emplace_back();
            auto& head = heads.back();
            LoadHeadFromDataHeadBlob(head, key, value);

            while (head.GetBatches().size() > 1) {
                head.ExtractFirstBatch();
            }

            nextOffset += BatchesPerDataHeadBlob;
        }

        ui32 sharedBatches = 0;
        for (const auto& head : heads) {
            UNIT_ASSERT_VALUES_EQUAL(head.GetBatches().size(), 1);
            sharedBatches += IsBatchPayloadShared(head.GetLastBatch()) ? 1 : 0;
        }

        UNIT_ASSERT_VALUES_EQUAL_C(
            sharedBatches,
            0,
            "A small live suffix of each DataHead blob must not keep the whole original DataHead value in memory"
        );
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
