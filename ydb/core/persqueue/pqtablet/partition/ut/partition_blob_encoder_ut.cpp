#include <ydb/core/persqueue/pqtablet/partition/partition_blob_encoder.h>
#include <ydb/core/persqueue/pqtablet/partition/partition_util.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NKikimr::NPQ;

namespace {

TBlobKeyTokenPtr MakeBlobKeyToken(const TString& key)
{
    auto token = std::make_shared<TBlobKeyToken>();
    token->Key = key;
    return token;
}

TString MakePayload(ui32 batchNo, size_t size)
{
    TString payload;
    payload.reserve(size);
    for (size_t i = 0; i < size; ++i) {
        payload.push_back(static_cast<char>(1 + (i * 131 + batchNo * 29) % 251));
    }
    return payload;
}

TString MakeDataHeadValue(ui64 firstOffset, ui32 batchCount, size_t payloadSize)
{
    TString value;
    value.reserve(batchCount * (payloadSize + 128));
    const auto ts = TInstant::Seconds(1000);

    for (ui32 batchNo = 0; batchNo < batchCount; ++batchNo) {
        TBatch batch(firstOffset + batchNo, 0);
        batch.AddBlob(TClientBlob(
            TString("sourceId"),
            batchNo + 1,
            MakePayload(batchNo, payloadSize),
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

void LoadSharedHead(THead& head, const TKey& key, const TString& value)
{
    head.Offset = key.GetOffset();
    head.PartNo = key.GetPartNo();

    for (TBlobIterator it(key, value, TBlobIterator::EDataOwnership::Shared); it.IsValid(); it.Next()) {
        head.AddBatch(it.GetBatch());
    }
    head.PackedSize += value.size();
}

TDataKey MakeDataKey(const TKey& key, ui32 size)
{
    return {
        .Key = key,
        .Size = size,
        .Timestamp = TInstant::Seconds(1000),
        .CumulativeSize = 0,
        .BlobKeyToken = MakeBlobKeyToken(key.ToString()),
    };
}

TKey MakeHeadKey(ui64 offset, ui32 count)
{
    return TKey::ForHead(
        TKeyPrefix::TypeData,
        TPartitionId(0),
        offset,
        0,
        count,
        0
    );
}

} // namespace

TEST(TPartitionBlobEncoderTest, SyncNewHeadKeyMaterializesSparseSharedHeadAfterKeyReplacement)
{
    constexpr ui32 BatchesPerDataHeadBlob = 64;
    constexpr size_t PayloadSize = 1_KB;

    TPartitionBlobEncoder encoder(TPartitionId(0), false);
    const auto key = MakeHeadKey(1000, BatchesPerDataHeadBlob);
    const TString value = MakeDataHeadValue(key.GetOffset(), BatchesPerDataHeadBlob, PayloadSize);

    LoadSharedHead(encoder.Head, key, value);
    encoder.HeadKeys.push_back(MakeDataKey(key, value.size()));

    while (encoder.Head.GetBatches().size() > 1) {
        encoder.Head.ExtractFirstBatch(false);
    }

    ASSERT_TRUE(encoder.Head.GetLastBatch().PackedData.IsShared());

    encoder.NewHeadKey = MakeDataKey(key, encoder.Head.GetLastBatch().GetPackedSize());
    encoder.SyncNewHeadKey();

    EXPECT_FALSE(encoder.Head.GetLastBatch().PackedData.IsShared());
}

TEST(TPartitionBlobEncoderTest, SyncNewHeadKeyKeepsDenseSharedHeadAfterKeyReplacement)
{
    constexpr ui32 BatchesPerDataHeadBlob = 64;
    constexpr size_t PayloadSize = 1_KB;

    TPartitionBlobEncoder encoder(TPartitionId(0), false);
    const auto key = MakeHeadKey(1000, BatchesPerDataHeadBlob);
    const TString value = MakeDataHeadValue(key.GetOffset(), BatchesPerDataHeadBlob, PayloadSize);

    LoadSharedHead(encoder.Head, key, value);
    encoder.HeadKeys.push_back(MakeDataKey(key, value.size()));

    ASSERT_TRUE(encoder.Head.GetLastBatch().PackedData.IsShared());

    encoder.NewHeadKey = MakeDataKey(key, value.size());
    encoder.SyncNewHeadKey();

    EXPECT_TRUE(encoder.Head.GetLastBatch().PackedData.IsShared());
}

TEST(TPartitionBlobEncoderTest, SyncNewHeadKeyKeepsMostlyLiveSharedHeadAfterKeyReplacement)
{
    constexpr ui32 BatchesPerDataHeadBlob = 64;
    constexpr size_t PayloadSize = 1_KB;

    TPartitionBlobEncoder encoder(TPartitionId(0), false);
    const auto key = MakeHeadKey(1000, BatchesPerDataHeadBlob);
    const TString value = MakeDataHeadValue(key.GetOffset(), BatchesPerDataHeadBlob, PayloadSize);

    LoadSharedHead(encoder.Head, key, value);
    encoder.HeadKeys.push_back(MakeDataKey(key, value.size()));

    encoder.Head.ExtractFirstBatch(false);

    ASSERT_TRUE(encoder.Head.GetLastBatch().PackedData.IsShared());

    encoder.NewHeadKey = MakeDataKey(key, value.size() - PayloadSize);
    encoder.SyncNewHeadKey();

    EXPECT_TRUE(encoder.Head.GetLastBatch().PackedData.IsShared());
}

TEST(TPartitionBlobEncoderTest, SyncNewHeadKeyDefersSparseSharedHeadWhenCompactedKeysWillClearHead)
{
    constexpr ui32 BatchesPerDataHeadBlob = 64;
    constexpr size_t PayloadSize = 1_KB;

    TPartitionBlobEncoder encoder(TPartitionId(0), false);
    const auto key = MakeHeadKey(1000, BatchesPerDataHeadBlob);
    const TString value = MakeDataHeadValue(key.GetOffset(), BatchesPerDataHeadBlob, PayloadSize);

    LoadSharedHead(encoder.Head, key, value);
    encoder.HeadKeys.push_back(MakeDataKey(key, value.size()));

    while (encoder.Head.GetBatches().size() > 1) {
        encoder.Head.ExtractFirstBatch(false);
    }

    ASSERT_TRUE(encoder.Head.GetLastBatch().PackedData.IsShared());

    encoder.CompactedKeys.emplace_back(TKey::ForBody(
        TKeyPrefix::TypeData,
        TPartitionId(0),
        key.GetOffset(),
        key.GetPartNo(),
        key.GetCount(),
        key.GetInternalPartsCount()
    ), value.size());
    encoder.NewHeadKey = MakeDataKey(key, encoder.Head.GetLastBatch().GetPackedSize());
    encoder.SyncNewHeadKey();

    ASSERT_TRUE(encoder.Head.GetLastBatch().PackedData.IsShared());

    encoder.SyncHeadFromNewHead();

    EXPECT_TRUE(encoder.Head.GetBatches().empty());
}
