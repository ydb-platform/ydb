#include <yt/yt/client/table_client/columnar.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <array>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TBuildValidityBitmapFromDictionaryIndexesWithZeroNullTest, Empty)
{
    BuildValidityBitmapFromDictionaryIndexesWithZeroNull(
        TRange<ui32>(),
        TMutableRef());
}

TEST(TBuildValidityBitmapFromDictionaryIndexesWithZeroNullTest, LessThanByte)
{
    std::array<ui32, 6> indexes{0, 0, 1, 3, 4, 0};
    ui8 result;
    BuildValidityBitmapFromDictionaryIndexesWithZeroNull(
        TRange(indexes),
        TMutableRef(&result, 1));
    EXPECT_EQ(0x1c, result);
}

TEST(TBuildValidityBitmapFromDictionaryIndexesWithZeroNullTest, TenBytes)
{
    std::array<ui32, 80> indexes;
    for (int i = 0; i < std::ssize(indexes); ++i) {
        indexes[i] = i % 2;
    }
    std::array<ui8, indexes.size() / 8> result;
    BuildValidityBitmapFromDictionaryIndexesWithZeroNull(
        TRange(indexes),
        TMutableRef(&result, result.size()));
    for (int i = 0; i < std::ssize(result); ++i) {
        EXPECT_EQ(0xaa, result[i]);
    }
}

TEST(TBuildValidityBitmapFromDictionaryIndexesWithZeroNullTest, ManyBits)
{
    std::array<ui32, 8001> indexes;
    for (int i = 0; i < std::ssize(indexes); ++i) {
        indexes[i] = i % 2;
    }
    std::array<ui8, indexes.size() / 8 + 1> result;
    BuildValidityBitmapFromDictionaryIndexesWithZeroNull(
        TRange(indexes),
        TMutableRef(&result, result.size()));
    for (int i = 0; i < std::ssize(result) - 1; ++i) {
        EXPECT_EQ(0xaa, result[i]);
    }
    EXPECT_EQ(0, result[result.size() - 1]);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBuildDictionaryIndexesFromDictionaryIndexesWithZeroNullTest, Empty)
{
    BuildDictionaryIndexesFromDictionaryIndexesWithZeroNull(
        TRange<ui32>(),
        TMutableRange<ui32>());
}

TEST(TBuildDictionaryIndexesFromDictionaryIndexesWithZeroNullTest, NonEmpty)
{
    std::array<ui32, 6> indexes{0, 1, 2, 3, 4, 5};
    std::array<ui32, indexes.size()> result;
    BuildDictionaryIndexesFromDictionaryIndexesWithZeroNull(
        TRange(indexes),
        TMutableRange(result));
    for (int i = 1; i < std::ssize(result); ++i) {
        EXPECT_EQ(indexes[i] - 1, result[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCountNullsInDictionaryIndexesWithZeroNullTest, Empty)
{
    EXPECT_EQ(0, CountNullsInDictionaryIndexesWithZeroNull(TRange<ui32>()));
}

TEST(TCountNullsInDictionaryIndexesWithZeroNullTest, NonEmpty)
{
    std::array<ui32, 6> indexes{0, 1, 2, 0, 4, 5};
    EXPECT_EQ(2, CountNullsInDictionaryIndexesWithZeroNull(TRange(indexes)));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCountOnesInBitmapTest, Empty)
{
    EXPECT_EQ(0, CountOnesInBitmap(TRef(), 0, 0));
}

TEST(TCountOnesInBitmapTest, SingleByte)
{
    ui8 byte = 0xFF;
    for (int i = 0; i < 8; ++i) {
        for (int j = i; j < 8; ++j) {
            EXPECT_EQ(j - i, CountOnesInBitmap(TRef(&byte, 1), i, j))
                << "i = " << i << " j = " << j;
        }
    }
}

TEST(TCountOnesInBitmapTest, SevenBytes)
{
    std::array<ui8, 7> bytes = {0, 0xff, 0xff, 0xff, 0, 0, 1};
    auto bitmap = TRef(bytes.data(), bytes.size());
    EXPECT_EQ(25, CountOnesInBitmap(bitmap, 0, 56));
}

TEST(TCountOnesInBitmapTest, ThreeQwords)
{
    std::array<ui64, 3> qwords = {1, 1, 1};
    auto bitmap = TRef(qwords.data(), 8 * qwords.size());
    EXPECT_EQ(3, CountOnesInBitmap(bitmap, 0, 64 * 3));
}

TEST(TCountOnesInBitmapTest, QwordBorder)
{
    std::array<ui64, 2> qwords = {0xffffffffffffffffULL, 0xffffffffffffffffULL};
    auto bitmap = TRef(qwords.data(), 8 * qwords.size());
    for (int i = 0; i < 10; ++i) {
        for (int j = 64; j < 74; ++j) {
            EXPECT_EQ(j - i, CountOnesInBitmap(bitmap, i, j))
                << "i = " << i << " j = " << j;
        }
    }
}

TEST(TCountOnesInBitmapTest, WholeQwordInTheMiddle)
{
    std::array<ui64, 3> qwords = {0xffffffffffffffffULL, 1, 0xffffffffffffffffULL};
    auto bitmap = TRef(qwords.data(), 8 * qwords.size());
    for (int i = 0; i < 10; ++i) {
        for (int j = 128; j < 138; ++j) {
            EXPECT_EQ(j - i - 63, CountOnesInBitmap(bitmap, i, j))
                << "i = " << i << " j = " << j;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCopyBitmapRangeToBitmapTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int>>
{ };

TEST_P(TCopyBitmapRangeToBitmapTest, CheckAll)
{
    auto [startIndex, endIndex] = GetParam();

    std::array<ui64, 3> qwords = {0x1234567812345678ULL, 0x1234567812345678ULL, 0xabcdabcdabcdabcdULL};
    auto bitmap = TRef(qwords.data(), 8 * qwords.size());

    constexpr ssize_t ResultByteSize = 24;
    auto byteCount = GetBitmapByteSize(endIndex - startIndex);
    EXPECT_LE(byteCount, ResultByteSize);

    std::array<ui8, ResultByteSize> guard;
    for (int i = 0; i < ResultByteSize; ++i) {
        guard[i] = i;
    }

    auto result = guard;
    auto resultNegated = guard;

    CopyBitmapRangeToBitmap(bitmap, startIndex, endIndex, TMutableRef(result.data(), byteCount));
    CopyBitmapRangeToBitmapNegated(bitmap, startIndex, endIndex, TMutableRef(resultNegated.data(), byteCount));

    auto getBit = [] (const auto& array, int index) {
        return (reinterpret_cast<const char*>(array.begin())[index / 8] & (1U << (index % 8))) != 0;
    };

    for (int i = startIndex; i < endIndex; ++i) {
        auto srcBit = getBit(bitmap, i);
        auto dstBit = getBit(result, i - startIndex);
        auto invertedDstBit = getBit(resultNegated, i - startIndex);
        EXPECT_EQ(srcBit, dstBit) << "i = " << i;
        EXPECT_NE(srcBit, invertedDstBit) << "i = " << i;
    }

    for (int i = byteCount; i < ResultByteSize; ++i) {
        EXPECT_EQ(guard[i], result[i]);
        EXPECT_EQ(guard[i], resultNegated[i]);
    }
}

INSTANTIATE_TEST_SUITE_P(
    TCopyBitmapRangeToBitmapTest,
    TCopyBitmapRangeToBitmapTest,
    ::testing::Values(
        std::tuple(  0,   0),
        std::tuple(  0,  64),
        std::tuple(  0, 192),
        std::tuple( 64, 128),
        std::tuple(  8,  16),
        std::tuple( 10,  13),
        std::tuple(  5, 120),
        std::tuple( 23,  67),
        std::tuple(  1, 191)));

////////////////////////////////////////////////////////////////////////////////

TEST(TTranslateRleIndexTest, CheckAll)
{
    std::array<ui64, 8> rleIndexes{0, 1, 3, 10, 11, 15, 20, 21};
    for (ui64 i = 0; i < rleIndexes[rleIndexes.size() - 1] + 10; ++i) {
        auto j = TranslateRleIndex(TRange(rleIndexes), i);
        EXPECT_LE(rleIndexes[j], i);
        if (j < std::ssize(rleIndexes) - 1) {
            EXPECT_LT(i, rleIndexes[j + 1]);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDecodeRleVectorTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int, std::vector<int>>>
{ };

TEST_P(TDecodeRleVectorTest, CheckAll)
{
    std::vector<int> values{1, 2, 3, 4, 5, 6};
    std::vector<ui64> rleIndexes{0, 1, 10, 11, 20, 25};
    const auto& [startIndex, endIndex, expected] = GetParam();
    std::vector<int> decoded;
    DecodeRawVector<int>(
        startIndex,
        endIndex,
        {},
        rleIndexes,
        [&] (auto index) {
            return values[index];
        },
        [&] (auto value) {
            decoded.push_back(value);
        });
    EXPECT_EQ(expected, decoded);
}

INSTANTIATE_TEST_SUITE_P(
    TDecodeRleVectorTest,
    TDecodeRleVectorTest,
    ::testing::Values(
        std::tuple(  0,   0, std::vector<int>{}),
        std::tuple(  0,   1, std::vector<int>{1}),
        std::tuple(  0,   5, std::vector<int>{1, 2, 2, 2, 2}),
        std::tuple(  9,  13, std::vector<int>{2, 3, 4, 4}),
        std::tuple( 20,  25, std::vector<int>{5, 5, 5, 5, 5}),
        std::tuple( 25,  27, std::vector<int>{6, 6}),
        std::tuple( 50,  53, std::vector<int>{6, 6, 6})));

////////////////////////////////////////////////////////////////////////////////

TEST(TDecodeIntegerValueTest, CheckAll)
{
    EXPECT_EQ(100, DecodeIntegerValue<int>(100,  0, false));
    EXPECT_EQ(100, DecodeIntegerValue<int>( 40, 60, false));
    EXPECT_EQ(  9, DecodeIntegerValue<int>( 18,  0, true));
    EXPECT_EQ(-10, DecodeIntegerValue<int>( 19,  0, true));
    EXPECT_EQ(  9, DecodeIntegerValue<int>(  1, 17, true));
    EXPECT_EQ(-10, DecodeIntegerValue<int>(  2, 17, true));
}

////////////////////////////////////////////////////////////////////////////////

class TDecodeStringsTest
    : public ::testing::Test
{
protected:
    std::vector<ui32> Offsets = {1, 2, 3, 4, 5};
    static constexpr ui32 AvgLength = 10;
    std::vector<ui32> Expected = {0, 9, 21, 28, 42, 47};
};

TEST_F(TDecodeStringsTest, Single)
{
    std::vector<ui32> result;
    for (int index = 0; index <= std::ssize(Offsets); ++index) {
        result.push_back(DecodeStringOffset(TRange(Offsets), AvgLength, index));
    }
    EXPECT_EQ(Expected, result);
}

TEST_F(TDecodeStringsTest, Multiple)
{
    for (int i = 0; i <= std::ssize(Offsets); ++i) {
        for (int j = i; j <= std::ssize(Offsets); ++j) {
            std::vector<ui32> result(j - i + 1);
            DecodeStringOffsets(TRange(Offsets), AvgLength, i, j, TMutableRange(result));
            std::vector<ui32> expected;
            for (int k = i; k <= j; ++k) {
                expected.push_back(Expected[k] - DecodeStringOffset(TRange(Offsets), AvgLength, i));
            }
            EXPECT_EQ(expected, result);
        }
    }
}

TEST_F(TDecodeStringsTest, PointersAndLengths)
{
    std::vector<char> chars(100);
    std::vector<const char*> strings(Offsets.size());
    std::vector<i32> lengths(Offsets.size());
    DecodeStringPointersAndLengths(
        TRange(Offsets),
        AvgLength,
        TRef(chars.data(), chars.size()),
        TMutableRange(strings),
        TMutableRange(lengths));
    for (int i = 0; i < std::ssize(Offsets); ++i) {
        EXPECT_EQ(Expected[i], strings[i] - chars.data());
        EXPECT_EQ(static_cast<ssize_t>(Expected[i + 1] - Expected[i]), lengths[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDecodeNullsFromRleDictionaryIndexesWithZeroNullTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int>>
{
protected:
    void SetUp() override
    {
        Expected.resize(800);
        std::fill(Expected.begin() +   3, Expected.begin() +   5, true);
        std::fill(Expected.begin() +  20, Expected.begin() + 100, true);
        std::fill(Expected.begin() + 200, Expected.begin() + 800, true);
    }


    std::vector<ui32> DictionaryIndexes = {0, 1, 0,  1,   0,   1};
    std::vector<ui64> RleIndexes        = {0, 3, 5, 20, 100, 200};
    std::vector<bool> Expected;
};

TEST_P(TDecodeNullsFromRleDictionaryIndexesWithZeroNullTest, ValidityBitmap)
{
    auto [startIndex, endIndex] = GetParam();
    std::vector<ui8> bitmap(GetBitmapByteSize(endIndex - startIndex));
    BuildValidityBitmapFromRleDictionaryIndexesWithZeroNull(
        TRange(DictionaryIndexes),
        TRange(RleIndexes),
        startIndex,
        endIndex,
        TMutableRef(bitmap.data(), bitmap.size()));
    for (int i = startIndex; i < endIndex; ++i) {
        EXPECT_EQ(Expected[i], GetBit(TRef(bitmap.data(), bitmap.size()), i - startIndex))
            << "i = " << i;
    }
}

TEST_P(TDecodeNullsFromRleDictionaryIndexesWithZeroNullTest, NullBytemap)
{
    auto [startIndex, endIndex] = GetParam();
    std::vector<ui8> bytemap(endIndex - startIndex);
    BuildNullBytemapFromRleDictionaryIndexesWithZeroNull(
        TRange(DictionaryIndexes),
        TRange(RleIndexes),
        startIndex,
        endIndex,
        TMutableRange(bytemap));
    for (int i = startIndex; i < endIndex; ++i) {
        EXPECT_TRUE(bytemap[i - startIndex] == 0 || bytemap[i - startIndex] == 1);
        EXPECT_EQ(Expected[i], bytemap[i - startIndex] == 0)
            << "i = " << i;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TDecodeNullsFromRleDictionaryIndexesWithZeroNullTest,
    TDecodeNullsFromRleDictionaryIndexesWithZeroNullTest,
    ::testing::Values(
        std::tuple(  0,   0),
        std::tuple(  0, 800),
        std::tuple(256, 512),
        std::tuple( 10,  20),
        std::tuple( 10,  20),
        std::tuple( 20, 100),
        std::tuple( 90, 110)));

////////////////////////////////////////////////////////////////////////////////

class TBuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNullTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int>>
{
protected:
    std::vector<ui32> DictionaryIndexes = {0, 1, 0,  2,  3};
    std::vector<ui64> RleIndexes        = {0, 3, 5, 10, 12};

    static constexpr ui32 Z = static_cast<ui32>(-1);
    std::vector<ui32> Expected = {Z, Z, Z, 0, 0, Z, Z, Z, Z, Z, 1, 1, 2, 2, 2};
};

TEST_P(TBuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNullTest, CheckAll)
{
    auto [startIndex, endIndex] = GetParam();
    std::vector<ui32> result(endIndex - startIndex);
    BuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNull(
        TRange(DictionaryIndexes),
        TRange(RleIndexes),
        startIndex,
        endIndex,
        TMutableRange(result));
    for (int i = startIndex; i < endIndex; ++i) {
        EXPECT_EQ(Expected[i], result[i - startIndex])
            << "i = " << i;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TBuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNullTest,
    TBuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNullTest,
    ::testing::Values(
        std::tuple(  0,   0),
        std::tuple(  0,  15),
        std::tuple(  3,   5),
        std::tuple(  1,  10),
        std::tuple( 13,  15)));

////////////////////////////////////////////////////////////////////////////////

class TBuildIotaDictionaryIndexesFromRleIndexesTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int, std::vector<ui32>>>
{
protected:
    std::vector<ui64> RleIndexes = {0, 3, 5, 10, 12};
};

TEST_P(TBuildIotaDictionaryIndexesFromRleIndexesTest, CheckAll)
{
    auto [startIndex, endIndex, expected] = GetParam();
    std::vector<ui32> result(endIndex - startIndex);
    BuildIotaDictionaryIndexesFromRleIndexes(
        TRange(RleIndexes),
        startIndex,
        endIndex,
        TMutableRange(result));
    EXPECT_EQ(expected, result);
}

INSTANTIATE_TEST_SUITE_P(
    TBuildIotaDictionaryIndexesFromRleIndexesTest,
    TBuildIotaDictionaryIndexesFromRleIndexesTest,
    ::testing::Values(
        std::tuple(  0,   0, std::vector<ui32>{}),
        std::tuple(  0,  15, std::vector<ui32>{0, 0, 0, 1, 1, 2, 2, 2, 2, 2, 3, 3, 4, 4, 4}),
        std::tuple(  3,   5, std::vector<ui32>{0, 0}),
        std::tuple(  1,  10, std::vector<ui32>{0, 0, 1, 1, 2, 2, 2, 2, 2}),
        std::tuple( 13,  15, std::vector<ui32>{0, 0})));

////////////////////////////////////////////////////////////////////////////////

class TCountNullsInDictionaryIndexesWithZeroNullTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<std::vector<ui32>, int>>
{ };

TEST_P(TCountNullsInDictionaryIndexesWithZeroNullTest, CheckAll)
{
    const auto& [indexes, expected] = GetParam();
    EXPECT_EQ(expected, CountNullsInDictionaryIndexesWithZeroNull(TRange(indexes)));
}

INSTANTIATE_TEST_SUITE_P(
    TCountNullsInDictionaryIndexesWithZeroNullTest,
    TCountNullsInDictionaryIndexesWithZeroNullTest,
    ::testing::Values(
        std::tuple(std::vector<ui32>{}, 0),
        std::tuple(std::vector<ui32>{0, 0, 0}, 3),
        std::tuple(std::vector<ui32>{1, 2, 3}, 0),
        std::tuple(std::vector<ui32>{1, 0, 3}, 1)));

////////////////////////////////////////////////////////////////////////////////

class TCountOnesInRleBitmapTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int, int>>
{
protected:
    std::vector<ui64> RleIndexes = {0, 3, 5, 20, 50};
    ui8 Bitmap = 0b10101;
};

TEST_P(TCountOnesInRleBitmapTest, CheckAll)
{
    auto [startIndex, endIndex, expected] = GetParam();
    EXPECT_EQ(expected, CountOnesInRleBitmap(TRef(&Bitmap, 1), TRange(RleIndexes), startIndex, endIndex));
}

INSTANTIATE_TEST_SUITE_P(
    TCountOnesInRleBitmapTest,
    TCountOnesInRleBitmapTest,
    ::testing::Values(
        std::tuple(  0,   0,   0),
        std::tuple( 50,  60,  10),
        std::tuple( 40,  60,  10),
        std::tuple( 60, 100,  40),
        std::tuple(  3,   5,   0),
        std::tuple(  2,   6,   2)));

////////////////////////////////////////////////////////////////////////////////

class TDecodeNullsFromRleNullBitmapTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int>>
{
protected:
    void SetUp() override
    {
        Expected.resize(800);
        std::fill(Expected.begin() +   3, Expected.begin() +   5, true);
        std::fill(Expected.begin() +  20, Expected.begin() + 100, true);
        std::fill(Expected.begin() + 200, Expected.begin() + 800, true);
    }


    std::vector<ui64> RleIndexes = {0, 3, 5, 20, 100, 200};
    ui8 Bitmap = 0b010101;
    std::vector<bool> Expected;
};

TEST_P(TDecodeNullsFromRleNullBitmapTest, ValidityBitmap)
{
    auto [startIndex, endIndex] = GetParam();
    std::vector<ui8> bitmap(GetBitmapByteSize(endIndex - startIndex));
    BuildValidityBitmapFromRleNullBitmap(
        TRef(&Bitmap, 1),
        TRange(RleIndexes),
        startIndex,
        endIndex,
        TMutableRef(bitmap.data(), bitmap.size()));
    for (int i = startIndex; i < endIndex; ++i) {
        EXPECT_EQ(Expected[i], GetBit(TRef(bitmap.data(), bitmap.size()), i - startIndex))
            << "i = " << i;
    }
}

TEST_P(TDecodeNullsFromRleNullBitmapTest, NullBytemap)
{
    auto [startIndex, endIndex] = GetParam();
    std::vector<ui8> bytemap(endIndex - startIndex);
    BuildNullBytemapFromRleNullBitmap(
        TRef(&Bitmap, 1),
        TRange(RleIndexes),
        startIndex,
        endIndex,
        TMutableRange(bytemap));
    for (int i = startIndex; i < endIndex; ++i) {
        EXPECT_TRUE(bytemap[i - startIndex] == 0 || bytemap[i - startIndex] == 1);
        EXPECT_EQ(Expected[i], bytemap[i - startIndex] == 0)
            << "i = " << i;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TDecodeNullsFromRleNullBitmapTest,
    TDecodeNullsFromRleNullBitmapTest,
    ::testing::Values(
        std::tuple(  0,   0),
        std::tuple(  0, 800),
        std::tuple(256, 512),
        std::tuple( 10,  20),
        std::tuple( 10,  20),
        std::tuple( 20, 100),
        std::tuple( 90, 110)));

////////////////////////////////////////////////////////////////////////////////

class TDecodeBytemapFromBitmapTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int>>
{
protected:
    void SetUp() override
    {
        Expected.resize(800);
        std::fill(Expected.begin() +   3, Expected.begin() +   5, true);
        std::fill(Expected.begin() +  20, Expected.begin() + 100, true);
        std::fill(Expected.begin() + 200, Expected.begin() + 800, true);

        Bitmap.resize(GetBitmapByteSize(Expected.size()));
        for (int i = 0; i < std::ssize(Expected); ++i) {
            SetBit(TMutableRef(Bitmap.data(), Bitmap.size()), i, Expected[i]);
        }
    }

    std::vector<bool> Expected;
    std::vector<ui8> Bitmap;
};

TEST_P(TDecodeBytemapFromBitmapTest, CheckAll)
{
    auto [startIndex, endIndex] = GetParam();
    std::vector<ui8> bytemap(endIndex - startIndex);
    DecodeBytemapFromBitmap(
        TRef(Bitmap.data(), Bitmap.size()),
        startIndex,
        endIndex,
        TMutableRange(bytemap));
    for (int i = startIndex; i < endIndex; ++i) {
        EXPECT_TRUE(bytemap[i - startIndex] == 0 || bytemap[i - startIndex] == 1);
        EXPECT_EQ(Expected[i], bytemap[i - startIndex])
            << "i = " << i;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TDecodeBytemapFromBitmapTest,
    TDecodeBytemapFromBitmapTest,
    ::testing::Values(
        std::tuple(  0,    0),
        std::tuple(  18,  19),
        std::tuple(  0,  512),
        std::tuple(  0,  800),
        std::tuple(256,  512),
        std::tuple( 10,   20),
        std::tuple( 10,   20),
        std::tuple( 20,  100),
        std::tuple( 90,  110)));

////////////////////////////////////////////////////////////////////////////////

class TCountTotalStringLengthInRleDictionaryIndexesWithZeroNullTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int>>
{
protected:
    void SetUp() override
    {
        int j = -1;
        for (int i = 0; i < 40; ++i) {
            if (j + 1 < std::ssize(RleIndexes) && i == static_cast<ssize_t>(RleIndexes[j + 1])) {
                ++j;
            }
            auto dictionaryIndex = DictionaryIndexes[j];
            if (dictionaryIndex != 0) {
                auto stringIndex = dictionaryIndex - 1;
                DecodedOffsets.push_back(DecodeStringOffset(Offsets, AvgLength, stringIndex));
            } else {
                DecodedOffsets.push_back(DecodedOffsets.empty() ? 0 : DecodedOffsets.back());
            }
        }

        for (int i = 0; i < std::ssize(Offsets); ++i) {
            auto [startOffset, endOffset] = DecodeStringRange(
                TRange(Offsets),
                AvgLength,
                i);
            Lengths.push_back(endOffset - startOffset);
        }
    }

    std::vector<ui64> RleIndexes = {0, 1, 3, 10, 15, 16, 18};
    std::vector<ui32> DictionaryIndexes = {0, 1, 0, 2, 3, 4, 5};
    std::vector<ui32> Offsets = {1, 2, 3, 4, 5};
    static constexpr ui32 AvgLength = 10;
    std::vector<i32> Lengths;
    std::vector<i64> DecodedOffsets;
};

TEST_P(TCountTotalStringLengthInRleDictionaryIndexesWithZeroNullTest, CheckAll)
{
    auto [startIndex, endIndex] = GetParam();

    auto actual = CountTotalStringLengthInRleDictionaryIndexesWithZeroNull(
        TRange(DictionaryIndexes),
        TRange(RleIndexes),
        TRange(Lengths),
        startIndex,
        endIndex);

    i64 expected = 0;
    for (int i = startIndex; i < endIndex; ++i) {
        int j = TranslateRleIndex(TRange(RleIndexes), i);
        auto k = DictionaryIndexes[j];
        if (k != 0) {
            auto [startOffset, endOffset] = DecodeStringRange(TRange(Offsets), AvgLength, k - 1);
            expected += (endOffset - startOffset);
        }
    }

    EXPECT_EQ(expected, actual);
}

INSTANTIATE_TEST_SUITE_P(
    TCountTotalStringLengthInRleDictionaryIndexesWithZeroNullTest,
    TCountTotalStringLengthInRleDictionaryIndexesWithZeroNullTest,
    ::testing::Values(
        std::tuple(  0,   0),
        std::tuple(  0,  30),
        std::tuple(  1,   3),
        std::tuple(  5,  10),
        std::tuple(  4,  25),
        std::tuple(  2,   4)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
