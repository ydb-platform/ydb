#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/public/udf/arrow/bit_util.h>
#include <util/random/random.h>

#include <arrow/util/bit_util.h>

using namespace NYql::NUdf;

namespace {
void PerformTest(const ui8* testData, size_t offset, size_t length) {
    std::vector<ui8> copied(arrow::BitUtil::BytesForBits(length) + 1, 0);
    CopyDenseBitmap(copied.data(), testData, offset, length);

    std::vector<ui8> origSparse(length), copiedSparse(length);
    DecompressToSparseBitmap(origSparse.data(), testData, offset, length);
    DecompressToSparseBitmap(copiedSparse.data(), copied.data(), 0, length);
    for (size_t i = 0; i < length; i++) {
        UNIT_ASSERT_EQUAL_C(origSparse[i], copiedSparse[i], "Expected the same data");
    }
}
} // namespace

Y_UNIT_TEST_SUITE(CopyDenseBitmapTest) {
constexpr size_t testSize = 32;

Y_UNIT_TEST(Test) {
    SetRandomSeed(0);

    std::vector<ui8> testData(testSize);
    for (size_t i = 0; i < testSize; i++) {
        testData[i] = RandomNumber<ui8>();
    }

    for (size_t offset = 0; offset < testSize * 8; offset++) {
        for (size_t length = 0; length <= testSize * 8 - offset; length++) {
            PerformTest(testData.data(), offset, length);
        }
    }
}
} // Y_UNIT_TEST_SUITE(CopyDenseBitmapTest)

Y_UNIT_TEST_SUITE(BitExpanding) {

Y_UNIT_TEST(ReplicateEachBitTwice) {
    // Test case 1: All zeros
    UNIT_ASSERT_EQUAL(ReplicateEachBitTwice(0x00), 0x0000);

    // Test case 2: All ones
    UNIT_ASSERT_EQUAL(ReplicateEachBitTwice(0xFF), 0xFFFF);

    // Test case 3: Alternating bits
    UNIT_ASSERT_EQUAL(ReplicateEachBitTwice(0x55), 0x3333);
    UNIT_ASSERT_EQUAL(ReplicateEachBitTwice(0xAA), 0xCCCC);

    // Test case 4: Random pattern
    UNIT_ASSERT_EQUAL(ReplicateEachBitTwice(0x3C), 0x0FF0);

    // Test case 5: Single bit set
    UNIT_ASSERT_EQUAL(ReplicateEachBitTwice(0x01), 0x0003);
    UNIT_ASSERT_EQUAL(ReplicateEachBitTwice(0x80), 0xC000);
}

Y_UNIT_TEST(ReplicateEachBitFourTimes) {
    // Test case 1: All zeros
    UNIT_ASSERT_EQUAL(ReplicateEachBitFourTimes(0x00), 0x00000000);

    // Test case 2: All ones
    UNIT_ASSERT_EQUAL(ReplicateEachBitFourTimes(0xFF), 0xFFFFFFFF);

    // Test case 3: Alternating bits
    UNIT_ASSERT_EQUAL(ReplicateEachBitFourTimes(0x55), 0x0F0F0F0F);
    UNIT_ASSERT_EQUAL(ReplicateEachBitFourTimes(0xAA), 0xF0F0F0F0);

    // Test case 4: Random pattern
    UNIT_ASSERT_EQUAL(ReplicateEachBitFourTimes(0x3C), 0x00FFFF00);

    // Test case 5: Single bit set
    UNIT_ASSERT_EQUAL(ReplicateEachBitFourTimes(0x01), 0x0000000F);
    UNIT_ASSERT_EQUAL(ReplicateEachBitFourTimes(0x80), 0xF0000000);
}

Y_UNIT_TEST(ReplicateEachBitEightTimes) {
    // Test case 1: All zeros
    UNIT_ASSERT_EQUAL(ReplicateEachBitEightTimes(0x00), 0x00000000);

    // Test case 2: All ones
    UNIT_ASSERT_EQUAL(ReplicateEachBitEightTimes(0xFF), 0xFFFFFFFFFFFFFFFF);

    // Test case 3: Alternating bits
    UNIT_ASSERT_EQUAL(ReplicateEachBitEightTimes(0x55), 0x00FF00FF00FF00FF);
    UNIT_ASSERT_EQUAL(ReplicateEachBitEightTimes(0xAA), 0xFF00FF00FF00FF00);

    // Test case 4: Random pattern
    UNIT_ASSERT_EQUAL(ReplicateEachBitEightTimes(0x3C), 0x0000FFFFFFFF0000);

    // Test case 5: Single bit set
    UNIT_ASSERT_EQUAL(ReplicateEachBitEightTimes(0x01), 0x00000000000000FF);
    UNIT_ASSERT_EQUAL(ReplicateEachBitEightTimes(0x80), 0xFF00000000000000);
}

Y_UNIT_TEST(BitToByteExpand) {
    auto testBody = [](auto n) {
        using T = decltype(n);
        auto max = std::numeric_limits<T>::max();
        auto min = std::numeric_limits<T>::min();
        // Test case 1: All zeros
        auto result1 = BitToByteExpand<T>(min);
        for (size_t i = 0; i < 8; ++i) {
            UNIT_ASSERT_EQUAL(result1[i], min);
        }

        // Test case 2: All ones
        auto result2 = BitToByteExpand<T>(max);
        for (size_t i = 0; i < 8; ++i) {
            UNIT_ASSERT_EQUAL(result2[i], max);
        }

        // Test case 3: Alternating bits
        auto result3 = BitToByteExpand<T>(0x55);
        for (size_t i = 0; i < 8; ++i) {
            UNIT_ASSERT_EQUAL(result3[i], (i % 2 == 0) ? max : min);
        }

        // Test case 4: Random pattern
        auto result4 = BitToByteExpand<T>(0x3C);
        UNIT_ASSERT_EQUAL(result4[0], min);
        UNIT_ASSERT_EQUAL(result4[1], min);
        UNIT_ASSERT_EQUAL(result4[2], max);
        UNIT_ASSERT_EQUAL(result4[3], max);
        UNIT_ASSERT_EQUAL(result4[4], max);
        UNIT_ASSERT_EQUAL(result4[5], max);
        UNIT_ASSERT_EQUAL(result4[6], min);
        UNIT_ASSERT_EQUAL(result4[7], min);

        // Test case 5: Single bit set
        auto result5 = BitToByteExpand<T>(0x80);
        UNIT_ASSERT_EQUAL(result5[7], max);
        for (size_t i = 0; i < 7; ++i) {
            UNIT_ASSERT_EQUAL(result5[i], min);
        }
    };

    testBody(ui8());
    testBody(ui16());
    testBody(ui32());
    testBody(ui64());
}

} // Y_UNIT_TEST_SUITE(BitExpanding)
