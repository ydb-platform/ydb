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
}

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
}
