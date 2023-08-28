#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/arrow/mkql_bit_utils.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

ui8 NaiveCompressByte(ui8 value, ui8 mask) {
    ui8 result = 0;
    ui8 outPos = 0;
    for (ui8 i = 0; i < 8; ++i) {
        if (mask & (1 << i)) {
            ui8 bit = (value & (1 << i)) != 0;
            result |= (bit << outPos);
            ++outPos;
        }
    }
    return result;
}

} // namespace


Y_UNIT_TEST_SUITE(TMiniKQLBitUtilsTest) {
Y_UNIT_TEST(TestCompressByte) {
    for (size_t value = 0; value < 256; ++value) {
        for (size_t mask = 0; mask < 256; ++mask) {
            UNIT_ASSERT_EQUAL(NaiveCompressByte(value, mask), CompressByte(value, mask));
        }
    }
}

Y_UNIT_TEST(TestLoad) {
    const ui8 src[] = {0b01110100, 0b11011101, 0b01101011};
    UNIT_ASSERT_EQUAL(LoadByteUnaligned(src, 10), 0b11110111);
    UNIT_ASSERT_EQUAL(LoadByteUnaligned(src, 16), 0b01101011);
}

Y_UNIT_TEST(CompressAligned) {
    const ui8 data[] = {0b01110100, 0b11011101, 0b01101011};
    const ui8 mask[] = {0b11101100, 0b10111010, 0b10001111};
    ui8 result[100];
    auto res = CompressBitmap(data, 0, mask, 0, result, 0, 24);
    UNIT_ASSERT_EQUAL(res, 15);
    UNIT_ASSERT_EQUAL(result[0], 0b11001101);
    UNIT_ASSERT_EQUAL(result[1] & 0x7fu, 0b00101110);
}

Y_UNIT_TEST(CompressUnalignedOutput) {
    const ui8 data[] = {0b01110100, 0b11011101, 0b01101011};
    const ui8 mask[] = {0b11101100, 0b10111010, 0b10001111};
    ui8 result[100];
    result[0] = 0b101;
    auto res = CompressBitmap(data, 0, mask, 0, result, 3, 24);
    UNIT_ASSERT_EQUAL(res, 18);
    UNIT_ASSERT_EQUAL(result[0], 0b01101101);
    UNIT_ASSERT_EQUAL(result[1], 0b01110110);
    UNIT_ASSERT_EQUAL(result[2] & 0x3, 0b01);
}

}

} // namespace NMiniKQL
} // namespace NKikimr

