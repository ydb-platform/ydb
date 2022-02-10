#include <ydb/library/yql/minikql/pack_num.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ylimits.h>

Y_UNIT_TEST_SUITE(TPackNumTest) {
    static ui32 TEST_DATA_UI32[] = {
        0,
        0x80,
        0x4000,
        0x8000,
        0x200000,
        0x800000,
        0x80000000,
        Max<ui32>()
    };
    static ui64 TEST_DATA_UI64[] = {
        0ull,
        0x80ull,
        0x4000ull,
        0x8000ull,
        0x200000ull,
        0x800000ull,
        0x10000000ull,
        0x80000000ull,
        0x800000000ull,
        0x8000000000ull,
        0x40000000000ull,
        0x800000000000ull,
        0x2000000000000ull,
        0x80000000000000ull,
        0x8000000000000000ull,
        Max<ui64>()};

    static size_t DELTA = 10;

    Y_UNIT_TEST(PackUnpack32) {
        char buf[10];
        for (ui32 num: TEST_DATA_UI32) {
            for (ui32 i = num - Min<ui32>(num, DELTA); i < num + Min<ui32>(DELTA, Max<ui32>() - num); ++i) {
                size_t packedLen = NKikimr::Pack32(i, buf);
                UNIT_ASSERT(packedLen > 0);
                UNIT_ASSERT(packedLen <= 5);
                UNIT_ASSERT_VALUES_EQUAL(packedLen, NKikimr::GetPack32Length(i));
                ui32 unpacked = 0;
                size_t unpackedLen = NKikimr::Unpack32(buf, packedLen, unpacked);
                UNIT_ASSERT_VALUES_EQUAL(i, unpacked);
                UNIT_ASSERT_VALUES_EQUAL(packedLen, unpackedLen);
            }
        }
    }

    Y_UNIT_TEST(PackUnpack64) {
        char buf[10];
        for (ui64 num: TEST_DATA_UI64) {
            for (ui64 i = num - Min<ui64>(num, DELTA); i < num + Min<ui64>(DELTA, Max<ui64>() - num); ++i) {
                size_t packedLen = NKikimr::Pack64(i, buf);
                UNIT_ASSERT(packedLen > 0);
                UNIT_ASSERT(packedLen <= 9);
                UNIT_ASSERT_VALUES_EQUAL(packedLen, NKikimr::GetPack64Length(i));
                ui64 unpacked = 0;
                size_t unpackedLen = NKikimr::Unpack64(buf, packedLen, unpacked);
                UNIT_ASSERT_VALUES_EQUAL(i, unpacked);
                UNIT_ASSERT_VALUES_EQUAL(packedLen, unpackedLen);
            }
        }
    }
}
