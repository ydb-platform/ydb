#include "longs.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/digest/old_crc/crc.h>
#include <util/string/util.h>
#include <util/stream/output.h>
#include <util/system/hi_lo.h>

Y_UNIT_TEST_SUITE(TLongsTest) {
    Y_UNIT_TEST(TestLongs) {
        i16 x16 = 40;
        i64 x64 = 40;
        i64 y64;
        TString s;

        s += Sprintf("x16=0x%x\n", (int)x16);
        s += Sprintf("LO_8(x16)=0x%x HI_8(x16)=0x%x\n\n", (int)Lo8(x16), (int)Hi8(x16));

        char buf[100];
        memset(buf, 0, 100);
        char* p = buf;
        int l = out_long(x64, buf);
        s += Sprintf("x64=0x%" PRIi64 "\n", x64);
        s += Sprintf("LO_32(x64)=0x%" PRIu32 " HI_32(x64)=0x%" PRIu32 "\n", (ui32)Lo32(x64), (ui32)Hi32(x64));
        s += Sprintf("buf=%s, l=%d: ", buf, l);
        for (int i = 0; i < l; i++) {
            s += Sprintf("0x%02x ", buf[i]);
        }
        s += Sprintf("\n");

        p = buf;
        in_long(y64, p);
        s += Sprintf("x=0x%" PRIi64 " y=0x%" PRIi64 "\n", x64, y64);
        if (x64 != y64) {
            s += Sprintf("Error: y64 != x64\n");
        } else {
            s += Sprintf("OK\n");
        }

        UNIT_ASSERT_EQUAL(Crc<ui64>(s.data(), s.size()), 7251624297500315779ULL); // WTF?
    }

    template <typename TSignedInt>
    void TestOneValue(TSignedInt value) {
        char buffer[sizeof(TSignedInt) + 1];
        auto bytes = out_long(value, buffer);
        TSignedInt readValue = 0;
        auto readBytes = in_long(readValue, buffer);
        UNIT_ASSERT_EQUAL(bytes, readBytes);
        UNIT_ASSERT_EQUAL(bytes, len_long(value));
        UNIT_ASSERT_EQUAL(value, readValue);
    }

    template <typename TSignedInt>
    void TestCornerCasesImpl(int maxPow) {
        for (int i = 0; i <= maxPow; ++i) {
            TestOneValue<TSignedInt>((TSignedInt)(1ull << i));
            TestOneValue<TSignedInt>((TSignedInt)((1ull << i) - 1));
            TestOneValue<TSignedInt>((TSignedInt)((1ull << i) + 1));
        }
    }

    Y_UNIT_TEST(TestCornerCases) {
        TestCornerCasesImpl<i32>(31);
        TestCornerCasesImpl<i64>(63);
    }
}
