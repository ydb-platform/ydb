#include <library/cpp/int128/int128.h>

#include <library/cpp/testing/unittest/registar.h>

// from https://a.yandex-team.ru/arc/trunk/arcadia/library/ticket_parser/c/src/ut/utils_ut.cpp?rev=4221861

#if defined(Y_HAVE_INT128)
Y_UNIT_TEST_SUITE(Int128ViaIntrinsicSuite) {
    using guint128_t = unsigned __int128;
    guint128_t toGcc(ui128 num) {
        guint128_t res = 0;
        res |= GetLow(num);
        res |= guint128_t(GetHigh(num)) << 64;
        return res;
    }

    Y_UNIT_TEST(bigintTest) {
        UNIT_ASSERT(guint128_t(127) == toGcc(ui128(127)));
        UNIT_ASSERT(guint128_t(127) * guint128_t(127) == toGcc(ui128(127) * ui128(127)));
        UNIT_ASSERT(guint128_t(127) + guint128_t(127) == toGcc(ui128(127) + ui128(127)));
        UNIT_ASSERT(guint128_t(127) << 3 == toGcc(ui128(127) << 3));
        UNIT_ASSERT(guint128_t(127) >> 1 == toGcc(ui128(127) >> 1));

        UNIT_ASSERT(guint128_t(1000000000027UL) * guint128_t(1000000000027UL) == toGcc(ui128(1000000000027UL) * ui128(1000000000027UL)));
        UNIT_ASSERT(guint128_t(1000000000027UL) + guint128_t(1000000000027UL) == toGcc(ui128(1000000000027UL) + ui128(1000000000027UL)));
        UNIT_ASSERT(guint128_t(1000000000027UL) << 3 == toGcc(ui128(1000000000027UL) << 3));
        UNIT_ASSERT(guint128_t(1000000000027UL) >> 1 == toGcc(ui128(1000000000027UL) >> 1));
        UNIT_ASSERT((guint128_t(1000000000027UL) * guint128_t(1000000000027UL)) << 3 == toGcc((ui128(1000000000027UL) * ui128(1000000000027UL)) << 3));
        UNIT_ASSERT((guint128_t(1000000000027UL) + guint128_t(1000000000027UL)) >> 1 == toGcc((ui128(1000000000027UL) + ui128(1000000000027UL)) >> 1));

        UNIT_ASSERT((ui64)(guint128_t(1000000000027UL) * guint128_t(1000000000027UL)) == GetLow(ui128(1000000000027UL) * ui128(1000000000027UL)));
    }
}
#endif
