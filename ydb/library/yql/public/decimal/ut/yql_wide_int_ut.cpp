#include <ydb/library/yql/public/decimal/yql_wide_int.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYql {
Y_UNIT_TEST_SUITE(TYqlWideIntTest) {
    template<typename T>
    void TestUnary(const T aa) {
        using Test = TWide<typename THalfOf<T>::Type>;
        const Test at(aa);
        static_assert(sizeof(at) == sizeof(aa), "Bad wide int size!");

        UNIT_ASSERT_VALUES_EQUAL(static_cast<i8>(aa), static_cast<i8>(at));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(aa), static_cast<ui8>(at));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i16>(aa), static_cast<i16>(at));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui16>(aa), static_cast<ui16>(at));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i32>(aa), static_cast<i32>(at));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(aa), static_cast<ui32>(at));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(aa), static_cast<i64>(at));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(aa), static_cast<ui64>(at));
#ifndef _win_
        UNIT_ASSERT(static_cast<i128_t>(aa) == static_cast<i128_t>(at));
        UNIT_ASSERT(static_cast<ui128_t>(aa) == static_cast<ui128_t>(at));
#endif

        {
            const auto exp = ~aa;
            const auto tst = ~at;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        {
            const auto exp = +aa;
            const auto tst = +at;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        {
            const auto exp = -aa;
            const auto tst = -at;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        {
            auto exp = aa;
            auto tst = at;
            ++exp;
            ++tst;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        {
            auto exp = aa;
            auto tst = at;
            --exp;
            --tst;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        {
            auto exp = aa;
            auto tst = at;
            exp++;
            tst++;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        {
            auto exp = aa;
            auto tst = at;
            exp--;
            tst--;
            UNIT_ASSERT(T(tst) == T(exp));
        }
    }

    template<typename T>
    void TestBinary(const T ll, const T rr) {
        using Test = TWide<typename THalfOf<T>::Type>;
        const Test lt(ll), rt(rr);

        {
            const auto exp = ll & rr;
            const auto tst = lt & rt;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        {
            const auto exp = ll | rr;
            const auto tst = lt | rt;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        {
            const auto exp = ll ^ rr;
            const auto tst = lt ^ rt;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        {
            const auto exp = ll + rr;
            const auto tst = lt + rt;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        {
            const auto exp = ll - rr;
            const auto tst = lt - rt;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        if (rr > 0 && rr < T(sizeof(T) << 3U))
        {
            const auto exp = ll >> rr;
            const auto tst = lt >> rt;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        if (rr > 0 && rr < T(sizeof(T) << 3U))
        {
            const auto exp = ll << rr;
            const auto tst = lt << rt;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        {
            const auto exp = ll * rr;
            const auto tst = lt * rt;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        if (rr)
        {
            const auto exp = ll / rr;
            const auto tst = lt / rt;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        if (rr)
        {
            const auto exp = ll % rr;
            const auto tst = lt % rt;
            UNIT_ASSERT(T(tst) == T(exp));
        }

        {
            const auto exp = ll == rr;
            const auto tst = lt == rt;
            UNIT_ASSERT_VALUES_EQUAL(tst, exp);
        }

        {
            const auto exp = ll != rr;
            const auto tst = lt != rt;
            UNIT_ASSERT_VALUES_EQUAL(tst, exp);
        }

        {
            const auto exp = ll > rr;
            const auto tst = lt > rt;
            UNIT_ASSERT_VALUES_EQUAL(tst, exp);
        }

        {
            const auto exp = ll < rr;
            const auto tst = lt < rt;
            UNIT_ASSERT_VALUES_EQUAL(tst, exp);
        }

        {
            const auto exp = ll >= rr;
            const auto tst = lt >= rt;
            UNIT_ASSERT_VALUES_EQUAL(tst, exp);
        }

        {
            const auto exp = ll <= rr;
            const auto tst = lt <= rt;
            UNIT_ASSERT_VALUES_EQUAL(tst, exp);
        }
    }

    template<typename T>
    void TestsForUnsignedType() {
        static_assert(std::is_unsigned<T>::value, "Tests for unsigned type.");
        TestUnary<T>(2U);
        TestUnary<T>(4U);
        TestUnary<T>(17U);
        TestUnary<T>(42U);
        TestUnary<T>(127U);
        TestUnary<T>(128U);
        TestUnary<T>(129U);
        TestUnary<T>(200U);
        TestUnary<T>(255U);
        TestUnary<T>(256U);
        TestUnary<T>(257U);

        TestUnary<T>(std::numeric_limits<T>::min());
        TestUnary<T>(std::numeric_limits<T>::max());
        TestUnary<T>(std::numeric_limits<T>::max() - 1U);
        TestUnary<T>(std::numeric_limits<T>::max() >> 1U);
        TestUnary<T>(std::numeric_limits<T>::max() >> 3U);
        TestUnary<T>(std::numeric_limits<T>::max() >> 7U);


        TestUnary<T>(std::numeric_limits<T>::min() + 1U);
        TestUnary<T>(std::numeric_limits<T>::max() - 1U);

        TestUnary<T>(std::numeric_limits<T>::min() + 3U);
        TestUnary<T>(std::numeric_limits<T>::max() - 3U);

        TestUnary<T>(std::numeric_limits<T>::min() + 7U);
        TestUnary<T>(std::numeric_limits<T>::max() - 7U);


        TestBinary<T>(1U, 1U);
        TestBinary<T>(7U, 31U);
        TestBinary<T>(30000U, 13U);
        TestBinary<T>(127U, 13U);
        TestBinary<T>(128U, 19U);
        TestBinary<T>(129U, 17U);


        TestBinary<T>(std::numeric_limits<T>::min(), 7U);
        TestBinary<T>(std::numeric_limits<T>::max(), 7U);

        TestBinary<T>(std::numeric_limits<T>::min(), 8U);
        TestBinary<T>(std::numeric_limits<T>::max(), 8U);

        TestBinary<T>(std::numeric_limits<T>::min(), 9U);
        TestBinary<T>(std::numeric_limits<T>::max(), 9U);

        TestBinary<T>(std::numeric_limits<T>::max() - 1U, std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::max() - 1U, std::numeric_limits<T>::min() + 1U);


        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::max());
        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::min(), std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::min());

        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::min() + 1U);
        TestBinary<T>(std::numeric_limits<T>::max() - 1U, std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::max() - 1U, std::numeric_limits<T>::min() + 1U);

        TestBinary<T>(std::numeric_limits<T>::max() - 1U, std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::min() + 1U, std::numeric_limits<T>::min());

        TestBinary<T>(std::numeric_limits<T>::max(), 1U);
        TestBinary<T>(std::numeric_limits<T>::min(), 1U);
        TestBinary<T>(std::numeric_limits<T>::max() - 1U, 1U);
        TestBinary<T>(std::numeric_limits<T>::min() + 1U, 1U);

        TestBinary<T>(std::numeric_limits<T>::max(), 7U);
        TestBinary<T>(std::numeric_limits<T>::min(), 7U);
        TestBinary<T>(std::numeric_limits<T>::max() - 1U, 7U);
        TestBinary<T>(std::numeric_limits<T>::min() + 1U, 7U);

        TestBinary<T>(std::numeric_limits<T>::max() >> 1U, std::numeric_limits<T>::min() >> 1U);
        TestBinary<T>(std::numeric_limits<T>::max() >> 1U, std::numeric_limits<T>::min() + 1U);
        TestBinary<T>(std::numeric_limits<T>::max() - 1U, std::numeric_limits<T>::min() >> 1U);
        TestBinary<T>(std::numeric_limits<T>::max() >> 1U, std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::min() >> 1U);

        TestBinary<T>(std::numeric_limits<T>::max() >> 3U, std::numeric_limits<T>::min() >> 3U);
        TestBinary<T>(std::numeric_limits<T>::max() >> 3U, std::numeric_limits<T>::min() + 3U);
        TestBinary<T>(std::numeric_limits<T>::max() - 3U, std::numeric_limits<T>::min() >> 3U);
        TestBinary<T>(std::numeric_limits<T>::max() >> 3U, std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::min() >> 3U);
    }

    template<typename T>
    void TestsForSignedType() {
        static_assert(std::is_signed<T>::value, "Tests for signed type.");
        TestUnary<T>(0);

        TestUnary<T>(1);
        TestUnary<T>(-1);

        TestUnary<T>(2);
        TestUnary<T>(-2);

        TestUnary<T>(3);
        TestUnary<T>(-3);

        TestUnary<T>(4);
        TestUnary<T>(-4);

        TestUnary<T>(17);
        TestUnary<T>(-17);

        TestUnary<T>(42);
        TestUnary<T>(-42);

        TestUnary<T>(127);
        TestUnary<T>(-127);

        TestUnary<T>(128);
        TestUnary<T>(-128);

        TestUnary<T>(129);
        TestUnary<T>(-129);

        TestUnary<T>(200);
        TestUnary<T>(-200);

        TestUnary<T>(255);
        TestUnary<T>(-255);

        TestUnary<T>(256);
        TestUnary<T>(-256);

        TestUnary<T>(257);
        TestUnary<T>(-257);

        TestUnary<T>(258);
        TestUnary<T>(-258);

        TestUnary<T>(std::numeric_limits<T>::min());
        TestUnary<T>(std::numeric_limits<T>::max());

        TestUnary<T>(std::numeric_limits<T>::min() + 1);
        TestUnary<T>(std::numeric_limits<T>::max() - 1);

        TestUnary<T>(std::numeric_limits<T>::min() + 2);
        TestUnary<T>(std::numeric_limits<T>::max() - 2);

        TestUnary<T>(std::numeric_limits<T>::min() + 3);
        TestUnary<T>(std::numeric_limits<T>::max() - 3);

        TestUnary<T>(std::numeric_limits<T>::min() + 7);
        TestUnary<T>(std::numeric_limits<T>::max() - 7);

        TestUnary<T>(std::numeric_limits<T>::min() >> 1);
        TestUnary<T>(std::numeric_limits<T>::max() >> 1);

        TestUnary<T>(std::numeric_limits<T>::min() >> 3);
        TestUnary<T>(std::numeric_limits<T>::max() >> 3);

        TestUnary<T>(std::numeric_limits<T>::min() >> 7);
        TestUnary<T>(std::numeric_limits<T>::max() >> 7);


        TestUnary<T>(std::numeric_limits<T>::min() + 1);
        TestUnary<T>(std::numeric_limits<T>::max() - 1);

        TestUnary<T>(std::numeric_limits<T>::min() + 3);
        TestUnary<T>(std::numeric_limits<T>::max() - 3);

        TestUnary<T>(std::numeric_limits<T>::min() + 7);
        TestUnary<T>(std::numeric_limits<T>::max() - 7);


        TestBinary<T>(0, 0);
        TestBinary<T>(1, 0);
        TestBinary<T>(0, -1);

        TestBinary<T>(1, 1);
        TestBinary<T>(1, -1);
        TestBinary<T>(-1, 1);
        TestBinary<T>(-1, -1);

        TestBinary<T>(7, 42);
        TestBinary<T>(-7, 42);

        TestBinary<T>(0, -43);

        TestBinary<T>(-30000, 64);
        TestBinary<T>(30000, -64);
        TestBinary<T>(30000, 64);

        TestBinary<T>(21, 0);
        TestBinary<T>(13, -127);
        TestBinary<T>(-19, 128);
        TestBinary<T>(-77, -129);
        TestBinary<T>(13, 127);
        TestBinary<T>(19, 128);
        TestBinary<T>(77, 129);

        TestBinary<T>(std::numeric_limits<T>::max(), -1);

        TestBinary<T>(std::numeric_limits<T>::min(), -7);
        TestBinary<T>(std::numeric_limits<T>::max(), -7);

        TestBinary<T>(std::numeric_limits<T>::min(), -8);
        TestBinary<T>(std::numeric_limits<T>::max(), -8);

        TestBinary<T>(std::numeric_limits<T>::min(), -9);
        TestBinary<T>(std::numeric_limits<T>::max(), -9);

        TestBinary<T>(std::numeric_limits<T>::min(), std::numeric_limits<T>::min() >> 5);
        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::min() >> 5);


        TestBinary<T>(std::numeric_limits<T>::min(), 7);
        TestBinary<T>(std::numeric_limits<T>::max(), 7);

        TestBinary<T>(std::numeric_limits<T>::min(), 8);
        TestBinary<T>(std::numeric_limits<T>::max(), 8);

        TestBinary<T>(std::numeric_limits<T>::min(), 9);
        TestBinary<T>(std::numeric_limits<T>::max(), 9);

        TestBinary<T>(std::numeric_limits<T>::max() - 1, std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::max() - 1, std::numeric_limits<T>::min() + 1);


        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::max());
        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::min(), std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::min());

        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::min() + 1);
        TestBinary<T>(std::numeric_limits<T>::max() - 1, std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::max() - 1, std::numeric_limits<T>::min() + 1);

        TestBinary<T>(std::numeric_limits<T>::max(), 0);
        TestBinary<T>(std::numeric_limits<T>::min(), 0);
        TestBinary<T>(std::numeric_limits<T>::max() - 1, 0);
        TestBinary<T>(std::numeric_limits<T>::min() + 1, 0);

        TestBinary<T>(std::numeric_limits<T>::max(), 1);
        TestBinary<T>(std::numeric_limits<T>::min(), 1);
        TestBinary<T>(std::numeric_limits<T>::max() - 1, 1);
        TestBinary<T>(std::numeric_limits<T>::min() + 1, 1);

        TestBinary<T>(std::numeric_limits<T>::max(), 7);
        TestBinary<T>(std::numeric_limits<T>::min(), 7);
        TestBinary<T>(std::numeric_limits<T>::max() - 1, 7);
        TestBinary<T>(std::numeric_limits<T>::min() + 1, 7);

        TestBinary<T>(std::numeric_limits<T>::max() >> 1, std::numeric_limits<T>::min() >> 1);
        TestBinary<T>(std::numeric_limits<T>::max() >> 1, std::numeric_limits<T>::min() + 1);
        TestBinary<T>(std::numeric_limits<T>::max() - 1, std::numeric_limits<T>::min() >> 1);
        TestBinary<T>(std::numeric_limits<T>::max() >> 1, std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::min() >> 1);

        TestBinary<T>(std::numeric_limits<T>::max() >> 3, std::numeric_limits<T>::min() >> 3);
        TestBinary<T>(std::numeric_limits<T>::max() >> 3, std::numeric_limits<T>::min() + 3);
        TestBinary<T>(std::numeric_limits<T>::max() - 3, std::numeric_limits<T>::min() >> 3);
        TestBinary<T>(std::numeric_limits<T>::max() >> 3, std::numeric_limits<T>::min());
        TestBinary<T>(std::numeric_limits<T>::max(), std::numeric_limits<T>::min() >> 3);
    }

    Y_UNIT_TEST(CheckUnsignedByCompilerIntegrals) {
        TestsForUnsignedType<ui32>();
        TestsForUnsignedType<ui64>();
#ifndef _win_
        TestsForUnsignedType<ui128_t>();
#endif
    }

#ifndef _ubsan_enabled_
#ifndef _msan_enabled_
    Y_UNIT_TEST(CheckSignedByCompilerIntegrals) {
        TestsForSignedType<i32>();
        TestsForSignedType<i64>();
#ifndef _win_
        TestsForSignedType<i128_t>();
#endif
    }
#endif
#endif
}

}
