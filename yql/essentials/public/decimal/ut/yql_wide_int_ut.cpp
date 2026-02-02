#include <yql/essentials/public/decimal/yql_wide_int.h>
#include <library/cpp/testing/unittest/registar.h>
#include <compare>

namespace NYql {
Y_UNIT_TEST_SUITE(TYqlWideIntTest) {
template <typename T>
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

template <typename T>
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

template <typename T>
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

template <typename T>
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

Y_UNIT_TEST(MixedTypeArithmetic) {
    using Wide32 = TWide<i16>;
    using Wide64 = TWide<i32>;

    // Test TWide + integral types
    {
        Wide32 w(100);
        UNIT_ASSERT_VALUES_EQUAL(i32(w + i8(5)), 105);
        UNIT_ASSERT_VALUES_EQUAL(i32(w + i16(10)), 110);
        UNIT_ASSERT_VALUES_EQUAL(i32(w + i32(20)), 120);
        UNIT_ASSERT_VALUES_EQUAL(i32(w + i64(30)), 130);
    }

    // Test integral types + TWide
    {
        Wide32 w(100);
        UNIT_ASSERT_VALUES_EQUAL(i32(i8(5) + w), 105);
        UNIT_ASSERT_VALUES_EQUAL(i32(i16(10) + w), 110);
        UNIT_ASSERT_VALUES_EQUAL(i32(i32(20) + w), 120);
        UNIT_ASSERT_VALUES_EQUAL(i32(i64(30) + w), 130);
    }

    // Test TWide - integral types
    {
        Wide32 w(100);
        UNIT_ASSERT_VALUES_EQUAL(i32(w - i8(5)), 95);
        UNIT_ASSERT_VALUES_EQUAL(i32(w - i16(10)), 90);
        UNIT_ASSERT_VALUES_EQUAL(i32(w - i32(20)), 80);
        UNIT_ASSERT_VALUES_EQUAL(i32(w - i64(30)), 70);
    }

    // Test integral types - TWide
    {
        Wide32 w(50);
        UNIT_ASSERT_VALUES_EQUAL(i32(i8(100) - w), 50);
        UNIT_ASSERT_VALUES_EQUAL(i32(i16(100) - w), 50);
        UNIT_ASSERT_VALUES_EQUAL(i32(i32(100) - w), 50);
        UNIT_ASSERT_VALUES_EQUAL(i32(i64(100) - w), 50);
    }

    // Test TWide + floating point (returns floating point)
    {
        Wide32 w(100);
        UNIT_ASSERT_DOUBLES_EQUAL(w + 1.5f, 101.5f, 0.001f);
        UNIT_ASSERT_DOUBLES_EQUAL(w + 2.5, 102.5, 0.001);
    }

    // Test floating point + TWide (returns floating point)
    {
        Wide32 w(100);
        UNIT_ASSERT_DOUBLES_EQUAL(1.5f + w, 101.5f, 0.001f);
        UNIT_ASSERT_DOUBLES_EQUAL(2.5 + w, 102.5, 0.001);
    }

    // Test TWide - floating point (returns floating point)
    {
        Wide32 w(100);
        UNIT_ASSERT_DOUBLES_EQUAL(w - 1.5f, 98.5f, 0.001f);
        UNIT_ASSERT_DOUBLES_EQUAL(w - 2.5, 97.5, 0.001);
    }

    // Test floating point - TWide (returns floating point)
    {
        Wide32 w(50);
        UNIT_ASSERT_DOUBLES_EQUAL(100.5f - w, 50.5f, 0.001f);
        UNIT_ASSERT_DOUBLES_EQUAL(100.5 - w, 50.5, 0.001);
    }

    // Test with 64-bit wide integers
    {
        Wide64 w(1000000);
        UNIT_ASSERT_VALUES_EQUAL(i64(w + i32(500)), 1000500);
        UNIT_ASSERT_VALUES_EQUAL(i64(w - i32(500)), 999500);
        UNIT_ASSERT_DOUBLES_EQUAL(w + 0.5, 1000000.5, 0.001);
    }
}

Y_UNIT_TEST(MixedTypeArithmeticBoundaryValues) {
    using Wide32 = TWide<i16>;
    using Wide64 = TWide<i32>;
    using UWide32 = TWide<ui16>;
    using UWide64 = TWide<ui32>;

    // Test with i8 boundary values
    {
        Wide32 w(0);
        UNIT_ASSERT_VALUES_EQUAL(i32(w + std::numeric_limits<i8>::max()), 127);
        UNIT_ASSERT_VALUES_EQUAL(i32(w + std::numeric_limits<i8>::min()), -128);
        UNIT_ASSERT_VALUES_EQUAL(i32(std::numeric_limits<i8>::max() + w), 127);
        UNIT_ASSERT_VALUES_EQUAL(i32(std::numeric_limits<i8>::min() + w), -128);

        UNIT_ASSERT_VALUES_EQUAL(i32(w - std::numeric_limits<i8>::max()), -127);
        UNIT_ASSERT_VALUES_EQUAL(i32(w - std::numeric_limits<i8>::min()), 128);
        UNIT_ASSERT_VALUES_EQUAL(i32(std::numeric_limits<i8>::max() - w), 127);
        UNIT_ASSERT_VALUES_EQUAL(i32(std::numeric_limits<i8>::min() - w), -128);
    }

    // Test with ui8 boundary values
    {
        UWide32 w(0);
        UNIT_ASSERT_VALUES_EQUAL(ui32(w + std::numeric_limits<ui8>::max()), 255u);
        UNIT_ASSERT_VALUES_EQUAL(ui32(w + std::numeric_limits<ui8>::min()), 0u);
        UNIT_ASSERT_VALUES_EQUAL(ui32(std::numeric_limits<ui8>::max() + w), 255u);
        UNIT_ASSERT_VALUES_EQUAL(ui32(std::numeric_limits<ui8>::min() + w), 0u);
    }

    {
        Wide32 w(std::numeric_limits<i16>::max());
        UNIT_ASSERT_VALUES_EQUAL(i64(w + std::numeric_limits<i8>::max()), static_cast<i64>(std::numeric_limits<i16>::max()) + std::numeric_limits<i8>::max());
    }

    // Test with i32 boundary values
    {
        Wide64 w(0);
        UNIT_ASSERT_VALUES_EQUAL(i64(w + std::numeric_limits<i32>::max()), 2147483647LL);
        UNIT_ASSERT_VALUES_EQUAL(i64(w + std::numeric_limits<i32>::min()), -2147483648LL);
        UNIT_ASSERT_VALUES_EQUAL(i64(std::numeric_limits<i32>::max() + w), 2147483647LL);
        UNIT_ASSERT_VALUES_EQUAL(i64(std::numeric_limits<i32>::min() + w), -2147483648LL);
    }

    // Test with i64 boundary values
    {
        Wide64 w(0);
        UNIT_ASSERT_VALUES_EQUAL(i64(w + std::numeric_limits<i64>::max()), std::numeric_limits<i64>::max());
        UNIT_ASSERT_VALUES_EQUAL(i64(w + std::numeric_limits<i64>::min()), std::numeric_limits<i64>::min());
        UNIT_ASSERT_VALUES_EQUAL(i64(std::numeric_limits<i64>::max() + w), std::numeric_limits<i64>::max());
        UNIT_ASSERT_VALUES_EQUAL(i64(std::numeric_limits<i64>::min() + w), std::numeric_limits<i64>::min());
    }

    // Test overflow behavior with boundary values
    {
        Wide32 w(std::numeric_limits<i32>::max());
        UNIT_ASSERT_VALUES_EQUAL(i32(w + i8(1)), std::numeric_limits<i32>::min()); // overflow wraps

        Wide32 w2(std::numeric_limits<i32>::min());
        UNIT_ASSERT_VALUES_EQUAL(i32(w2 - i8(1)), std::numeric_limits<i32>::max()); // underflow wraps
    }

    // Test with floating point boundary values
    {
        Wide64 w(1000000);
        UNIT_ASSERT_DOUBLES_EQUAL(w + std::numeric_limits<float>::max(), std::numeric_limits<float>::max(), 1e30f);
        UNIT_ASSERT_DOUBLES_EQUAL(w + std::numeric_limits<float>::min(), 1000000.0f, 1.0f);
        UNIT_ASSERT_DOUBLES_EQUAL(std::numeric_limits<float>::max() + w, std::numeric_limits<float>::max(), 1e30f);

        UNIT_ASSERT_DOUBLES_EQUAL(w + std::numeric_limits<double>::max(), std::numeric_limits<double>::max(), 1e300);
        UNIT_ASSERT_DOUBLES_EQUAL(w + std::numeric_limits<double>::min(), 1000000.0, 1.0);
    }

    // Test with negative floating point values
    {
        Wide32 w(100);
        UNIT_ASSERT_DOUBLES_EQUAL(w + (-50.5f), 49.5f, 0.001f);
        UNIT_ASSERT_DOUBLES_EQUAL((-50.5f) + w, 49.5f, 0.001f);
        UNIT_ASSERT_DOUBLES_EQUAL(w - (-50.5f), 150.5f, 0.001f);
        UNIT_ASSERT_DOUBLES_EQUAL((-50.5f) - w, -150.5f, 0.001f);
    }

    // Test with large TWide values and floating point
    {
        Wide64 w(std::numeric_limits<i32>::max());
        double expected = static_cast<double>(std::numeric_limits<i32>::max()) + 1.5;
        UNIT_ASSERT_DOUBLES_EQUAL(w + 1.5, expected, 0.001);
        UNIT_ASSERT_DOUBLES_EQUAL(1.5 + w, expected, 0.001);
    }

    // Test unsigned wide with boundary values
    {
        UWide64 w(std::numeric_limits<ui32>::max());
        UNIT_ASSERT_VALUES_EQUAL(ui64(w + ui32(1)), static_cast<ui64>(std::numeric_limits<ui32>::max()) + 1);
        UNIT_ASSERT_VALUES_EQUAL(ui64(ui32(1) + w), static_cast<ui64>(std::numeric_limits<ui32>::max()) + 1);
    }
}

Y_UNIT_TEST(MixedTypeSpaceshipOperator) {
    using Wide32 = TWide<i16>;
    using Wide64 = TWide<i32>;
    using UWide32 = TWide<ui16>;
    using UWide64 = TWide<ui32>;

    // Test TWide <=> TWide
    {
        Wide32 w1(100);
        Wide32 w2(200);
        Wide32 w3(100);

        UNIT_ASSERT((w1 <=> w2) < 0);
        UNIT_ASSERT((w2 <=> w1) > 0);
        UNIT_ASSERT((w1 <=> w3) == 0);
    }

    // Test TWide <=> integral types
    {
        Wide32 w(100);
        UNIT_ASSERT((w <=> i8(50)) > 0);
        UNIT_ASSERT((w <=> i8(100)) == 0);
        UNIT_ASSERT((w <=> i8(127)) < 0);

        UNIT_ASSERT((w <=> i16(50)) > 0);
        UNIT_ASSERT((w <=> i16(100)) == 0);
        UNIT_ASSERT((w <=> i16(200)) < 0);

        UNIT_ASSERT((w <=> i32(50)) > 0);
        UNIT_ASSERT((w <=> i32(100)) == 0);
        UNIT_ASSERT((w <=> i32(200)) < 0);

        UNIT_ASSERT((w <=> i64(50)) > 0);
        UNIT_ASSERT((w <=> i64(100)) == 0);
        UNIT_ASSERT((w <=> i64(200)) < 0);
    }

    // Test integral types <=> TWide
    {
        Wide32 w(100);
        UNIT_ASSERT((i8(50) <=> w) < 0);
        UNIT_ASSERT((i8(100) <=> w) == 0);
        UNIT_ASSERT((i8(127) <=> w) > 0);

        UNIT_ASSERT((i16(50) <=> w) < 0);
        UNIT_ASSERT((i16(100) <=> w) == 0);
        UNIT_ASSERT((i16(200) <=> w) > 0);

        UNIT_ASSERT((i32(50) <=> w) < 0);
        UNIT_ASSERT((i32(100) <=> w) == 0);
        UNIT_ASSERT((i32(200) <=> w) > 0);

        UNIT_ASSERT((i64(50) <=> w) < 0);
        UNIT_ASSERT((i64(100) <=> w) == 0);
        UNIT_ASSERT((i64(200) <=> w) > 0);
    }

    // Test TWide <=> floating point
    {
        Wide32 w(100);
        UNIT_ASSERT((w <=> 50.0f) > 0);
        UNIT_ASSERT((w <=> 100.0f) == 0);
        UNIT_ASSERT((w <=> 200.0f) < 0);

        UNIT_ASSERT((w <=> 50.0) > 0);
        UNIT_ASSERT((w <=> 100.0) == 0);
        UNIT_ASSERT((w <=> 200.0) < 0);
    }

    // Test floating point <=> TWide
    {
        Wide32 w(100);
        UNIT_ASSERT((50.0f <=> w) < 0);
        UNIT_ASSERT((100.0f <=> w) == 0);
        UNIT_ASSERT((200.0f <=> w) > 0);

        UNIT_ASSERT((50.0 <=> w) < 0);
        UNIT_ASSERT((100.0 <=> w) == 0);
        UNIT_ASSERT((200.0 <=> w) > 0);
    }

    // Test with negative values
    {
        Wide32 w(-100);
        UNIT_ASSERT((w <=> i32(-50)) < 0);
        UNIT_ASSERT((w <=> i32(-100)) == 0);
        UNIT_ASSERT((w <=> i32(-200)) > 0);
        UNIT_ASSERT((w <=> i32(0)) < 0);

        UNIT_ASSERT((i32(-50) <=> w) > 0);
        UNIT_ASSERT((i32(-100) <=> w) == 0);
        UNIT_ASSERT((i32(-200) <=> w) < 0);
        UNIT_ASSERT((i32(0) <=> w) > 0);
    }

    // Test with 64-bit wide integers
    {
        Wide64 w(1000000);
        UNIT_ASSERT((w <=> i32(500000)) > 0);
        UNIT_ASSERT((w <=> i32(1000000)) == 0);
        UNIT_ASSERT((w <=> i64(2000000)) < 0);
    }

    // Test unsigned wide integers
    {
        UWide32 w(100);
        UNIT_ASSERT((w <=> ui8(50)) > 0);
        UNIT_ASSERT((w <=> ui8(100)) == 0);
        UNIT_ASSERT((w <=> ui16(200)) < 0);
    }

    // Test boundary values
    {
        Wide32 w(std::numeric_limits<i32>::max());
        UNIT_ASSERT((w <=> i32(0)) > 0);
        UNIT_ASSERT((w <=> std::numeric_limits<i32>::max()) == 0);

        Wide32 w2(std::numeric_limits<i32>::min());
        UNIT_ASSERT((w2 <=> i32(0)) < 0);
        UNIT_ASSERT((w2 <=> std::numeric_limits<i32>::min()) == 0);
    }

    // Test unsigned boundary values
    {
        UWide64 w(std::numeric_limits<ui64>::max());
        UNIT_ASSERT((w <=> ui64(0)) > 0);
        UNIT_ASSERT((w <=> std::numeric_limits<ui64>::max()) == 0);
    }
}

} // Y_UNIT_TEST_SUITE(TYqlWideIntTest)

} // namespace NYql
