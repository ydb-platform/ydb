#include <yql/essentials/types/dynumber/dynumber.h>
#include <yql/essentials/types/dynumber/cast.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/format.h>
#include <util/stream/str.h>

using namespace NKikimr::NDyNumber;

namespace {
void TestDyNumber(TStringBuf test) {
    UNIT_ASSERT(IsValidDyNumberString(test));

    const auto dyNumber = ParseDyNumberString(test);
    UNIT_ASSERT(dyNumber.Defined());
    UNIT_ASSERT(IsValidDyNumber(*dyNumber));

    const auto restoredTest = DyNumberToString(*dyNumber);
    UNIT_ASSERT(restoredTest.Defined());
    UNIT_ASSERT(IsValidDyNumberString(*restoredTest));

    const auto dyNumberAfterString = ParseDyNumberString(*restoredTest);
    UNIT_ASSERT(dyNumberAfterString.Defined());
    UNIT_ASSERT(IsValidDyNumber(*dyNumberAfterString));

    UNIT_ASSERT_EQUAL(*dyNumber, *dyNumberAfterString);
}

template <typename T>
void TestCast(TStringBuf test, TMaybe<T> value) {
    UNIT_ASSERT_C(IsValidDyNumberString(test), test);

    const auto dyNumber = ParseDyNumberString(test);
    UNIT_ASSERT(dyNumber.Defined());
    UNIT_ASSERT(IsValidDyNumber(*dyNumber));

    const auto casted = TryFromDyNumber<T>(*dyNumber);

    if constexpr (std::is_integral<T>::value) {
        UNIT_ASSERT_VALUES_EQUAL(casted, value);
    } else if (casted && value) {
        UNIT_ASSERT_DOUBLES_EQUAL(*casted, *value, 1e-9);
    } else {
        UNIT_ASSERT_C(!casted && !value, "Casted: " << casted << ", value: " << value);
    }
}

void TestInstant(TStringBuf text, ui32 unitsInSecond, ui64 expectedMicros) {
    const auto binary = ParseDyNumberString(text);
    UNIT_ASSERT_C(binary, text);
    const auto instant = DyNumberToInstant(*binary, unitsInSecond);
    UNIT_ASSERT_C(instant, text);
    UNIT_ASSERT_VALUES_EQUAL_C(instant->MicroSeconds(), expectedMicros, text);
}
} // namespace

Y_UNIT_TEST_SUITE(TDyNumberTests) {
Y_UNIT_TEST(ToInstantUnitsAndRounding) {
    for (const ui32 units : {1U, 1000U, 1000000U, 1000000000U}) {
        const TString second = ToString(units);
        TestInstant(second, units, 1000000);
        TestInstant(second + "e-6", units, 1);
        TestInstant(second + "e-7", units, 1);
        TestInstant(second + ".0000000000001", units, 1000001);
        TestInstant("1e-130", units, 1);
        TestInstant("0", units, 0);
        TestInstant("-1e-130", units, 0);
        TestInstant("-1e125", units, 0);
        TestInstant("9.9999999999999999999999999999999999999e125", units, Max<ui64>());
    }
    TestInstant("123456789.123456", 1, 123456789123456);
    TestInstant("123456789.12345600000000000000000000001", 1, 123456789123457);
    TestInstant("1.01", 1000000, 2);
    TestInstant("1.001", 1000000, 2);
    TestInstant("1000", 1000000000, 1);
    TestInstant("1001", 1000000000, 2);
}

Y_UNIT_TEST(ToInstantBounds) {
    for (const auto& [units, suffix] : {std::pair<ui32, TString>{1, "e-6"}, {1000, "e-3"}, {1000000, ""}, {1000000000, "e3"}}) {
        TestInstant("18446744073709551614" + suffix, units, Max<ui64>() - 1);
        TestInstant("18446744073709551615" + suffix, units, Max<ui64>());
        TestInstant("18446744073709551616" + suffix, units, Max<ui64>());
        TestInstant("18446744073709551614.1" + suffix, units, Max<ui64>());
        TestInstant("18446744073709551615.1" + suffix, units, Max<ui64>());
    }
}

Y_UNIT_TEST(ToInstantInvalidInput) {
    for (const TStringBuf binary : {TStringBuf(), TStringBuf("\x02", 1), TStringBuf("\x03", 1),
             TStringBuf("\x01\x00", 2), TStringBuf("\x02\x81\xfa", 3), TStringBuf("\x00\x81\x00", 3)}) {
        UNIT_ASSERT(!DyNumberToInstant(binary, 1));
    }
    const auto binary = ParseDyNumberString("1");
    UNIT_ASSERT(binary);
    for (const ui32 units : {0U, 2U, 999U, Max<ui32>()}) {
        UNIT_ASSERT(!DyNumberToInstant(*binary, units));
    }
}

Y_UNIT_TEST(ParseAndRestore) {
    TestDyNumber("0");
    TestDyNumber(".0");
    TestDyNumber("1");
    TestDyNumber("18");
    TestDyNumber("181");
    TestDyNumber("1817");
    TestDyNumber("-1");
    TestDyNumber("-18");
    TestDyNumber("-181");
    TestDyNumber("-1817");
    TestDyNumber(".023");
    TestDyNumber("0.93");
    TestDyNumber("724.1");
    TestDyNumber("150e2");
    TestDyNumber("15e3");
    TestDyNumber("0.150e4");
    TestDyNumber("0.15e4");
    TestDyNumber("1E-130");
    TestDyNumber("9.9999999999999999999999999999999999999E+125");
    TestDyNumber("9.9999999999999999999999999999999999999000E+125");
    TestDyNumber("-1E-130");
    TestDyNumber("-9.9999999999999999999999999999999999999E+125");
    TestDyNumber("-9.9999999999999999999999999999999999999000E+125");
}

Y_UNIT_TEST(Cast) {
    TestCast<int>("0", 0);

    TestCast<int>("1", 1);
    TestCast<int>("-1", -1);

    TestCast<int>("12", 12);
    TestCast<int>("-12", -12);
    TestCast<int>("123", 123);
    TestCast<int>("-123", -123);
    TestCast<int>("1234", 1234);
    TestCast<int>("-1234", -1234);

    TestCast<int>(ToString(Max<int>()), Max<int>());
    TestCast<int>(ToString(Min<int>()), Min<int>());

    TestCast<i8>("200", Nothing());
    TestCast<i16>("40000", Nothing());
    TestCast<i32>("3000000000", Nothing());

    TestCast<ui8>("300", Nothing());
    TestCast<ui16>("70000", Nothing());
    TestCast<ui32>("5000000000", Nothing());

    // int to floating point
    TestCast<double>("1", 1);
    TestCast<double>("12", 12);
    TestCast<double>("123", 123);

    // floating point to int
    TestCast<int>("0.1", Nothing());
    TestCast<int>("0.23", Nothing());
    TestCast<int>("1.2", Nothing());
    TestCast<int>("1.23", Nothing());
    TestCast<int>("12.3", Nothing());
    TestCast<int>("123.4", Nothing());

    // double
    TestCast<double>("0.1", 0.1);
    TestCast<double>("0.23", 0.23);
    TestCast<double>("-1.23", -1.23);
    TestCast<double>("12.3", 12.3);
    TestCast<double>("123.4", 123.4);
    TestCast<double>("1.23E20", 1.23E20);
    TestCast<double>("1.23E-20", 1.23E-20);

    // float
    TestCast<float>("-0.1", -0.1F);
    TestCast<float>("-0.23", -0.23F);
    TestCast<float>("1.23", 1.23F);
    TestCast<float>("-12.3", -12.3F);
    TestCast<float>("-123.4", -123.4F);
    TestCast<float>("-1.23E10", -1.23E10F);
    TestCast<float>("-1.23E-10", -1.23E-10F);

    // unsigned Max
    TestCast<ui8>(ToString(Max<ui8>()), Max<ui8>());
    TestCast<ui16>(ToString(Max<ui16>()), Max<ui16>());
    TestCast<ui32>(ToString(Max<ui32>()), Max<ui32>());
    TestCast<ui64>(ToString(Max<ui64>()), Max<ui64>());

    // signed Max
    TestCast<i8>(ToString(Max<i8>()), Max<i8>());
    TestCast<i16>(ToString(Max<i16>()), Max<i16>());
    TestCast<i32>(ToString(Max<i32>()), Max<i32>());
    TestCast<i64>(ToString(Max<i64>()), Max<i64>());

    // signed Min
    TestCast<i8>(ToString(Min<i8>()), Min<i8>());
    TestCast<i16>(ToString(Min<i16>()), Min<i16>());
    TestCast<i32>(ToString(Min<i32>()), Min<i32>());
    TestCast<i64>(ToString(Min<i64>()), Min<i64>());

    // unsigned out of range
    TestCast<ui8>(ToString(static_cast<ui64>(Max<ui8>()) + 1), Nothing());
    TestCast<ui16>(ToString(static_cast<ui64>(Max<ui16>()) + 1), Nothing());
    TestCast<ui32>(ToString(static_cast<ui64>(Max<ui32>()) + 1), Nothing());

    // signed out of range (right)
    TestCast<i8>(ToString(static_cast<i64>(Max<i8>()) + 1), Nothing());
    TestCast<i16>(ToString(static_cast<i64>(Max<i16>()) + 1), Nothing());
    TestCast<i32>(ToString(static_cast<i64>(Max<i32>()) + 1), Nothing());

    // signed out of range (left)
    TestCast<i8>(ToString(static_cast<i64>(Min<i8>()) - 1), Nothing());
    TestCast<i16>(ToString(static_cast<i64>(Min<i16>()) - 1), Nothing());
    TestCast<i32>(ToString(static_cast<i64>(Min<i32>()) - 1), Nothing());

    // positive signed to unsigned
    TestCast<ui8>(ToString(Max<i8>()), Max<i8>());
    TestCast<ui16>(ToString(Max<i16>()), Max<i16>());
    TestCast<ui32>(ToString(Max<i32>()), Max<i32>());
    TestCast<ui64>(ToString(Max<i64>()), Max<i64>());

    // negative signed to unsigned
    TestCast<ui8>(ToString(Min<i8>()), Nothing());
    TestCast<ui16>(ToString(Min<i16>()), Nothing());
    TestCast<ui32>(ToString(Min<i32>()), Nothing());
    TestCast<ui64>(ToString(Min<i64>()), Nothing());

    // DyNumber limits
    TestCast<ui64>("9.9999999999999999999999999999999999999E+125", Nothing());
    TestCast<ui64>("-9.9999999999999999999999999999999999999E+125", Nothing());
    TestCast<double>("1E-130", 1E-130);
    TestCast<double>("-1E-130", -1E-130);
}
} // Y_UNIT_TEST_SUITE(TDyNumberTests)
