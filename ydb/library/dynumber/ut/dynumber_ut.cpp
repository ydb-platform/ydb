#include <ydb/library/dynumber/dynumber.h>
#include <ydb/library/dynumber/cast.h>

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
}

Y_UNIT_TEST_SUITE(TDyNumberTests) {
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

        TestCast<i8> ("200", Nothing());
        TestCast<i16>("40000", Nothing());
        TestCast<i32>("3000000000", Nothing());

        TestCast<ui8> ("300", Nothing());
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
        TestCast<float>("-0.1", -0.1f);
        TestCast<float>("-0.23", -0.23f);
        TestCast<float>("1.23", 1.23f);
        TestCast<float>("-12.3", -12.3f);
        TestCast<float>("-123.4", -123.4f);
        TestCast<float>("-1.23E10", -1.23E10f);
        TestCast<float>("-1.23E-10", -1.23E-10f);

        // unsigned Max
        TestCast<ui8> (ToString(Max<ui8>()),  Max<ui8>());
        TestCast<ui16>(ToString(Max<ui16>()), Max<ui16>());
        TestCast<ui32>(ToString(Max<ui32>()), Max<ui32>());
        TestCast<ui64>(ToString(Max<ui64>()), Max<ui64>());

        // signed Max
        TestCast<i8> (ToString(Max<i8>()),  Max<i8>());
        TestCast<i16>(ToString(Max<i16>()), Max<i16>());
        TestCast<i32>(ToString(Max<i32>()), Max<i32>());
        TestCast<i64>(ToString(Max<i64>()), Max<i64>());

        // signed Min
        TestCast<i8> (ToString(Min<i8>()),  Min<i8>());
        TestCast<i16>(ToString(Min<i16>()), Min<i16>());
        TestCast<i32>(ToString(Min<i32>()), Min<i32>());
        TestCast<i64>(ToString(Min<i64>()), Min<i64>());

        // unsigned out of range
        TestCast<ui8> (ToString(static_cast<ui64>(Max<ui8>())  + 1), Nothing());
        TestCast<ui16>(ToString(static_cast<ui64>(Max<ui16>()) + 1), Nothing());
        TestCast<ui32>(ToString(static_cast<ui64>(Max<ui32>()) + 1), Nothing());

        // signed out of range (right)
        TestCast<i8> (ToString(static_cast<i64>(Max<i8>())  + 1), Nothing());
        TestCast<i16>(ToString(static_cast<i64>(Max<i16>()) + 1), Nothing());
        TestCast<i32>(ToString(static_cast<i64>(Max<i32>()) + 1), Nothing());

        // signed out of range (left)
        TestCast<i8> (ToString(static_cast<i64>(Min<i8>())  - 1), Nothing());
        TestCast<i16>(ToString(static_cast<i64>(Min<i16>()) - 1), Nothing());
        TestCast<i32>(ToString(static_cast<i64>(Min<i32>()) - 1), Nothing());

        // positive signed to unsigned
        TestCast<ui8> (ToString(Max<i8>()),  Max<i8>());
        TestCast<ui16>(ToString(Max<i16>()), Max<i16>());
        TestCast<ui32>(ToString(Max<i32>()), Max<i32>());
        TestCast<ui64>(ToString(Max<i64>()), Max<i64>());

        // negative signed to unsigned
        TestCast<ui8> (ToString(Min<i8>()),  Nothing());
        TestCast<ui16>(ToString(Min<i16>()), Nothing());
        TestCast<ui32>(ToString(Min<i32>()), Nothing());
        TestCast<ui64>(ToString(Min<i64>()), Nothing());

        // DyNumber limits
        TestCast<ui64>("9.9999999999999999999999999999999999999E+125", Nothing());
        TestCast<ui64>("-9.9999999999999999999999999999999999999E+125", Nothing());
        TestCast<double>("1E-130", 1E-130);
        TestCast<double>("-1E-130", -1E-130);
    }
}
