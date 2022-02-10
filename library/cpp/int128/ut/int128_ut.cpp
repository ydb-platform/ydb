#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/int128/int128.h>

#include <util/generic/cast.h>

#include <type_traits>

Y_UNIT_TEST_SUITE(Uint128Suite) {
    Y_UNIT_TEST(Uint128DefaultCtor) {
        const ui128 value{};
        UNIT_ASSERT_EQUAL(GetLow(value), 0);
        UNIT_ASSERT_EQUAL(GetHigh(value), 0);
    }

    Y_UNIT_TEST(Uint128NumericLimits) {
        UNIT_ASSERT_EQUAL(std::numeric_limits<ui128>::digits, 128);
        UNIT_ASSERT_EQUAL(std::numeric_limits<ui128>::max() + 1, ui128{0});
    }

    Y_UNIT_TEST(Uint128Sizeof) {
        UNIT_ASSERT_EQUAL(sizeof(ui128), sizeof(ui64) * 2);
    }

    Y_UNIT_TEST(Uint128Cast) {
        // see util/generic/cast.h
        const auto underlyingTypeIsSelf = std::is_same<::NPrivate::TUnderlyingTypeOrSelf<ui128>, ui128>::value;
        UNIT_ASSERT_EQUAL(underlyingTypeIsSelf, true);

        const auto convertibleUi128Ui128 = ::NPrivate::TSafelyConvertible<ui128, ui128>::Result;
        const auto convertibleUi64Ui128 = ::NPrivate::TSafelyConvertible<ui64, ui128>::Result;
        const auto convertibleUi128Ui64 = ::NPrivate::TSafelyConvertible<ui128, ui64>::Result;
        UNIT_ASSERT_EQUAL(convertibleUi128Ui128, true); // from ui128 to ui128 => safe
        UNIT_ASSERT_EQUAL(convertibleUi64Ui128, false); // from ui128 to ui64 => not safe
        UNIT_ASSERT_EQUAL(convertibleUi128Ui64, true); // from ui64 to ui128 => safe
    }

    Y_UNIT_TEST(SafeIntegerCastTest) {
        ui128 narrowNumber = 1;

        UNIT_ASSERT_NO_EXCEPTION(SafeIntegerCast<ui64>(narrowNumber));

        ui128 wideNumber{0};
        wideNumber -= 1;
        UNIT_ASSERT_EXCEPTION(SafeIntegerCast<ui64>(wideNumber), yexception);
    }

    Y_UNIT_TEST(SignbitTest) {
        UNIT_ASSERT(!std::signbit(ui128{0}));
        UNIT_ASSERT(!std::signbit(ui128{-1}));
        UNIT_ASSERT(!std::signbit(i128{0}));
        UNIT_ASSERT(std::signbit(i128{-1}));
    }

    Y_UNIT_TEST(ToStringTest) {
        // int128
        UNIT_ASSERT_VALUES_EQUAL(ToString(i128(0)), "0");
        UNIT_ASSERT_VALUES_EQUAL(ToString(i128(42)), "42");
        UNIT_ASSERT_VALUES_EQUAL(ToString(i128(-142)), "-142");
        UNIT_ASSERT_VALUES_EQUAL(ToString(std::numeric_limits<i128>::min()), "-170141183460469231731687303715884105728");
        UNIT_ASSERT_VALUES_EQUAL(ToString(std::numeric_limits<i128>::max()), "170141183460469231731687303715884105727");

        // Just random number
        UNIT_ASSERT_VALUES_EQUAL(
            ToString(
                - ((i128(8741349088318632894ul) << 64) | i128(1258331728153556511ul))
            ),
            "-161249429491168133245752281683002013215");

        // uint128
        UNIT_ASSERT_VALUES_EQUAL(ToString(ui128(0)), "0");
        UNIT_ASSERT_VALUES_EQUAL(ToString(ui128(42)), "42");
        UNIT_ASSERT_VALUES_EQUAL(ToString(std::numeric_limits<ui128>::min()), "0");
        UNIT_ASSERT_VALUES_EQUAL(ToString(std::numeric_limits<ui128>::max()), "340282366920938463463374607431768211455");

        // Just random number
        UNIT_ASSERT_VALUES_EQUAL(
            ToString(
                ((ui128(12745260439834612983ul) << 64) | ui128(10970669179777569799ul))
            ),
            "235108557486403940296800289353599800327");
    }
}
