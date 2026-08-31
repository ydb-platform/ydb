#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/int128/bench/to_string_legacy.h>
#include <library/cpp/int128/int128.h>

#include <util/generic/cast.h>

#include <iterator>
#include <limits>
#include <type_traits>

#if defined(Y_HAVE_INT128)
namespace {
    TString IntrinsicToString(unsigned __int128 value, const bool negative = false) {
        constexpr size_t BufferSize = std::numeric_limits<ui128>::digits10 + 2;
        char buffer[BufferSize];
        char* position = std::end(buffer);
        do {
            *--position = static_cast<char>('0' + value % 10);
            value /= 10;
        } while (value != 0);
        if (negative) {
            *--position = '-';
        }
        return TString(position, std::end(buffer));
    }

    TString IntrinsicToString(const ui128 value) {
        const unsigned __int128 intrinsicValue =
            (static_cast<unsigned __int128>(GetHigh(value)) << 64) | GetLow(value);
        return IntrinsicToString(intrinsicValue);
    }

    TString IntrinsicToString(const i128 value) {
        unsigned __int128 magnitude =
            (static_cast<unsigned __int128>(GetHigh(value)) << 64) | GetLow(value);
        const bool negative = signbit(value);
        if (negative) {
            magnitude = ~magnitude + 1;
        }
        return IntrinsicToString(magnitude, negative);
    }
} // namespace
#endif

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
        UNIT_ASSERT_EQUAL(convertibleUi128Ui64, true);  // from ui64 to ui128 => safe
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
                -((i128(8741349088318632894ul) << 64) | i128(1258331728153556511ul))),
            "-161249429491168133245752281683002013215");

        // uint128
        UNIT_ASSERT_VALUES_EQUAL(ToString(ui128(0)), "0");
        UNIT_ASSERT_VALUES_EQUAL(ToString(ui128(42)), "42");
        UNIT_ASSERT_VALUES_EQUAL(ToString(std::numeric_limits<ui128>::min()), "0");
        UNIT_ASSERT_VALUES_EQUAL(ToString(std::numeric_limits<ui128>::max()), "340282366920938463463374607431768211455");

        // Just random number
        UNIT_ASSERT_VALUES_EQUAL(
            ToString(
                ((ui128(12745260439834612983ul) << 64) | ui128(10970669179777569799ul))),
            "235108557486403940296800289353599800327");
    }

    Y_UNIT_TEST(ToStringImplementationsMatchUnsignedTest) {
        const auto check = [](const ui128 value) {
#if defined(Y_HAVE_INT128)
            const TString expected = IntrinsicToString(value);
#else
            const TString expected = NInt128ToStringBenchmark::LegacyToString(value);
#endif
            UNIT_ASSERT_VALUES_EQUAL(
                NInt128ToStringBenchmark::LegacyToString(value),
                expected);
            UNIT_ASSERT_VALUES_EQUAL(
                ToString(value),
                expected);
            UNIT_ASSERT_VALUES_EQUAL(
                NInt128ToStringBenchmark::Base1e9ToString(value),
                expected);
            UNIT_ASSERT_VALUES_EQUAL(
                NInt128ToStringBenchmark::Base1e19ToString(value),
                expected);
            UNIT_ASSERT_VALUES_EQUAL(
                NInt128ToStringBenchmark::DivisionBy10ToString(value),
                expected);
        };

        for (const ui128 value : NInt128ToStringBenchmark::GetUnsignedToStringTestValues()) {
            check(value);
        }

        ui64 high = 0x243f6a8885a308d3ULL;
        ui64 low = 0x13198a2e03707344ULL;
        for (size_t i = 0; i < 1024; ++i) {
            high = high * 6364136223846793005ULL + 1442695040888963407ULL;
            low = low * 2862933555777941757ULL + 3037000493ULL;
            check(ui128{high, low});
        }
    }

    Y_UNIT_TEST(ToStringImplementationsMatchSignedTest) {
        const auto check = [](const i128 value) {
#if defined(Y_HAVE_INT128)
            const TString expected = IntrinsicToString(value);
#else
            const TString expected = NInt128ToStringBenchmark::LegacyToString(value);
#endif
            UNIT_ASSERT_VALUES_EQUAL(
                NInt128ToStringBenchmark::LegacyToString(value),
                expected);
            UNIT_ASSERT_VALUES_EQUAL(
                ToString(value),
                expected);
            UNIT_ASSERT_VALUES_EQUAL(
                NInt128ToStringBenchmark::Base1e9ToString(value),
                expected);
            UNIT_ASSERT_VALUES_EQUAL(
                NInt128ToStringBenchmark::Base1e19ToString(value),
                expected);
            UNIT_ASSERT_VALUES_EQUAL(
                NInt128ToStringBenchmark::DivisionBy10ToString(value),
                expected);
        };

        for (const i128 value : NInt128ToStringBenchmark::GetSignedToStringTestValues()) {
            check(value);
        }

        ui64 high = 0xa4093822299f31d0ULL;
        ui64 low = 0x082efa98ec4e6c89ULL;
        for (size_t i = 0; i < 1024; ++i) {
            high = high * 6364136223846793005ULL + 1442695040888963407ULL;
            low = low * 2862933555777941757ULL + 3037000493ULL;
            check(i128{high, low});
        }
    }
} // Y_UNIT_TEST_SUITE(Uint128Suite)
