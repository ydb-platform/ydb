#include <ydb/public/sdk/cpp/src/library/decimal/yql_decimal.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NDecimal {

Y_UNIT_TEST_SUITE(TYqlDecimalTest) {
    void SimplePositiveTest(TInt128 v, ui8 precision, ui8 scale, const std::string& expected) {
        std::string result = ToString(v, precision, scale);
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
        TInt128 parsed = FromString(result, precision, scale);
        UNIT_ASSERT(parsed == v);
    }

    void SimpleNegativeFormatTest(TInt128 v, ui8 precision, ui8 scale) {
        std::string result = ToString(v, precision, scale);
        UNIT_ASSERT_VALUES_EQUAL(result, "");
    }

    Y_UNIT_TEST(TestZeroFormat) {
        UNIT_ASSERT_VALUES_EQUAL(ToString(0, 1, 0), "0");
        UNIT_ASSERT_VALUES_EQUAL(ToString(0, 15, 6), "0");
        UNIT_ASSERT_VALUES_EQUAL(ToString(0, 15, 0), "0");
    }

    Y_UNIT_TEST(TestZeroScale) {
        SimplePositiveTest(1, 5, 0, "1");
        SimplePositiveTest(10, 5, 0, "10");
        SimplePositiveTest(100, 5, 0, "100");
        SimplePositiveTest(1000, 5, 0, "1000");
        SimplePositiveTest(10000, 5, 0, "10000");
        SimpleNegativeFormatTest(100000, 5, 0);
        SimpleNegativeFormatTest(1000000, 5, 0);

        // negative numbers
        SimplePositiveTest(-1, 5, 0, "-1");
        SimplePositiveTest(-10, 5, 0, "-10");
        SimplePositiveTest(-100, 5, 0, "-100");
        SimplePositiveTest(-1000, 5, 0, "-1000");
        SimplePositiveTest(-10000, 5, 0, "-10000");
        SimpleNegativeFormatTest(-100000, 5, 0);
    }

    Y_UNIT_TEST(TestFormats) {
        // we have no trailing zeros
        SimplePositiveTest(1, 15, 6, "0.000001");
        SimplePositiveTest(10, 15, 6, "0.00001");
        SimplePositiveTest(100, 15, 6, "0.0001");
        SimplePositiveTest(1000, 15, 6, "0.001");
        SimplePositiveTest(10000, 15, 6, "0.01");
        SimplePositiveTest(100000, 15, 6, "0.1");
        SimplePositiveTest(1000000, 15, 6, "1");
        SimplePositiveTest(10000000, 15, 6, "10");
        SimplePositiveTest(100000000, 15, 6, "100");

        SimplePositiveTest(2020000, 15, 6, "2.02");
        SimplePositiveTest(3003000, 15, 6, "3.003");

        // negative numbers
        SimplePositiveTest(-1, 15, 6, "-0.000001");
        SimplePositiveTest(-10, 15, 6, "-0.00001");
        SimplePositiveTest(-100, 15, 6, "-0.0001");
        SimplePositiveTest(-1000, 15, 6, "-0.001");
        SimplePositiveTest(-10000, 15, 6, "-0.01");
        SimplePositiveTest(-100000, 15, 6, "-0.1");
        SimplePositiveTest(-1000000, 15, 6, "-1");
        SimplePositiveTest(-10000000, 15, 6, "-10");
        SimplePositiveTest(-100000000, 15, 6, "-100");

        SimplePositiveTest(-2020000, 15, 6, "-2.02");
        SimplePositiveTest(-3003000, 15, 6, "-3.003");

        SimplePositiveTest(1, 15, 6, "0.000001");
        SimplePositiveTest(12, 15, 6, "0.000012");
        SimplePositiveTest(123, 15, 6, "0.000123");
        SimplePositiveTest(1234, 15, 6, "0.001234");
        SimplePositiveTest(12345, 15, 6, "0.012345");
        SimplePositiveTest(123456, 15, 6, "0.123456");
        SimplePositiveTest(1234567, 15, 6, "1.234567");
        SimplePositiveTest(12345678, 15, 6, "12.345678");
        SimplePositiveTest(123456789, 15, 6, "123.456789");
        SimplePositiveTest(1234567898, 15, 6, "1234.567898");
        SimplePositiveTest(12345678987ll, 15, 6, "12345.678987");
        SimplePositiveTest(123456789876ll, 15, 6, "123456.789876");
    }

    Y_UNIT_TEST(TestHugeNumberFormat) {
        TInt128 max120 = Inf() - 1;
        const char max120String[] = "99999999999999999999999999999999999"; // 35 digits
        static_assert(sizeof(max120String) == 36, "sizeof(max120String) == 36");
        SimplePositiveTest(max120, MaxPrecision, 0, max120String);
        SimplePositiveTest(max120 + 1, MaxPrecision, 0, "inf");

        TInt128 min120 = -Inf() + 1;
        const char min120String[] = "-99999999999999999999999999999999999";
        static_assert(sizeof(min120String) == 37, "sizeof(min120String) == 37");
        SimplePositiveTest(min120, MaxPrecision, 0, min120String);
        SimplePositiveTest(min120 - 1, MaxPrecision, 0, "-inf");

        // take spot for sign and zero before dot
        const char min120StringAfterDot[] = "-0.99999999999999999999999999999999999"; // 35 by nine + leading zero
        static_assert(sizeof(min120StringAfterDot) == 39, "sizeof(min120StringAfterDot) == 39");
        SimplePositiveTest(min120, MaxPrecision, MaxPrecision, min120StringAfterDot);

        SimpleNegativeFormatTest(1, MaxPrecision + 1, MaxPrecision + 1);
        SimpleNegativeFormatTest(1, MaxPrecision + 1, 0);
        SimpleNegativeFormatTest(1, 2, 3);
    }

    Y_UNIT_TEST(TestFormStringRoundToEven) {
        UNIT_ASSERT(FromString(".51", 1, 0) == 1);
        UNIT_ASSERT(FromString("-0.51", 1, 0) == -1);

        UNIT_ASSERT(FromString("+00000008.5", 1, 0) == 8);
        UNIT_ASSERT(FromString("-8.5000000000000000000000000000000", 1, 0) == -8);

        UNIT_ASSERT(FromString("00008.51", 1, 0) == 9);
        UNIT_ASSERT(FromString("-8.5000000000000000000000000000001", 1, 0) == -9);

        UNIT_ASSERT(FromString("09.499999999999999999999999999999999999999999999999999999999", 1, 0) == 9);
        UNIT_ASSERT(FromString("-9.499999999999999999999999999999999999999999999999999999999", 1, 0) == -9);

        UNIT_ASSERT(FromString("9.50", 2, 0) == 10);
        UNIT_ASSERT(FromString("-9.5", 2, 0) == -10);

        UNIT_ASSERT(FromString("+0.9949", 2, 2) == 99);
        UNIT_ASSERT(FromString("-0.9949", 2, 2) == -99);
    }

    Y_UNIT_TEST(TestInfinityValues) {
        UNIT_ASSERT(FromString("+1", 1, 1) == Inf());
        UNIT_ASSERT(FromString("-1", 1, 1) == -Inf());

        UNIT_ASSERT(FromString("10.000", 1, 0) == Inf());
        UNIT_ASSERT(FromString("-10.000", 1, 0) == -Inf());

        UNIT_ASSERT(FromString("9.500", 1, 0) == Inf());
        UNIT_ASSERT(FromString("-9.500", 1, 0) == -Inf());

        UNIT_ASSERT(FromString("+0.950", 1, 1) == Inf());
        UNIT_ASSERT(FromString("-0.950", 1, 1) == -Inf());

        UNIT_ASSERT(FromString("+0.9950", 2, 2) == Inf());
        UNIT_ASSERT(FromString("-0.9950", 2, 2) == -Inf());

        UNIT_ASSERT(FromString("9999999999999999999999999999999999999.5", 35, 0) == Inf());
        UNIT_ASSERT(FromString("-9999999999999999999999999999999999999.5", 35, 0) == -Inf());
    }

    Y_UNIT_TEST(TestInvalidValues) {
        UNIT_ASSERT(IsValid("+999999999999999991234567890.039493804903849038490312345678909999999999999999990"));

        UNIT_ASSERT(!IsValid("")); // empty
        UNIT_ASSERT(!IsValid("12.2.3")); // double dot
        UNIT_ASSERT(!IsValid("+-12")); // extra sign
        UNIT_ASSERT(!IsValid("463786378O74674")); // letter inside

        UNIT_ASSERT(IsError(FromString("", 35, 15))); // empty
        UNIT_ASSERT(IsError(FromString("12.2.3", 35, 15))); // double dot
        UNIT_ASSERT(IsError(FromString("+-12", 35, 15))); // extra sign
        UNIT_ASSERT(IsError(FromString("463786378O74674", 35, 15))); // letter inside
        UNIT_ASSERT(IsError(FromString("+7.039493804E1", 35, 5))); // letter in tail after scale
    }

    Y_UNIT_TEST(TestSpecialAsString) {
        UNIT_ASSERT(IsValid("Nan"));
        UNIT_ASSERT(IsValid("INF"));
        UNIT_ASSERT(IsValid("-inf"));

        UNIT_ASSERT_VALUES_EQUAL(ToString(Nan(), 10, 2), "nan");

        UNIT_ASSERT_VALUES_EQUAL(ToString(+Inf(), 10, 2), "inf");
        UNIT_ASSERT_VALUES_EQUAL(ToString(-Inf(), 10, 2), "-inf");

        UNIT_ASSERT(IsNan(FromString("nan", 10, 2)));
        UNIT_ASSERT(IsInf(FromString("+INf", MaxPrecision, 6)));
        UNIT_ASSERT(IsInf(FromString("-inF", 4, 2)));
    }

    Y_UNIT_TEST(TestToStringOfNonNormal) {
        // above Inf
        for (TInt128 i = Inf() + 2, end = Inf() + 100; i < end; i++) {
            UNIT_ASSERT(!IsNormal(i));
            UNIT_ASSERT(ToString(i, MaxPrecision, 0) == nullptr);
        }

        // below -Inf
        for (TInt128 i = -Inf() - 2, end = -Inf() - 100; i < end; i--) {
            UNIT_ASSERT(!IsNormal(i));
            UNIT_ASSERT(ToString(i, MaxPrecision, 0) == nullptr);
        }
    }
}

}
