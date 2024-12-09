#include "parse_double.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TParseDouble) {

    template <typename T, typename F>
    void ParseAndCheck(TStringBuf buf, F f, T expected) {
        T result = 0;
        UNIT_ASSERT(f(buf, result));
        UNIT_ASSERT_DOUBLES_EQUAL(expected, result, 1e-6);
    }

    Y_UNIT_TEST(ExactValues) {
        ParseAndCheck(TStringBuf("nan"), TryFloatFromString, std::numeric_limits<float>::quiet_NaN());
        ParseAndCheck(TStringBuf("nAn"), TryDoubleFromString, std::numeric_limits<double>::quiet_NaN());

        ParseAndCheck(TStringBuf("+nan"), TryFloatFromString, std::numeric_limits<float>::quiet_NaN());
        ParseAndCheck(TStringBuf("+NAN"), TryDoubleFromString, std::numeric_limits<double>::quiet_NaN());

        ParseAndCheck(TStringBuf("-nan"), TryFloatFromString, std::numeric_limits<float>::quiet_NaN());
        ParseAndCheck(TStringBuf("-NaN"), TryDoubleFromString, std::numeric_limits<double>::quiet_NaN());

        ParseAndCheck(TStringBuf("inf"), TryFloatFromString, std::numeric_limits<float>::infinity());
        ParseAndCheck(TStringBuf("iNf"), TryDoubleFromString, std::numeric_limits<double>::infinity());

        ParseAndCheck(TStringBuf("+inf"), TryFloatFromString, std::numeric_limits<float>::infinity());
        ParseAndCheck(TStringBuf("+INF"), TryDoubleFromString, std::numeric_limits<double>::infinity());

        ParseAndCheck(TStringBuf("-inf"), TryFloatFromString, -std::numeric_limits<float>::infinity());
        ParseAndCheck(TStringBuf("-InF"), TryDoubleFromString, -std::numeric_limits<double>::infinity());

        ParseAndCheck<float>(TStringBuf("-12.3456"), TryFloatFromString, -12.3456);
        ParseAndCheck(TStringBuf("-12.3456"), TryDoubleFromString, -12.3456);

        ParseAndCheck<float>(TStringBuf("1.23e-2"), TryFloatFromString, 0.0123);
        ParseAndCheck(TStringBuf("1.23e-2"), TryDoubleFromString, 0.0123);

        UNIT_ASSERT_EQUAL(FloatFromString(TStringBuf("iNf")), std::numeric_limits<float>::infinity());
        UNIT_ASSERT_EQUAL(DoubleFromString(TStringBuf("iNf")), std::numeric_limits<float>::infinity());
    }

    Y_UNIT_TEST(Errors) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(FloatFromString(TStringBuf("")), std::exception, "unable to parse float from ''");
        UNIT_ASSERT_EXCEPTION_CONTAINS(DoubleFromString(TStringBuf("")), std::exception, "unable to parse double from ''");

        UNIT_ASSERT_EXCEPTION_CONTAINS(FloatFromString(TStringBuf("info")), std::exception, "unable to parse float from 'info'");
        UNIT_ASSERT_EXCEPTION_CONTAINS(DoubleFromString(TStringBuf("-nana")), std::exception, "unable to parse double from '-nana'");

        UNIT_ASSERT_EXCEPTION_CONTAINS(FloatFromString(TStringBuf(nullptr)), std::exception, "unable to parse float from ''");
        UNIT_ASSERT_EXCEPTION_CONTAINS(DoubleFromString(TStringBuf(nullptr)), std::exception, "unable to parse double from ''");
    }
}
}
