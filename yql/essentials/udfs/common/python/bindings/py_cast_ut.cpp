#include "py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/strip.h>

using namespace NPython;

namespace {
template <typename TType>
void TestBadUtf8Encode() {
#if PY_MAJOR_VERSION == 2
    // In Python 2, strings can encode single surrogate pairs, so this issue does not occur there.
    return;
#endif // PY_MAJOR_VERSION == 2

    TPythonTestEngine engine;

    constexpr char programToRun[] = R"(
def Test():
    return "\uDC00"
)";
    constexpr char expectedError[] = R"(
Failed to convert the string to UTF-8 format. Original message is:
UnicodeEncodeError: 'utf-8' codec can't encode character '\udc00' in position 0: surrogates not allowed
)";

    UNIT_ASSERT_EXCEPTION_CONTAINS(
            engine.ToMiniKQL<TType>(
                 StripString(TString(programToRun)),
                [](const NUdf::TUnboxedValuePod& value) {
                    Y_UNUSED(value);
                }),
            yexception, StripString(TString(expectedError)));
}
} // namespace

Y_UNIT_TEST_SUITE(TPyCastTest) {
    Y_UNIT_TEST(FromPyStrToInt) {
        TPythonTestEngine engine;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            engine.ToMiniKQL<i32>(
                "def Test():\n"
                "    return '123a'",
                [](const NUdf::TUnboxedValuePod& value) {
                    Y_UNUSED(value);
                }),
            yexception, "str");
    }

    Y_UNIT_TEST(FromPyTupleToLong) {
        TPythonTestEngine engine;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            engine.ToMiniKQL<ui64>(
                "def Test():\n"
                "    return 1, 1",
                [](const NUdf::TUnboxedValuePod& value) {
                    Y_UNUSED(value);
                }),
            yexception, "tuple");
    }

    Y_UNIT_TEST(FromPyFuncToString) {
        TPythonTestEngine engine;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            engine.ToMiniKQL<char*>(
                "def f():\n"
                "    return 42\n"
                "def Test():\n"
                "    return f",
                [](const NUdf::TUnboxedValuePod& value) {
                    Y_UNUSED(value);
                }),
            yexception, "function");
    }

    Y_UNIT_TEST(FromPyNoneToString) {
        TPythonTestEngine engine;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            engine.ToMiniKQL<char*>(
                "def Test():\n"
                "    return None",
                [](const NUdf::TUnboxedValuePod& value) {
                    Y_UNUSED(value);
                }),
            yexception, "None");
    }

    Y_UNIT_TEST(BadFromPythonFloat) {
        TPythonTestEngine engine;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            engine.ToMiniKQL<float>(
                "def Test():\n"
                "    return '3 <dot> 1415926'",
                [](const NUdf::TUnboxedValuePod& value) {
                    Y_UNUSED(value);
                    Y_UNREACHABLE();
                }),
            yexception, "Cast error object '3 <dot> 1415926' to Float");
    }

#if PY_MAJOR_VERSION >= 3
#   define RETVAL "-1"
#else
#   define RETVAL "-18446744073709551616L"
#endif

    Y_UNIT_TEST(BadFromPythonLong) {
        TPythonTestEngine engine;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            engine.ToMiniKQL<ui64>(
                "def Test():\n"
                "    return " RETVAL,
                [](const NUdf::TUnboxedValuePod& value) {
                    Y_UNUSED(value);
                    Y_UNREACHABLE();
                }),
            yexception, "Cast error object " RETVAL " to Long");
    }

    Y_UNIT_TEST(BadFromPythonUtf8) {
        TestBadUtf8Encode<NUdf::TUtf8>();
    }

    Y_UNIT_TEST(BadFromPythonJson) {
        TestBadUtf8Encode<NUdf::TJson>();
    }

    Y_UNIT_TEST(BadToPythonJson) {
        TPythonTestEngine engine;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            engine.UnsafeCall<void(NUdf::TJson)>(
                [](const TType*, const NUdf::IValueBuilder& builder) {
                    // XXX: The value below is built with the
                    // following expression:
                    // $query = "a=1&t%EDb=2";
                    // $qdict = Url::QueryStringToDict($query);
                    // $qyson = Yson::From($qdict);
                    // $badJson = Yson::SerializeJson($qyson);
                    //
                    // For more info, see YQL-20231 and YQL-20220.
                    constexpr TStringBuf badJson = "\x7b\x22\x61\x22\x3a\x5b\x22\x31\x22\x5d\x2c\x22\x74\xed\x62\x22\x3a\x5b\x22\x32\x22\x5d\x7d";
                    return builder.NewString(badJson);
                },
                "def Test(arg):\n"
                "   pass",
                [](const NUdf::TUnboxedValuePod&) {
                    Y_UNREACHABLE();
                }
            ),
            yexception, "Failed to export Json given as args[0]");
    }
} // Y_UNIT_TEST_SUITE(TPyCastTest)
