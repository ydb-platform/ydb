#include "ut3/py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NPython;

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

}
