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
}
