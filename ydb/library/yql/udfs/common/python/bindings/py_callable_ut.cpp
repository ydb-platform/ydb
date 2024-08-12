#include "ut3/py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NPython;

Y_UNIT_TEST_SUITE(TPyCallableTest) {
    struct TTestCallable: public NUdf::TBoxedValue {
        NUdf::TUnboxedValue Run(
                const NUdf::IValueBuilder* valueBuilder,
                const NUdf::TUnboxedValuePod* args) const override
        {
            Y_UNUSED(valueBuilder);
            return NUdf::TUnboxedValuePod(args[0].Get<ui32>() + 42);
        }
    };

    Y_UNIT_TEST(FromPyFunction) {
        TPythonTestEngine engine;
        const NUdf::IValueBuilder* vb = &engine.GetValueBuilder();

        engine.ToMiniKQL<char* (*)(char*, ui32)>(
                "def Test():\n"
                "   def test(str, count):\n"
                "       return str * count\n"
                "   return test",
                [vb](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    NUdf::TUnboxedValue args[2];
                    args[0] = vb->NewString("j");
                    args[1] = NUdf::TUnboxedValuePod((ui32) 5);
                    auto result = value.Run(vb, args);

                    UNIT_ASSERT(result);
                    UNIT_ASSERT(5 == result.AsStringRef().Size());
                    UNIT_ASSERT_STRINGS_EQUAL(result.AsStringRef(), "jjjjj");
                });
    }

    Y_UNIT_TEST(ToPython) {
        TPythonTestEngine engine;
        engine.ToPython<i32 (*)(i32)>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new TTestCallable);
                },
                "def Test(value):\n"
                "    assert type(value).__name__ == 'TCallable'\n"
                "    assert value.__call__ != None\n"
                "    assert value(-2) == 40\n"
                "    assert value(-1) == 41\n"
                "    assert value(0) == 42\n"
                "    assert value(1) == 43\n"
                "    assert value(2) == 44\n");
    }

    Y_UNIT_TEST(ToPythonAndBack) {
        struct TTestCallable: public NUdf::TBoxedValue {
            NUdf::TUnboxedValue Run(
                    const NUdf::IValueBuilder* valueBuilder,
                    const NUdf::TUnboxedValuePod* args) const override
            {
                Y_UNUSED(valueBuilder);
                return NUdf::TUnboxedValuePod(args[0].Get<ui32>() + 42);
            }
        };

        TPythonTestEngine engine;
        engine.ToPythonAndBack<i32 (*)(i32)>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new TTestCallable);
                },
                "def Test(value): return value",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    NUdf::TUnboxedValue arg = NUdf::TUnboxedValuePod((ui32) 5);
                    const auto result = value.Run(nullptr, &arg);

                    UNIT_ASSERT(result);
                    UNIT_ASSERT_VALUES_EQUAL(47, result.Get<ui32>());
                });
    }
}
