#include "ut3/py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NPython;

Y_UNIT_TEST_SUITE(TPyTupleTest) {
    Y_UNIT_TEST(FromPyEmptyTuple) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TTuple<>>(
                "def Test(): return ()",
                [](const NUdf::TUnboxedValuePod&) {});
    }

    Y_UNIT_TEST(FromPyList) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TTuple<int, int, int>>(
                "def Test(): return [1, 2, 3]",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT_EQUAL(value.GetElement(0).Get<int>(), 1);
                    UNIT_ASSERT_EQUAL(value.GetElement(1).Get<int>(), 2);
                    UNIT_ASSERT_EQUAL(value.GetElement(2).Get<int>(), 3);
                });
    }

    Y_UNIT_TEST(FromPyIter) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TTuple<int, int, int>>(
                "def Test(): return iter({1, 2, 3})",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT_EQUAL(value.GetElement(0).Get<int>(), 1);
                    UNIT_ASSERT_EQUAL(value.GetElement(1).Get<int>(), 2);
                    UNIT_ASSERT_EQUAL(value.GetElement(2).Get<int>(), 3);
                });
    }

    Y_UNIT_TEST(FromPyTuple) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TTuple<int, double, char*>>(
                "def Test(): return (1, float(2.3), '4')",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT_EQUAL(value.GetElement(0).Get<int>(), 1);
                    auto second = value.GetElement(1);
                    UNIT_ASSERT_DOUBLES_EQUAL(second.Get<double>(), 2.3, 0.0001);
                    const auto third = value.GetElement(2);
                    UNIT_ASSERT_EQUAL(third.AsStringRef(), "4");
                });
    }

    Y_UNIT_TEST(FromPyTupleInTuple) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TTuple<ui32, NUdf::TTuple<ui8, float>, char*>>(
                "def Test(): return (1, (2, float(3.4)), '5')",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(value.IsBoxed());
                    UNIT_ASSERT_EQUAL(value.GetElement(0).Get<ui32>(), 1);

                    auto second = value.GetElement(1);
                    UNIT_ASSERT(second);
                    UNIT_ASSERT(second.IsBoxed());
                    UNIT_ASSERT_EQUAL(second.GetElement(0).Get<ui8>(), 2);
                    UNIT_ASSERT_DOUBLES_EQUAL(
                            second.GetElement(1).Get<float>(), 3.4, 0.0001);

                    const auto third = value.GetElement(2);
                    UNIT_ASSERT_EQUAL(third.AsStringRef(), "5");
                });
    }

    Y_UNIT_TEST(ToPyEmptyTuple) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TTuple<>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    NUdf::TUnboxedValue* items = nullptr;
                    return vb.NewArray(static_cast<const TTupleType*>(type)->GetElementsCount(), items);
                },
                "def Test(value):\n"
                "    assert isinstance(value, tuple)\n"
                "    assert len(value) == 0\n"
                "    assert value == ()\n");
    }

    Y_UNIT_TEST(ToPyTuple) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TTuple<NUdf::TUtf8, ui64, ui8, float>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    NUdf::TUnboxedValue* items = nullptr;
                    auto tuple = vb.NewArray(static_cast<const TTupleType*>(type)->GetElementsCount(), items);
                    items[0] = vb.NewString("111");
                    items[1] = NUdf::TUnboxedValuePod((ui64) 2);
                    items[2] = NUdf::TUnboxedValuePod((ui8) 3);
                    items[3] = NUdf::TUnboxedValuePod((float) 4.5);
                    return tuple;
                },
                "def Test(value):\n"
                "    assert isinstance(value, tuple)\n"
                "    assert len(value) == 4\n"
                "    assert value == ('111', 2, 3, 4.5)\n");
    }
}
