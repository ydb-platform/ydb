#include "ut3/py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NPython;

Y_UNIT_TEST_SUITE(FromPyNone) {
    Y_UNIT_TEST(FromPyNone) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TOptional<ui32>>(
            "def Test(): return None",
            [](const NUdf::TUnboxedValuePod& value) {
                UNIT_ASSERT(!value);
        });
    }

    Y_UNIT_TEST(FromPyObject) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TOptional<ui32>>(
                "def Test(): return 42",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT_EQUAL(value.Get<ui32>(), 42);
                });
    }

    Y_UNIT_TEST(ToPyNone) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TOptional<char*>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod();
                },
                "def Test(value):\n"
                "    assert value == None\n");
    }

    Y_UNIT_TEST(ToPyFilledOptional) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TOptional<NUdf::TTuple<NUdf::TUtf8, bool>>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    const TOptionalType* optType =
                            static_cast<const TOptionalType*>(type);
                    NUdf::TUnboxedValue* items = nullptr;
                    auto tuple = vb.NewArray(static_cast<const TTupleType*>(optType->GetItemType())->GetElementsCount(), items);
                    items[0] = vb.NewString("test string");
                    items[1] = NUdf::TUnboxedValuePod(false);
                    return NUdf::TUnboxedValue(tuple);
                },
                "def Test(value):\n"
                "    assert isinstance(value, tuple)\n"
                "    assert len(value) == 2\n"
                "    assert value == ('test string', False)\n");
    }
}
