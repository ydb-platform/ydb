#include "py_variant.h"
#include "ut3/py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NPython;

Y_UNIT_TEST_SUITE(TPyVariantTest) {
    Y_UNIT_TEST(FromPyWithIndex) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<NUdf::TVariant<float, ui32, char*>>(
                "def Test():\n"
                "    return (2, 'hello')\n",
                [](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT_EQUAL(value.GetVariantIndex(), 2);
                    auto item = value.GetVariantItem();
                    UNIT_ASSERT_STRINGS_EQUAL(item.AsStringRef(), "hello");
                });
    }

    Y_UNIT_TEST(FromPyWithName) {
        TPythonTestEngine engine;

        ui32 ageIdx = 0, nameIdx = 0;
        NUdf::TType* personType = engine.GetTypeBuilder().Struct()->
                AddField<ui32>("age", &ageIdx)
                .AddField<char*>("name", &nameIdx)
                .Build();

        NUdf::TType* variantType = engine.GetTypeBuilder()
                .Variant()->Over(personType).Build();

        engine.ToMiniKQL(
                variantType,
                "def Test():\n"
                "    return ('age', 99)\n",
                [ageIdx](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT_EQUAL(value.GetVariantIndex(), ageIdx);
                    auto item = value.GetVariantItem();
                    UNIT_ASSERT_EQUAL(item.Get<ui32>(), 99);
                });

        engine.ToMiniKQL(
                variantType,
                "def Test():\n"
                "    return ('name', 'Jamel')\n",
                [nameIdx](const NUdf::TUnboxedValuePod& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT_EQUAL(value.GetVariantIndex(), nameIdx);
                    auto item = value.GetVariantItem();
                    UNIT_ASSERT_STRINGS_EQUAL(item.AsStringRef(), "Jamel");
                });
    }

    Y_UNIT_TEST(ToPyWithIndex) {
        TPythonTestEngine engine;
        engine.ToPython<NUdf::TVariant<float, ui32, char*>>(
                [](const TType* /*type*/, const NUdf::IValueBuilder& vb) {
                    return vb.NewVariant(1, NUdf::TUnboxedValuePod((ui32) 42));
                },
                "def Test(value):\n"
                "    assert isinstance(value, tuple)\n"
                "    assert value == (1, 42)\n");
    }

    Y_UNIT_TEST(ToPyWithName) {
        TPythonTestEngine engine;

        ui32 ageIdx = 0, nameIdx = 0;
        NUdf::TType* personType = engine.GetTypeBuilder().Struct()->
                AddField<ui32>("age", &ageIdx)
                .AddField<NUdf::TUtf8>("name", &nameIdx)
                .Build();

        NUdf::TType* variantType = engine.GetTypeBuilder()
                .Variant()->Over(personType).Build();

        engine.ToPython(
                variantType,
                [ageIdx](const TType* /*type*/, const NUdf::IValueBuilder& vb) {
                    return vb.NewVariant(ageIdx, NUdf::TUnboxedValuePod((ui32) 99));
                },
                "def Test(value):\n"
                "    assert isinstance(value, tuple)\n"
                "    assert value == ('age', 99)\n"
        );

        engine.ToPython(
                variantType,
                [nameIdx](const TType* /*type*/, const NUdf::IValueBuilder& vb) {
                    return vb.NewVariant(nameIdx, vb.NewString("Jamel"));
                },
                "def Test(value):\n"
                "    assert isinstance(value, tuple)\n"
                "    assert value == ('name', 'Jamel')\n"
        );
    }
}
