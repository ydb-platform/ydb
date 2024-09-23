#include "ut3/py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NPython;

extern const char SimpleDataTag[] = "SimpleData";
extern const char PythonTestTag[] = PYTHON_TEST_TAG;

struct TSimpleData {
    TString Name;
    ui32 Age;

    TSimpleData(const TString& name, ui32 age)
        : Name(name)
        , Age(age)
    {}
};

using TSimpleDataResource = NUdf::TBoxedResource<TSimpleData, SimpleDataTag>;

Y_UNIT_TEST_SUITE(TPyResourceTest) {
    Y_UNIT_TEST(MkqlObject) {
        TPythonTestEngine engine;
        TPyObjectPtr pyValue = engine.ToPython<NUdf::TResource<SimpleDataTag>>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValuePod(new TSimpleDataResource("Jamel", 99));
                },
                "import yql\n"
                "\n"
                "def Test(value):\n"
                "   assert str(value).startswith('<capsule object \"YqlResourceCapsule\" at ')\n"
                "   assert repr(value).startswith('<capsule object \"YqlResourceCapsule\" at ')\n"
                "   assert type(value).__name__ == 'PyCapsule'\n"
                "   return value\n");
        UNIT_ASSERT(!!pyValue);

        engine.ToMiniKQLWithArg<NUdf::TResource<SimpleDataTag>>(
                    pyValue.Get(),
                    "import yql\n"
                    "\n"
                    "def Test(value):\n"
                    "    return value\n",
                    [](const NUdf::TUnboxedValuePod& value) {
                        UNIT_ASSERT(value);;
                        UNIT_ASSERT(value.IsBoxed());
                        UNIT_ASSERT_STRINGS_EQUAL(value.GetResourceTag(), SimpleDataTag);
                        auto simpleData =
                                reinterpret_cast<TSimpleData*>(value.GetResource());
                        UNIT_ASSERT_EQUAL(simpleData->Age, 99);
                        UNIT_ASSERT_STRINGS_EQUAL(simpleData->Name, "Jamel");
                    });
    }

    Y_UNIT_TEST(PythonObject) {
        TPythonTestEngine engine;
        NUdf::TUnboxedValue mkqlValue = engine.FromPython<NUdf::TResource<PythonTestTag>>(
                "class CustomStruct:\n"
                "    def __init__(self, name, age):\n"
                "        self.name = name\n"
                "        self.age = age\n"
                "\n"
                "def Test():\n"
                "   return CustomStruct('Jamel', 97)\n");
        UNIT_ASSERT(mkqlValue);
        UNIT_ASSERT_STRINGS_EQUAL(mkqlValue.GetResourceTag(), PythonTestTag);

        TPyObjectPtr pyValue = engine.ToPython<NUdf::TResource<PythonTestTag>>(
                [mkqlValue](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return mkqlValue;
                },
                "def Test(value):\n"
                "    assert isinstance(value, CustomStruct)\n"
                "    assert value.age, 97\n"
                "    assert value.name, 'Jamel'\n");
        UNIT_ASSERT(!!pyValue);
    }
}
