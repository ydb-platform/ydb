#include "ut3/py_test_engine.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NPython;

Y_UNIT_TEST_SUITE(TPyVoidTest) {
    Y_UNIT_TEST(FromPython) {
        TPythonTestEngine engine;
        engine.ToMiniKQL<void>(
                "import yql\n"
                "\n"
                "def Test():\n"
                "    return yql.Void\n",
                [](const NUdf::TUnboxedValue& value) {
                    UNIT_ASSERT(value);
                    UNIT_ASSERT(false == value.IsBoxed());
                });
    }

    Y_UNIT_TEST(ToPython) {
        TPythonTestEngine engine;
        engine.ToPython<void>(
                [](const TType* type, const NUdf::IValueBuilder& vb) {
                    Y_UNUSED(type); Y_UNUSED(vb);
                    return NUdf::TUnboxedValue::Void();
                },
                "import yql\n"
                "\n"
                "def Test(value):\n"
                "   assert str(value) == 'yql.Void'\n"
                "   assert repr(value) == 'yql.Void'\n"
                "   assert isinstance(value, yql.TVoid)\n"
                "   assert value is yql.Void\n");
    }
}
