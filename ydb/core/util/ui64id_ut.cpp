#include "ui64id.h"

#include <library/cpp/testing/unittest/registar.h>

STRONG_UI64_TYPE_DEF_NDC(TType2, 11);
STRONG_UI64_TYPE_DEF_DV(TType3, 10, 12);

Y_UNIT_TEST_SUITE(TStrongTypeTest) {
    Y_UNIT_TEST(DefaultConstructorDeleted) {
        static_assert(!std::is_default_constructible<TType2>::value, "Type TType2 shouldn't have default constructor");
        TType2 type(11);
        UNIT_ASSERT(!bool(type));
        type.SetValue(10);
        UNIT_ASSERT(bool(type));
    }

    Y_UNIT_TEST(DefaultConstructorValue) {
        TType3 type;
        UNIT_ASSERT_VALUES_EQUAL(type.GetValue(), 10);
        UNIT_ASSERT(bool(type));
        type.SetValue(12);
        UNIT_ASSERT(!bool(type));
    }
}
