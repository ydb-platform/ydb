#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TFunctionsTests) {
    Y_UNIT_TEST(TestMissing) {
        auto ret = LookupFunctionSignature("_foo_bar_");
        UNIT_ASSERT(!ret);
    }

    Y_UNIT_TEST(TestAmbiguous) {
        UNIT_ASSERT_EXCEPTION(LookupFunctionSignature("substring"), yexception);
    }

    Y_UNIT_TEST(TestOk) {
        auto ret = LookupFunctionSignature("int4pl");
        UNIT_ASSERT(ret);
        UNIT_ASSERT_VALUES_EQUAL(ret->ResultType, "int4");
        UNIT_ASSERT_VALUES_EQUAL(ret->ArgTypes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ret->ArgTypes[0], "int4");
        UNIT_ASSERT_VALUES_EQUAL(ret->ArgTypes[1], "int4");

        ret = LookupFunctionSignature("text_substr");
        UNIT_ASSERT(ret);
        UNIT_ASSERT_VALUES_EQUAL(ret->ResultType, "text");
        UNIT_ASSERT_VALUES_EQUAL(ret->ArgTypes.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ret->ArgTypes[0], "text");
        UNIT_ASSERT_VALUES_EQUAL(ret->ArgTypes[1], "int4");
        UNIT_ASSERT_VALUES_EQUAL(ret->ArgTypes[2], "int4");

        ret = LookupFunctionSignature("text_substr_no_len");
        UNIT_ASSERT(ret);
        UNIT_ASSERT_VALUES_EQUAL(ret->ResultType, "text");
        UNIT_ASSERT_VALUES_EQUAL(ret->ArgTypes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ret->ArgTypes[0], "text");
        UNIT_ASSERT_VALUES_EQUAL(ret->ArgTypes[1], "int4");
    }
}
