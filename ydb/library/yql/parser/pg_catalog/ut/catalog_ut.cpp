#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TFunctionsTests) {
    Y_UNIT_TEST(TestMissing) {
        UNIT_ASSERT_EXCEPTION(LookupProc("_foo_bar_", {}), yexception);
    }

    Y_UNIT_TEST(TestMismatchArgTypes) {
        UNIT_ASSERT_EXCEPTION(LookupProc("int4pl", {}), yexception);
    }

    Y_UNIT_TEST(TestOk) {
        auto ret = LookupProc("int4pl", {"int4", "int4"});
        UNIT_ASSERT_VALUES_EQUAL(ret.ResultType, "int4");
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[0], "int4");
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[1], "int4");
        UNIT_ASSERT_VALUES_EQUAL(ret.Src, "int4pl");

        ret = LookupProc("substring", {"text", "int4", "int4"});
        UNIT_ASSERT_VALUES_EQUAL(ret.ResultType, "text");
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[0], "text");
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[1], "int4");
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[2], "int4");
        UNIT_ASSERT_VALUES_EQUAL(ret.Src, "text_substr");

        ret = LookupProc("substring", {"text", "int4"});
        UNIT_ASSERT_VALUES_EQUAL(ret.ResultType, "text");
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[0], "text");
        UNIT_ASSERT_VALUES_EQUAL(ret.ArgTypes[1], "int4");
        UNIT_ASSERT_VALUES_EQUAL(ret.Src, "text_substr_no_len");
    }
}
