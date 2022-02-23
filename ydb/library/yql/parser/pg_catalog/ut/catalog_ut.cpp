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
        UNIT_ASSERT_NO_DIFF(*ret, "Callable<(pg_int4,pg_int4)->pg_int4>");

        ret = LookupFunctionSignature("text_substr");
        UNIT_ASSERT(ret);
        UNIT_ASSERT_NO_DIFF(*ret, "Callable<(pg_text,pg_int4,pg_int4)->pg_text>");

        ret = LookupFunctionSignature("text_substr_no_len");
        UNIT_ASSERT(ret);
        UNIT_ASSERT_NO_DIFF(*ret, "Callable<(pg_text,pg_int4)->pg_text>");
    }
}
