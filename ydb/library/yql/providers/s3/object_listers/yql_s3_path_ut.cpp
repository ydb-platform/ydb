#include "yql_s3_path.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NS3 {

Y_UNIT_TEST_SUITE(TPathTests) {
    Y_UNIT_TEST(NormalizeEmpty) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(NormalizePath(""), std::exception, "Path should not be empty");
    }

    Y_UNIT_TEST(NormalizeSlashes) {
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("/"), "/");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("//"), "/");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("///"), "/");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("////////////"), "/");
    }

    Y_UNIT_TEST(NormalizeNoSlashes) {
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("a"), "a");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("abc"), "abc");
    }

    Y_UNIT_TEST(NormalizeWithSlashes) {
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("a/b/c"), "a/b/c");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("a/b/c/"), "a/b/c/");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("/a/b/c"), "a/b/c");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("/a/b/c/"), "a/b/c/");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("///a/b/c///"), "a/b/c/");
    }
}

}
