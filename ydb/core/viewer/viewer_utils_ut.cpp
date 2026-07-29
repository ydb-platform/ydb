#include "viewer_utils.h"

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NViewer {

Y_UNIT_TEST_SUITE(GetDatabaseParam) {

Y_UNIT_TEST(FromQueryParam) {
    TCgiParameters params("database=db1");
    UNIT_ASSERT_VALUES_EQUAL(GetDatabaseParam(params, "GET", {}), "db1");
}

Y_UNIT_TEST(NonPostWithoutQueryParam) {
    TCgiParameters params;
    UNIT_ASSERT(GetDatabaseParam(params, "GET", {}).empty());
}

Y_UNIT_TEST(PostWithEmptyBodyWithoutQueryParam) {
    TCgiParameters params;
    UNIT_ASSERT(GetDatabaseParam(params, "POST", {}).empty());
}

Y_UNIT_TEST(PostWithBodyWithoutDatabaseField) {
    TCgiParameters params;
    UNIT_ASSERT(GetDatabaseParam(params, "POST", R"({"query":"SELECT 1"})").empty());
}

Y_UNIT_TEST(PostWithBodyWithDatabaseField) {
    TCgiParameters params;
    UNIT_ASSERT_VALUES_EQUAL(
        GetDatabaseParam(params, "POST", R"({"database":"db2","query":"SELECT 1"})"),
        "db2");
}

Y_UNIT_TEST(QueryParamHasPriorityOverPostBody) {
    TCgiParameters params("database=db1");
    UNIT_ASSERT_VALUES_EQUAL(
        GetDatabaseParam(params, "POST", R"({"database":"db2"})"),
        "db1");
}

Y_UNIT_TEST(PostWithBodyWithEmptyDatabaseField) {
    TCgiParameters params;
    UNIT_ASSERT(GetDatabaseParam(params, "POST", R"({"database":"","query":"SELECT 1"})").empty());
}

Y_UNIT_TEST(PostWithBodyWithMalformedPostBody) {
    TCgiParameters params;
    UNIT_ASSERT(GetDatabaseParam(params, "POST", R"({"database":"db")").empty());
}

}

} // namespace NKikimr::NViewer
