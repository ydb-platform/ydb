#include <ydb/core/base/http_database_param.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(HttpDatabaseParam) {

Y_UNIT_TEST(FromQueryParam) {
    TCgiParameters params("database=db1");
    UNIT_ASSERT_VALUES_EQUAL(ExtractHttpDatabaseParam(params, "GET", {}, {}), "db1");
}

Y_UNIT_TEST(NonPostWithoutQueryParam) {
    TCgiParameters params;
    UNIT_ASSERT(ExtractHttpDatabaseParam(params, "GET", {}, {}).empty());
}

Y_UNIT_TEST(PostWithEmptyBodyWithoutQueryParam) {
    TCgiParameters params;
    UNIT_ASSERT(ExtractHttpDatabaseParam(params, "POST", {}, {}).empty());
}

Y_UNIT_TEST(PostWithBodyWithoutDatabaseField) {
    TCgiParameters params;
    UNIT_ASSERT(ExtractHttpDatabaseParam(params, "POST", R"({"query":"SELECT 1"})", "application/json").empty());
}

Y_UNIT_TEST(PostWithBodyWithDatabaseField) {
    TCgiParameters params;
    UNIT_ASSERT_VALUES_EQUAL(
        ExtractHttpDatabaseParam(params, "POST", R"({"database":"db2","query":"SELECT 1"})", "application/json"),
        "db2");
}

Y_UNIT_TEST(QueryParamHasPriorityOverPostBody) {
    TCgiParameters params("database=db1");
    UNIT_ASSERT_VALUES_EQUAL(
        ExtractHttpDatabaseParam(params, "POST", R"({"database":"db2"})", "application/json"),
        "db1");
}

Y_UNIT_TEST(PostWithBodyWithEmptyDatabaseField) {
    TCgiParameters params;
    UNIT_ASSERT(ExtractHttpDatabaseParam(params, "POST", R"({"database":"","query":"SELECT 1"})", "application/json").empty());
}

Y_UNIT_TEST(PostWithBodyWithMalformedPostBody) {
    TCgiParameters params;
    UNIT_ASSERT(ExtractHttpDatabaseParam(params, "POST", R"({"database":"db")", "application/json").empty());
}

Y_UNIT_TEST(PostWithEmptyContentTypeAndJsonBody) {
    TCgiParameters params;
    UNIT_ASSERT_VALUES_EQUAL(
        ExtractHttpDatabaseParam(params, "POST", R"({"database":"db-from-body"})", {}),
        "db-from-body");
}

Y_UNIT_TEST(PostWithFormUrlEncodedContentTypeIsIgnored) {
    TCgiParameters params;
    UNIT_ASSERT(ExtractHttpDatabaseParam(params, "POST", "database=db", "application/x-www-form-urlencoded").empty());
}

Y_UNIT_TEST(QueryParamHasPriorityOverPostBodyWithContentType) {
    TCgiParameters params("database=db1");
    UNIT_ASSERT_VALUES_EQUAL(
        ExtractHttpDatabaseParam(params, "POST", R"({"database":"db2"})", "application/json"),
        "db1");
}

Y_UNIT_TEST(TrimHttpContentTypeHeaderStripsParametersAndWhitespace) {
    UNIT_ASSERT_VALUES_EQUAL(TrimHttpContentTypeHeader(" application/json ; charset=utf-8 "), "application/json");
}

Y_UNIT_TEST(ExtractHttpDatabaseParamFromUrl) {
    UNIT_ASSERT_VALUES_EQUAL(
        ExtractHttpDatabaseParamFromUrl("/viewer/json/query?database=db1", "GET", {}, {}),
        "db1");
    UNIT_ASSERT_VALUES_EQUAL(
        ExtractHttpDatabaseParamFromUrl("/viewer/json/query", "POST", R"({"database":"db2"})", "application/json"),
        "db2");
}

}

} // namespace NKikimr
