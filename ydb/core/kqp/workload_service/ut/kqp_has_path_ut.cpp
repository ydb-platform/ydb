#include <ydb/core/base/path.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_query_classifier_ut_common.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NKqp {

using namespace NWorkload;


namespace {

TString GetPostPoolId(const IQueryClassifier::TPostCompileClassifyResult& result) {
    UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TResolvedPoolId>(result),
        TStringBuilder() << "Expected TResolvedPoolId, got variant index " << result.index());
    return std::get<IQueryClassifier::TResolvedPoolId>(result).PoolId;
}

}  // anonymous namespace


Y_UNIT_TEST_SUITE(TQueryClassifierHasPath) {

    Y_UNIT_TEST(ShouldTriggerPendingCompilation) {
        TClassifyTestCase tc;
        tc.ClassifierHasPath = "/Root/testdb/my_table";
        auto result = tc.RunPreClassify();
        UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TPendingCompilation>(result),
            TStringBuilder() << "Expected TPendingCompilation, got variant index " << result.index());
    }

    Y_UNIT_TEST(ShouldMatchExactPath) {
        TClassifyTestCase tc;
        tc.ClassifierHasPath = "/Root/testdb/my_table";
        auto result = tc.RunPostClassifyForPath("/Root/testdb/my_table");
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(result), "pool_target");
    }

    Y_UNIT_TEST(ShouldNotMatchDifferentPath) {
        TClassifyTestCase tc;
        tc.ClassifierHasPath = "/Root/testdb/my_table";
        auto result = tc.RunPostClassifyForPath("/Root/testdb/other_table");
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(result), "default");
    }

    Y_UNIT_TEST(ShouldMatchGlobPattern) {
        TClassifyTestCase tc;
        tc.ClassifierHasPath = "/Root/testdb/archive/*";
        auto result = tc.RunPostClassifyForPath("/Root/testdb/archive/orders_2024");
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(result), "pool_target");
    }

    Y_UNIT_TEST(ShouldMatchIgnoringMissingLeadingSlash) {
        // Plan-side paths may omit '/'. CanonizePath normalizes before regex match.
        TClassifyTestCase tc;
        tc.ClassifierHasPath = "/Root/testdb/my_table";
        auto result = tc.RunPostClassifyForPath("Root/testdb/my_table");
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(result), "pool_target");
    }
}

}  // namespace NKikimr::NKqp
