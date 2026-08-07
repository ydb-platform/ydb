#include <ydb/services/workload_manager/ut/common/query_classifier_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NWorkloadManager {

using namespace NWorkloadManager;


namespace {

TString GetPostPoolId(const IQueryClassifier::TPostCompileClassifyResult& result) {
    UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TResolvedPoolId>(result),
        TStringBuilder() << "Expected TResolvedPoolId, got variant index " << result.index());
    return std::get<IQueryClassifier::TResolvedPoolId>(result).PoolId;
}

}  // anonymous namespace


Y_UNIT_TEST_SUITE(TQueryClassifierHasStream) {

    Y_UNIT_TEST(ShouldTriggerPendingCompilation) {
        TClassifyTestCase tc;
        tc.ClassifierHasStream = true;
        auto result = tc.RunPreClassify();
        UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TPendingCompilation>(result),
            TStringBuilder() << "Expected TPendingCompilation, got variant index " << result.index());
    }

    Y_UNIT_TEST(TrueMatchesStreamingQuery) {
        TClassifyTestCase tc;
        tc.ClassifierHasStream = true;
        auto result = tc.RunPostClassifyForStream(true);
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(result), "pool_target");
    }

    Y_UNIT_TEST(TrueDoesNotMatchNonStreamingQuery) {
        TClassifyTestCase tc;
        tc.ClassifierHasStream = true;
        auto result = tc.RunPostClassifyForStream(false);
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(result), "default");
    }

    Y_UNIT_TEST(FalseMatchesNonStreamingQuery) {
        TClassifyTestCase tc;
        tc.ClassifierHasStream = false;
        auto result = tc.RunPostClassifyForStream(false);
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(result), "pool_target");
    }

    Y_UNIT_TEST(FalseDoesNotMatchStreamingQuery) {
        TClassifyTestCase tc;
        tc.ClassifierHasStream = false;
        auto result = tc.RunPostClassifyForStream(true);
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(result), "default");
    }
}

}  // namespace NKikimr::NWorkloadManager
