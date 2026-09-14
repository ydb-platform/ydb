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


Y_UNIT_TEST_SUITE(TQueryClassifierStreamingAlwaysPostClassify) {

    Y_UNIT_TEST(NonStreamingResolvesStaticInPreCompile) {
        TClassifyTestCase tc;
        tc.ClassifierHasAppName = "app";
        tc.ContextAppName = "app";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify(/*isStreamingQuery=*/false)), "pool_target");
    }

    Y_UNIT_TEST(StreamingAlwaysPendingWhenClassifiersExist) {
        // Static-only classifier would resolve in pre-compile for ordinary queries,
        // but streaming must still defer to post-compile from ResumeRank 0.
        TClassifyTestCase tc;
        tc.ClassifierHasAppName = "app";
        tc.ContextAppName = "app";
        auto result = tc.RunPreClassify(/*isStreamingQuery=*/true);
        UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TPendingCompilation>(result),
            TStringBuilder() << "Expected TPendingCompilation, got variant index " << result.index());
        UNIT_ASSERT_VALUES_EQUAL(std::get<IQueryClassifier::TPendingCompilation>(result).ResumeRank, 0);
    }

    Y_UNIT_TEST(StreamingStaticOnlyResolvesViaPostCompile) {
        TClassifyTestCase tc;
        tc.ClassifierHasAppName = "app";
        tc.ContextAppName = "app";
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(tc.RunPostClassifyForStream(true)), "pool_target");
    }

    Y_UNIT_TEST(StreamingHasPathViaAlwaysPending) {
        TClassifyTestCase tc;
        tc.ClassifierHasPath = "/Root/testdb/my_table";
        auto pre = tc.RunPreClassify(/*isStreamingQuery=*/true);
        UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TPendingCompilation>(pre),
            TStringBuilder() << "Expected TPendingCompilation, got variant index " << pre.index());
        UNIT_ASSERT_VALUES_EQUAL(std::get<IQueryClassifier::TPendingCompilation>(pre).ResumeRank, 0);

        auto post = tc.RunPostClassifyForPath("/Root/testdb/my_table", /*isStreamingQuery=*/true);
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(post), "pool_target");
    }

    Y_UNIT_TEST(StreamingFallsToDefaultWithoutClassifiers) {
        auto poolSnap = MakeResourcePoolMap({
            {_JoinPath(TEST_DB, "default"), MakePoolEntry(10)},
        });
        TClassifyContext ctx{.PoolId = "", .AppName = "", .UserToken = nullptr};
        auto classifier = CreateQueryClassifier(poolSnap, TClassifierConfigsView{}, TEST_DB, std::move(ctx), std::nullopt);

        NKqp::TUserRequestContext userRequestContext{};
        userRequestContext.IsStreamingQuery = true;
        auto result = classifier->PreCompileClassify(userRequestContext);
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(result), "default");
    }

    Y_UNIT_TEST(StreamingHonorsExplicitPoolId) {
        auto classifierSnap = MakeClassifierSnapshot({
            MakeClassifierConfig(TEST_DB, "c1", 100, "pool_target",
                /*memberName=*/std::nullopt, /*hasAppName=*/std::nullopt,
                /*hasFullScan=*/std::nullopt, /*hasPath=*/std::nullopt,
                /*hasStream=*/true),
        });
        auto poolSnap = MakeResourcePoolMap({
            {_JoinPath(TEST_DB, "pool_target"), MakePoolEntry(10)},
            {_JoinPath(TEST_DB, "explicit_pool"), MakePoolEntry(10)},
            {_JoinPath(TEST_DB, "default"), MakePoolEntry(10)},
        });
        TClassifyContext ctx{.PoolId = "explicit_pool", .AppName = "", .UserToken = nullptr};
        auto classifier = CreateQueryClassifier(
            poolSnap, TClassifierConfigsView(classifierSnap, TEST_DB), TEST_DB, std::move(ctx), std::nullopt);

        NKqp::TUserRequestContext userRequestContext;
        userRequestContext.IsStreamingQuery = true;
        auto result = classifier->PreCompileClassify(userRequestContext);
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(result), "explicit_pool");
    }

    Y_UNIT_TEST(StreamingWaitCompileState) {
        TClassifyTestCase tc;
        tc.ClassifierHasAppName = "app";
        tc.ContextAppName = "app";

        auto classifier = tc.BuildClassifier();
        NKqp::TUserRequestContext userRequestContext;
        userRequestContext.IsStreamingQuery = true;

        UNIT_ASSERT_EQUAL(classifier->GetState(), IQueryClassifier::EState::None);
        (void)classifier->PreCompileClassify(userRequestContext);
        UNIT_ASSERT_EQUAL(classifier->GetState(), IQueryClassifier::EState::WaitCompile);

        auto proto = std::make_unique<NKikimrKqp::TPreparedQuery>();
        NKqp::TPreparedQueryHolder holder(proto.release(), nullptr, /*noFillTables=*/true);
        (void)classifier->PostCompileClassify(holder, userRequestContext);
        UNIT_ASSERT_EQUAL(classifier->GetState(), IQueryClassifier::EState::PostCompileDone);
    }
}

}  // namespace NKikimr::NWorkloadManager
