#include <ydb/core/kqp/workload_service/ut/common/kqp_query_classifier_ut_common.h>

namespace NKikimr::NKqp {

namespace {

enum class EExpected {
    Bypass,
    ResolvedAdmission,      // SkipAdmission = false
    ResolvedSkipAdmission,  // SkipAdmission = true
};

void CheckDecision(std::function<void(NResourcePool::TPoolSettings&)> setup, EExpected expected) {
    NResourcePool::TPoolSettings config;
    setup(config);

    auto classifierSnap = MakeClassifierSnapshot(TEST_DB, {
        MakeClassifierConfig(TEST_DB, "c1", 100, "test_pool"),
    });
    auto poolSnap = MakeResourcePoolMap({
        {_JoinPath(TEST_DB, "test_pool"), TResourcePoolEntry{.Config = config}},
        {_JoinPath(TEST_DB, "default"), MakePoolEntry(10)},
    });

    TClassifyContext ctx{.PoolId = "", .AppName = "", .UserToken = nullptr};
    auto classifier = NWorkload::CreateQueryClassifier(
        poolSnap, TClassifierConfigsView(classifierSnap, TEST_DB), TEST_DB, std::move(ctx));

    auto result = classifier->PreCompileClassify();

    switch (expected) {
        case EExpected::Bypass:
            UNIT_ASSERT_C(std::holds_alternative<NWorkload::IQueryClassifier::TBypass>(result),
                TStringBuilder() << "Expected TBypass, got variant index " << result.index());
            break;
        case EExpected::ResolvedAdmission: {
            UNIT_ASSERT_C(std::holds_alternative<NWorkload::IQueryClassifier::TResolvedPoolId>(result),
                TStringBuilder() << "Expected TResolvedPoolId, got variant index " << result.index());
            UNIT_ASSERT_C(!std::get<NWorkload::IQueryClassifier::TResolvedPoolId>(result).SkipAdmission,
                "Expected SkipAdmission=false");
            break;
        }
        case EExpected::ResolvedSkipAdmission: {
            UNIT_ASSERT_C(std::holds_alternative<NWorkload::IQueryClassifier::TResolvedPoolId>(result),
                TStringBuilder() << "Expected TResolvedPoolId, got variant index " << result.index());
            UNIT_ASSERT_C(std::get<NWorkload::IQueryClassifier::TResolvedPoolId>(result).SkipAdmission,
                "Expected SkipAdmission=true");
            break;
        }
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TQueryClassifierRankPriority) {

    Y_UNIT_TEST(ShouldLowerRankWin) {
        TClassifyTestCase tc;
        tc.Rank = 50;
        tc.ResourcePool = "pool_low";
        tc.ExtraClassifiers.push_back({
            .Name = "c_high",
            .Rank = 200,
            .ResourcePool = "pool_high",
        });
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "pool_low");
    }

    Y_UNIT_TEST(ShouldSkipNonMatchingLowerRank) {
        TClassifyTestCase tc;
        tc.Rank = 10;
        tc.ResourcePool = "pool_bob";
        tc.ClassifierMemberName = "bob";
        tc.ContextMemberName = "alice";
        tc.ExtraClassifiers.push_back({
            .Name = "c_all",
            .Rank = 20,
            .ResourcePool = "pool_all",
        });
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "pool_all");
    }

    Y_UNIT_TEST(ShouldExplicitPoolIdOverrideClassifiers) {
        TClassifyTestCase tc;
        tc.ExplicitPoolId = "my_explicit_pool";
        tc.ExtraPools.push_back({"my_explicit_pool", 10});
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "my_explicit_pool");
    }
}

Y_UNIT_TEST_SUITE(TQueryClassifierDecision) {

    Y_UNIT_TEST(BypassOnAllDefaults) {
        CheckDecision([](auto&){}, EExpected::Bypass);
    }

    Y_UNIT_TEST(AdmissionOnConcurrentQueryLimit) {
        CheckDecision([](auto& c){ c.ConcurrentQueryLimit = 10; }, EExpected::ResolvedAdmission);
    }

    Y_UNIT_TEST(AdmissionOnDatabaseLoadCpuThreshold) {
        CheckDecision([](auto& c){ c.DatabaseLoadCpuThreshold = 0.8; }, EExpected::ResolvedAdmission);
    }

    Y_UNIT_TEST(AdmissionOnQueryCancelAfter) {
        CheckDecision([](auto& c){ c.QueryCancelAfter = TDuration::Seconds(30); }, EExpected::ResolvedAdmission);
    }

    Y_UNIT_TEST(SkipAdmissionOnTotalCpuLimit) {
        CheckDecision([](auto& c){ c.TotalCpuLimitPercentPerNode = 50; }, EExpected::ResolvedSkipAdmission);
    }

    Y_UNIT_TEST(SkipAdmissionOnTotalMemoryLimit) {
        CheckDecision([](auto& c){ c.TotalMemoryLimitPercentPerNode = 80; }, EExpected::ResolvedSkipAdmission);
    }

    Y_UNIT_TEST(SkipAdmissionOnResourceWeight) {
        CheckDecision([](auto& c){ c.ResourceWeight = 1.0; }, EExpected::ResolvedSkipAdmission);
    }

    Y_UNIT_TEST(SkipAdmissionOnQueryCpuLimit) {
        CheckDecision([](auto& c){ c.QueryCpuLimitPercentPerNode = 25; }, EExpected::ResolvedSkipAdmission);
    }

    Y_UNIT_TEST(SkipAdmissionOnQueryMemoryLimit) {
        CheckDecision([](auto& c){ c.QueryMemoryLimitPercentPerNode = 50; }, EExpected::ResolvedSkipAdmission);
    }

    Y_UNIT_TEST(AdmissionWinsWhenBothSet) {
        CheckDecision([](auto& c){
            c.ConcurrentQueryLimit = 10;
            c.TotalCpuLimitPercentPerNode = 50;
        }, EExpected::ResolvedAdmission);
    }
}

} // namespace NKikimr::NKqp
