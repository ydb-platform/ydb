#include <ydb/services/workload_manager/ut/common/query_classifier_ut_common.h>

namespace NKikimr::NWorkloadManager {

Y_UNIT_TEST_SUITE(TQueryClassifierMemberName) {

    Y_UNIT_TEST(ShouldMatchUserSID) {
        TClassifyTestCase tc;
        tc.ClassifierMemberName = "user@domain";
        tc.ContextMemberName = "user@domain";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "pool_target");
    }

    Y_UNIT_TEST(ShouldMatchGroupSID) {
        auto classifierSnap = MakeClassifierSnapshot({
            MakeClassifierConfig(TEST_DB, "c1", 100, "pool_target", "admins"),
        });

        auto poolSnap = MakeResourcePoolMap({
            {_JoinPath(TEST_DB, "pool_target"), MakePoolEntry(10)},
        });

        auto token = MakeIntrusive<NACLib::TUserToken>(
            NACLib::TSID("alice"), TVector<NACLib::TSID>{"admins", "devs"});

        TClassifyContext ctx{
            .PoolId = "",
            .AppName = "",
            .UserToken = token,
        };

        auto view = TClassifierConfigsView(classifierSnap, TEST_DB);
        auto classifier = NWorkloadManager::CreateQueryClassifier(poolSnap, view, TEST_DB, ctx);
        auto result = classifier->PreCompileClassify();
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(result), "pool_target");
    }

    Y_UNIT_TEST(ShouldFallToDefaultWhenNoMatch) {
        TClassifyTestCase tc;
        tc.ClassifierMemberName = "bob";
        tc.ContextMemberName = "alice";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "default");
    }

    Y_UNIT_TEST(ShouldMatchAnonymousUserWithEmptySID) {
        auto classifierSnap = MakeClassifierSnapshot({
            MakeClassifierConfig(TEST_DB, "c1", 100, "pool_target",
                TString(NACLib::TSID())),
        });

        auto poolSnap = MakeResourcePoolMap({
            {_JoinPath(TEST_DB, "pool_target"), MakePoolEntry(10)},
        });

        TClassifyContext ctx{
            .PoolId = "",
            .AppName = "",
            .UserToken = nullptr,
        };

        auto view = TClassifierConfigsView(classifierSnap, TEST_DB);
        auto classifier = NWorkloadManager::CreateQueryClassifier(poolSnap, view, TEST_DB, ctx);
        auto result = classifier->PreCompileClassify();
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(result), "pool_target");
    }
}

} // namespace NKikimr::NWorkloadManager
