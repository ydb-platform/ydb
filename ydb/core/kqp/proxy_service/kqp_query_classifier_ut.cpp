#include <ydb/core/kqp/proxy_service/kqp_query_classifier.h>
#include <ydb/core/kqp/gateway/behaviour/resource_pool_classifier/snapshot.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/library/aclib/aclib.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

const TString TEST_DB = "/Root/testdb";

TResourcePoolClassifierConfig MakeClassifierConfig(
    const TString& database, const TString& name, i64 rank,
    const TString& resourcePool,
    std::optional<TString> memberName = std::nullopt,
    std::optional<TString> appName = std::nullopt,
    std::optional<TString> fullScanOn = std::nullopt)
{
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["resource_pool"] = resourcePool;
    if (memberName) json["member_name"] = *memberName;
    if (appName)    json["app_name"] = *appName;
    if (fullScanOn) json["full_scan_on"] = *fullScanOn;

    TResourcePoolClassifierConfig config;
    config.SetDatabase(database);
    config.SetName(name);
    config.SetRank(rank);
    config.SetConfigJson(json);
    return config;
}

std::shared_ptr<TResourcePoolClassifierSnapshot> MakeClassifierSnapshot(
    const TString& db,
    std::vector<TResourcePoolClassifierConfig> configs)
{
    auto snapshot = std::make_shared<TResourcePoolClassifierSnapshot>(TInstant::Now());
    for (auto& cfg : configs) {
        snapshot->MutableResourcePoolClassifierConfigs()[db]
            .emplace(cfg.GetName(), cfg);
        snapshot->MutableResourcePoolClassifierConfigsByRank()[db]
            .emplace(cfg.GetRank(), cfg);
    }
    return snapshot;
}

std::shared_ptr<TPoolInfoSnapshot> MakePoolInfoSnapshot(
    std::vector<std::pair<TString, TPoolInfoSnapshot::TPoolEntry>> entries)
{
    TPoolInfoSnapshot::TPoolsMap pools;
    for (auto& [key, entry] : entries) {
        pools.emplace(std::move(key), std::move(entry));
    }
    return std::make_shared<TPoolInfoSnapshot>(std::move(pools));
}

TPoolInfoSnapshot::TPoolEntry MakePoolEntry(i32 concurrentQueryLimit = -1) {
    NResourcePool::TPoolSettings settings;
    settings.ConcurrentQueryLimit = concurrentQueryLimit;
    return {.Config = settings, .SecurityObject = std::nullopt};
}

struct TClassifyTestCase {
    TString ResourcePool = "pool_target";
    i64 Rank = 100;
    std::optional<TString> ClassifierAppName;
    std::optional<TString> ClassifierMemberName;
    std::optional<TString> FullScanOn;

    TString ContextAppName;
    TString ContextMemberName;
    TString ExplicitPoolId;

    std::vector<std::pair<TString, i32>> ExtraPools;

    struct TExtraClassifier {
        TString Name;
        i64 Rank;
        TString ResourcePool;
        std::optional<TString> MemberName;
        std::optional<TString> AppName;
        std::optional<TString> FullScanOn;
    };
    std::vector<TExtraClassifier> ExtraClassifiers;

    TWmQueryClassifier BuildClassifier() const {
        std::vector<TResourcePoolClassifierConfig> configs;
        configs.push_back(MakeClassifierConfig(
            TEST_DB, "c_main", Rank, ResourcePool,
            ClassifierMemberName, ClassifierAppName, FullScanOn));

        for (const auto& extra : ExtraClassifiers) {
            configs.push_back(MakeClassifierConfig(
                TEST_DB, extra.Name, extra.Rank, extra.ResourcePool,
                extra.MemberName, extra.AppName, extra.FullScanOn));
        }

        auto classifierSnap = MakeClassifierSnapshot(TEST_DB, std::move(configs));

        std::vector<std::pair<TString, TPoolInfoSnapshot::TPoolEntry>> poolEntries = {
            {TEST_DB + "/" + ResourcePool, MakePoolEntry(10)},
            {TEST_DB + "/default", MakePoolEntry(10)},
        };
        for (const auto& extra : ExtraClassifiers) {
            poolEntries.push_back({TEST_DB + "/" + extra.ResourcePool, MakePoolEntry(10)});
        }
        for (const auto& [name, limit] : ExtraPools) {
            poolEntries.push_back({TEST_DB + "/" + name, MakePoolEntry(limit)});
        }
        auto poolSnap = MakePoolInfoSnapshot(std::move(poolEntries));

        auto token = ContextMemberName.empty()
            ? nullptr
            : MakeIntrusive<NACLib::TUserToken>(
                NACLib::TSID(ContextMemberName), TVector<NACLib::TSID>{});

        TClassifyContext ctx{
            .PoolId = ExplicitPoolId,
            .DatabaseId = TEST_DB,
            .AppName = ContextAppName,
            .UserToken = token,
        };

        return TWmQueryClassifier(poolSnap, classifierSnap, std::move(ctx));
    }

    IWmQueryClassifier::TPreClassifyResult RunPreClassify() const {
        auto classifier = BuildClassifier();
        classifier.PreCompileClassify();
        return classifier.GetPreClassifyResult();
    }

    IWmQueryClassifier::TPostClassifyResult RunPostClassify(
        const TString& queryTablePath, bool isFullScan) const
    {
        auto classifier = BuildClassifier();
        classifier.PreCompileClassify();

        auto* proto = new NKikimrKqp::TPreparedQuery();
        auto* phyQuery = proto->MutablePhysicalQuery();
        auto* tx = phyQuery->AddTransactions();
        auto* stage = tx->AddStages();
        auto* op = stage->AddTableOps();
        op->MutableTable()->SetPath(queryTablePath);

        if (isFullScan) {
            op->MutableReadRange()->MutableKeyRange();
        } else {
            auto* range = op->MutableReadRange()->MutableKeyRange();
            range->MutableFrom()->AddValues();
        }

        TPreparedQueryHolder holder(proto, nullptr, /*noFillTables=*/true);
        return classifier.PostCompileClassify(holder);
    }
};

TString GetPoolId(const IWmQueryClassifier::TPreClassifyResult& result) {
    UNIT_ASSERT_C(std::holds_alternative<IWmQueryClassifier::TResolvedPoolId>(result),
        "Expected TResolvedPoolId but got different variant");
    return std::get<IWmQueryClassifier::TResolvedPoolId>(result).PoolId;
}

TString GetPoolId(const IWmQueryClassifier::TPostClassifyResult& result) {
    UNIT_ASSERT_C(std::holds_alternative<IWmQueryClassifier::TResolvedPoolId>(result),
        "Expected TResolvedPoolId but got different variant");
    return std::get<IWmQueryClassifier::TResolvedPoolId>(result).PoolId;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TWmQueryClassifierMemberName) {

    Y_UNIT_TEST(ShouldMatchUserSID) {
        TClassifyTestCase tc;
        tc.ClassifierMemberName = "user@domain";
        tc.ContextMemberName = "user@domain";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "pool_target");
    }

    Y_UNIT_TEST(ShouldMatchGroupSID) {
        auto classifierSnap = MakeClassifierSnapshot(TEST_DB, {
            MakeClassifierConfig(TEST_DB, "c1", 100, "pool_target", "admins"),
        });

        auto poolSnap = MakePoolInfoSnapshot({
            {TEST_DB + "/pool_target", MakePoolEntry(10)},
        });

        auto token = MakeIntrusive<NACLib::TUserToken>(
            NACLib::TSID("alice"), TVector<NACLib::TSID>{"admins", "devs"});

        TClassifyContext ctx{
            .PoolId = "",
            .DatabaseId = TEST_DB,
            .AppName = "",
            .UserToken = token,
        };

        TWmQueryClassifier classifier(poolSnap, classifierSnap, ctx);
        classifier.PreCompileClassify();
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(classifier.GetPreClassifyResult()), "pool_target");
    }

    Y_UNIT_TEST(ShouldFallToDefaultWhenNoMatch) {
        TClassifyTestCase tc;
        tc.ClassifierMemberName = "bob";
        tc.ContextMemberName = "alice";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "default");
    }

    Y_UNIT_TEST(ShouldMatchAnonymousUserWithEmptySID) {
        auto classifierSnap = MakeClassifierSnapshot(TEST_DB, {
            MakeClassifierConfig(TEST_DB, "c1", 100, "pool_target",
                TString(NACLib::TSID())),
        });

        auto poolSnap = MakePoolInfoSnapshot({
            {TEST_DB + "/pool_target", MakePoolEntry(10)},
        });

        TClassifyContext ctx{
            .PoolId = "",
            .DatabaseId = TEST_DB,
            .AppName = "",
            .UserToken = nullptr,
        };

        TWmQueryClassifier classifier(poolSnap, classifierSnap, ctx);
        classifier.PreCompileClassify();
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(classifier.GetPreClassifyResult()), "pool_target");
    }
}

Y_UNIT_TEST_SUITE(TWmQueryClassifierAppName) {

    Y_UNIT_TEST(ShouldMatchAppName) {
        TClassifyTestCase tc;
        tc.ClassifierAppName = "my_app";
        tc.ContextAppName = "my_app";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "pool_target");
    }

    Y_UNIT_TEST(ShouldNotMatchDifferentAppName) {
        TClassifyTestCase tc;
        tc.ClassifierAppName = "expected_app";
        tc.ContextAppName = "other_app";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "default");
    }

    Y_UNIT_TEST(ShouldMatchAnyAppWhenFilterNotSet) {
        TClassifyTestCase tc;
        tc.ContextAppName = "some_random_app";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "pool_target");
    }

    Y_UNIT_TEST(ShouldMatchCombinedAppNameAndMemberName) {
        TClassifyTestCase tc;
        tc.ClassifierAppName = "my_app";
        tc.ClassifierMemberName = "alice";
        tc.ContextAppName = "my_app";
        tc.ContextMemberName = "alice";
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(tc.RunPreClassify()), "pool_target");
    }

}

Y_UNIT_TEST_SUITE(TWmQueryClassifierFullScan) {

    Y_UNIT_TEST(ShouldTriggerPendingCompilation) {
        TClassifyTestCase tc;
        tc.FullScanOn = "/Root/testdb/my_table";
        auto result = tc.RunPreClassify();
        UNIT_ASSERT(std::holds_alternative<IWmQueryClassifier::TPendingCompilation>(result));
    }

    Y_UNIT_TEST(ShouldMatchFullScan) {
        TClassifyTestCase tc;
        tc.FullScanOn = "/Root/testdb/my_table";
        auto result = tc.RunPostClassify("/Root/testdb/my_table", /*isFullScan=*/true);
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(result), "pool_target");
    }

    Y_UNIT_TEST(ShouldNotMatchPartialScan) {
        TClassifyTestCase tc;
        tc.FullScanOn = "/Root/testdb/my_table";
        auto result = tc.RunPostClassify("/Root/testdb/my_table", /*isFullScan=*/false);
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(result), "default");
    }

    Y_UNIT_TEST(ShouldNotMatchFullScanOnDifferentTable) {
        TClassifyTestCase tc;
        tc.FullScanOn = "/Root/testdb/my_table";
        auto result = tc.RunPostClassify("/Root/testdb/other_table", /*isFullScan=*/true);
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(result), "default");
    }

    Y_UNIT_TEST(ShouldMatchFullScanWithAppNameAndMemberNameFilter) {
        TClassifyTestCase tc;
        tc.ClassifierAppName = "my_app";
        tc.ClassifierMemberName = "alice";
        tc.FullScanOn = "/Root/testdb/my_table";
        tc.ContextAppName = "my_app";
        tc.ContextMemberName = "alice";

        auto pre = tc.RunPreClassify();
        UNIT_ASSERT(std::holds_alternative<IWmQueryClassifier::TPendingCompilation>(pre));

        auto post = tc.RunPostClassify("/Root/testdb/my_table", /*isFullScan=*/true);
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(post), "pool_target");
    }
}

Y_UNIT_TEST_SUITE(TWmQueryClassifierRankPriority) {

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

} // namespace NKikimr::NKqp

