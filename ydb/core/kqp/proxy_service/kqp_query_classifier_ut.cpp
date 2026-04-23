#include <ydb/core/base/path.h>
#include <ydb/core/kqp/gateway/behaviour/resource_pool_classifier/snapshot.h>
#include <ydb/core/kqp/proxy_service/kqp_query_classifier.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/library/aclib/aclib.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

namespace {

constexpr char TEST_DB[] = "/Root/testdb";

TResourcePoolClassifierConfig MakeClassifierConfig(
    const TString& database, const TString& name, i64 rank,
    const TString& resourcePool,
    std::optional<TString> memberName = std::nullopt)
{
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["resource_pool"] = resourcePool;
    if (memberName) {
        json["member_name"] = *memberName;
    }

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
    for (const auto& cfg : configs) {
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
    return {.Config = settings};
}

template <typename... TArgs>
TString _JoinPath(TArgs&&... args) {
    TVector<TString> path;
    path.reserve(sizeof...(args));
    (path.push_back(TString(std::forward<TArgs>(args))), ...);
    return JoinPath(path);
}

struct TClassifyTestCase {
    TString ResourcePool = "pool_target";
    i64 Rank = 100;
    std::optional<TString> ClassifierMemberName;

    TString ContextAppName;
    TString ContextMemberName;
    TString ExplicitPoolId;

    std::vector<std::pair<TString, i32>> ExtraPools;

    struct TExtraClassifier {
        TString Name;
        i64 Rank = 0;
        TString ResourcePool;
        std::optional<TString> MemberName;
    };
    std::vector<TExtraClassifier> ExtraClassifiers;

    std::shared_ptr<IWmQueryClassifier> BuildClassifier() const {
        std::vector<TResourcePoolClassifierConfig> configs;
        configs.push_back(MakeClassifierConfig(
            TEST_DB, "c_main", Rank, ResourcePool,
            ClassifierMemberName));

        for (const auto& extra : ExtraClassifiers) {
            configs.push_back(MakeClassifierConfig(
                TEST_DB, extra.Name, extra.Rank, extra.ResourcePool,
                extra.MemberName));
        }

        auto classifierSnap = MakeClassifierSnapshot(TEST_DB, std::move(configs));

        std::vector<std::pair<TString, TPoolInfoSnapshot::TPoolEntry>> poolEntries = {
            {_JoinPath(TEST_DB, ResourcePool), MakePoolEntry(10)},
            {_JoinPath(TEST_DB, "default"), MakePoolEntry(10)},
        };
        for (const auto& extra : ExtraClassifiers) {
            poolEntries.push_back({_JoinPath(TEST_DB, extra.ResourcePool), MakePoolEntry(10)});
        }
        for (const auto& [name, limit] : ExtraPools) {
            poolEntries.push_back({_JoinPath(TEST_DB, name), MakePoolEntry(limit)});
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

        return CreateWmQueryClassifier(poolSnap, classifierSnap, std::move(ctx));
    }

    IWmQueryClassifier::TPreClassifyResult RunPreClassify() const {
        auto classifier = BuildClassifier();
        return classifier->PreCompileClassify();
    }
};

TString GetPoolId(const IWmQueryClassifier::TPreClassifyResult& result) {
    UNIT_ASSERT_C(std::holds_alternative<IWmQueryClassifier::TResolvedPoolId>(result),
        TStringBuilder()
            << "Expected TResolvedPoolId, with index: " << ToString(std::variant_size_v<IWmQueryClassifier::TPreClassifyResult>)
            << ", but got variant with index: " << result.index()
    );
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
            {_JoinPath(TEST_DB, "pool_target"), MakePoolEntry(10)},
        });

        auto token = MakeIntrusive<NACLib::TUserToken>(
            NACLib::TSID("alice"), TVector<NACLib::TSID>{"admins", "devs"});

        TClassifyContext ctx{
            .PoolId = "",
            .DatabaseId = TEST_DB,
            .AppName = "",
            .UserToken = token,
        };

        auto classifier = CreateWmQueryClassifier(poolSnap, classifierSnap, ctx);
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
        auto classifierSnap = MakeClassifierSnapshot(TEST_DB, {
            MakeClassifierConfig(TEST_DB, "c1", 100, "pool_target",
                TString(NACLib::TSID())),
        });

        auto poolSnap = MakePoolInfoSnapshot({
            {_JoinPath(TEST_DB, "pool_target"), MakePoolEntry(10)},
        });

        TClassifyContext ctx{
            .PoolId = "",
            .DatabaseId = TEST_DB,
            .AppName = "",
            .UserToken = nullptr,
        };

        auto classifier = CreateWmQueryClassifier(poolSnap, classifierSnap, ctx);
        auto result = classifier->PreCompileClassify();
        UNIT_ASSERT_VALUES_EQUAL(GetPoolId(result), "pool_target");
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
