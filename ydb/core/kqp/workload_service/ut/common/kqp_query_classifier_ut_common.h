#pragma once

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/gateway/behaviour/resource_pool_classifier/snapshot.h>
#include <ydb/core/kqp/workload_service/kqp_query_classifier.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/library/aclib/aclib.h>

#include <library/cpp/testing/unittest/registar.h>

#include <optional>
#include <utility>
#include <vector>


namespace NKikimr::NKqp {

inline constexpr char TEST_DB[] = "/Root/testdb";

inline TResourcePoolClassifierConfig MakeClassifierConfig(
    const TString& database, const TString& name, i64 rank,
    const TString& resourcePool,
    std::optional<TString> memberName = std::nullopt,
    std::optional<TString> hasAppName = std::nullopt,
    std::optional<TString> action = std::nullopt)
{
    NJson::TJsonValue json(NJson::JSON_MAP);
    json["resource_pool"] = resourcePool;
    if (memberName) {
        json["member_name"] = *memberName;
    }
    if (hasAppName) {
        json["has_app_name"] = *hasAppName;
    }
    if (action) {
        json["action"] = *action;
    }

    TResourcePoolClassifierConfig config;
    config.SetDatabase(database);
    config.SetName(name);
    config.SetRank(rank);
    config.SetConfigJson(json);
    return config;
}

inline std::shared_ptr<TResourcePoolClassifierSnapshot> MakeClassifierSnapshot(
    std::vector<TResourcePoolClassifierConfig> configs)
{
    auto snapshot = std::make_shared<TResourcePoolClassifierSnapshot>(TInstant::Now());
    for (auto& cfg : configs) {
        snapshot->AddConfig(std::move(cfg));
    }
    return snapshot;
}

inline std::shared_ptr<TResourcePoolMap> MakeResourcePoolMap(
    std::vector<std::pair<TString, TResourcePoolEntry>> entries)
{
    auto pools = std::make_shared<TResourcePoolMap>();
    for (auto& [key, entry] : entries) {
        pools->emplace(std::move(key), std::move(entry));
    }
    return pools;
}

inline TResourcePoolEntry MakePoolEntry(i32 concurrentQueryLimit = -1) {
    NResourcePool::TPoolSettings settings;
    settings.ConcurrentQueryLimit = concurrentQueryLimit;
    return {.Config = settings};
}

template <typename... TArgs>
inline TString _JoinPath(TArgs&&... args) {
    TVector<TString> path;
    path.reserve(sizeof...(args));
    (path.push_back(TString(std::forward<TArgs>(args))), ...);
    return JoinPath(path);
}

struct TClassifyTestCase {
    TString ResourcePool = "pool_target";
    i64 Rank = 100;
    std::optional<TString> ClassifierMemberName;
    std::optional<TString> ClassifierHasAppName;
    std::optional<TString> ClassifierAction;

    TString ContextAppName;
    TString ContextMemberName;
    TString ExplicitPoolId;

    std::vector<std::pair<TString, i32>> ExtraPools;

    struct TExtraClassifier {
        TString Name;
        i64 Rank = 0;
        TString ResourcePool;
        std::optional<TString> MemberName;
        std::optional<TString> HasAppName;
        std::optional<TString> Action;
    };
    std::vector<TExtraClassifier> ExtraClassifiers;

    std::shared_ptr<NWorkload::IQueryClassifier> BuildClassifier() const {
        std::vector<TResourcePoolClassifierConfig> configs;
        configs.push_back(MakeClassifierConfig(
            TEST_DB, "c_main", Rank, ResourcePool,
            ClassifierMemberName, ClassifierHasAppName, ClassifierAction));

        for (const auto& extra : ExtraClassifiers) {
            configs.push_back(MakeClassifierConfig(
                TEST_DB, extra.Name, extra.Rank, extra.ResourcePool,
                extra.MemberName, extra.HasAppName, extra.Action));
        }

        auto classifierSnap = MakeClassifierSnapshot(std::move(configs));

        std::vector<std::pair<TString, TResourcePoolEntry>> poolEntries = {
            {_JoinPath(TEST_DB, ResourcePool), MakePoolEntry(10)},
            {_JoinPath(TEST_DB, "default"), MakePoolEntry(10)},
        };
        for (const auto& extra : ExtraClassifiers) {
            poolEntries.push_back({_JoinPath(TEST_DB, extra.ResourcePool), MakePoolEntry(10)});
        }
        for (const auto& [name, limit] : ExtraPools) {
            poolEntries.push_back({_JoinPath(TEST_DB, name), MakePoolEntry(limit)});
        }
        auto poolSnap = MakeResourcePoolMap(std::move(poolEntries));

        auto token = ContextMemberName.empty()
            ? nullptr
            : MakeIntrusive<NACLib::TUserToken>(
                NACLib::TSID(ContextMemberName), TVector<NACLib::TSID>{});

        TClassifyContext ctx{
            .PoolId = ExplicitPoolId,
            .AppName = ContextAppName,
            .UserToken = token,
        };

        return NWorkload::CreateQueryClassifier(poolSnap, TClassifierConfigsView(classifierSnap, TEST_DB), TEST_DB, std::move(ctx));
    }

    NWorkload::IQueryClassifier::TPreCompileClassifyResult RunPreClassify() const {
        auto classifier = BuildClassifier();
        return classifier->PreCompileClassify();
    }
};

inline TString GetPoolId(const NWorkload::IQueryClassifier::TPreCompileClassifyResult& result) {
    UNIT_ASSERT_C(std::holds_alternative<NWorkload::IQueryClassifier::TResolvedPoolId>(result),
        TStringBuilder() << "Expected TResolvedPoolId, got variant with index: " << result.index()
    );
    return std::get<NWorkload::IQueryClassifier::TResolvedPoolId>(result).PoolId;
}

}  // namespace NKikimr::NKqp
