#include "kqp_query_classifier.h"
#include "kqp_proxy_service_impl.h"

#include <ydb/core/kqp/workload_service/kqp_workload_service.h>

namespace NKikimr::NKqp {

inline constexpr char RESOLVER_IS_USER[] = "User request";
inline constexpr char DEFAULT_RESOLVER[] = "Default";

class TWmQueryClassifier : public IWmQueryClassifier {
public:
    TWmQueryClassifier(TResourcePoolMapPtr resourcePoolMap,
                       TClassifierSnapshotPtr classifierSnapshot,
                       TString databaseId,
                       TClassifyContext context)
        : ResourcePoolMap(std::move(resourcePoolMap))
        , ClassifierSnapshot(std::move(classifierSnapshot))
        , DatabaseId(std::move(databaseId))
        , Context(std::move(context))
        , Configs(nullptr)
    {
        if (!ClassifierSnapshot) {
            return;
        }

        const auto& dbConfigs = ClassifierSnapshot->GetResourcePoolClassifierConfigsByRank();
        auto it = dbConfigs.find(DatabaseId);

        if (it != dbConfigs.end()) {
            Configs = &it->second;
        }
    }

    TWmQueryClassifier(const TWmQueryClassifier&) = delete;
    TWmQueryClassifier& operator=(const TWmQueryClassifier&) = delete;

    [[nodiscard]]
    TPreCompileClassifyResult PreCompileClassify() override {
        // User requested an explicit pool
        if (Context.PoolId) {
            TryResolve(Context.PoolId, PreClassifyResult, RESOLVER_IS_USER);
            return *PreClassifyResult;
        }

        // If no classification, use default pool
        if (!Configs) {
            TryResolve(NResourcePool::DEFAULT_POOL_ID, PreClassifyResult, DEFAULT_RESOLVER);
            return *PreClassifyResult;
        }

        for (const auto& [rank, value] : *Configs) {
            const NResourcePool::TClassifierSettings& settings = value.GetClassifierSettings();

            if (!MatchesStatic(settings)) {
                continue;
            }

            if (NeedsPreparedQuery(settings)) {
                return *PreClassifyResult = TPendingCompilation{.ResumeRank = rank};
            }

            if (TryResolve(settings, PreClassifyResult)) {
                return *PreClassifyResult;
            }
        }

        // No suitable classification, use default pool
        TryResolve(NResourcePool::DEFAULT_POOL_ID, PreClassifyResult, DEFAULT_RESOLVER);
        return *PreClassifyResult;
    }

    [[nodiscard]]
    TPostCompileClassifyResult PostCompileClassify(const TPreparedQueryHolder& preparedQuery) override {
        Y_VALIDATE(Configs, "Post compile classify without configuration");
        Y_VALIDATE(PreClassifyResult.has_value() && std::holds_alternative<TPendingCompilation>(*PreClassifyResult),
               "Post compile classify requires TPendingCompilation from pre-classification");

        const auto& pending = std::get<TPendingCompilation>(*PreClassifyResult);

        for (auto it = Configs->lower_bound(pending.ResumeRank); it != Configs->end(); ++it) {
            const auto& settings = it->second.GetClassifierSettings();

            if (!MatchesStatic(settings)) {
                continue;
            }

            if (!MatchesDynamic(settings, preparedQuery)) {
                continue;
            }

            if (TryResolve(settings, PostClassifyResult)) {
                return *PostClassifyResult;
            }
        }

        // No suitable classification, use default pool
        TryResolve(NResourcePool::DEFAULT_POOL_ID, PostClassifyResult, DEFAULT_RESOLVER);
        return *PostClassifyResult;
    }

    EState GetState() const override {
        if (!PreClassifyResult) {
            return EState::None;
        }

        if (std::holds_alternative<TPendingCompilation>(*PreClassifyResult)) {
            return !PostClassifyResult ? EState::WaitCompile : EState::PostCompileDone;
        }

        return EState::PreCompileDone;
    }
private:
    ///
    /// Check Predicate MemberName
    ///
    bool MatchesMemberName(const TString& target) const {
        // Check anonymous user
        if (!Context.UserToken) {
            return target == NACLib::TSID();
        }

        auto [it, inserted] = MemberNameCache.emplace(target, false);

        if (!inserted) {
            return it->second;
        }

        bool found = false;

        // Check UserSID only for non-system users.
        if (!Context.UserToken->IsSystemUser()) {
            found = target == Context.UserToken->GetUserSID();
        }

        // Check GroupSID for all users
        if (!found) {
            for (const auto& groupSID : Context.UserToken->GetGroupSIDs()) {
                if (target == groupSID) {
                    found = true;
                    break;
                }
            }
        }

        return it->second = found;
    }

    ///
    /// Performs query classification using static query parameters. Static parameters are:
    /// - Known before query compilation/execution.
    /// - Independent of SQL analysis, plan building, or computations.
    /// - Provided as session/connection metadata alongside the query.
    ///
    bool MatchesStatic(const NResourcePool::TClassifierSettings& s) const {
        if (s.MemberName && !MatchesMemberName(*s.MemberName)) {
            return false;
        }

        return true;
    }

    bool NeedsPreparedQuery(const NResourcePool::TClassifierSettings&) const {
        return false;
    }

    ///
    /// Performs query classification based on dynamic query parameters — data that:
    /// - Requires query compilation/execution to be determined.
    /// - Involves SQL analysis, plan building, or computations.
    /// - Depends on actual query structure and execution characteristics.
    ///
    /// Currently returns true (no dynamic filtering applied).
    ///
    bool MatchesDynamic(const NResourcePool::TClassifierSettings&, const TPreparedQueryHolder&) const {
        return true;
    }

    const TResourcePoolEntry* FindPool(const TString& poolId) const {
        if (!ResourcePoolMap) {
            return nullptr;
        }

        auto it = ResourcePoolMap->find(TResourcePoolsCache::GetPoolKey(DatabaseId, poolId));
        return it != ResourcePoolMap->end() ? &it->second : nullptr;
    }

    bool UserHasAccess(const TResourcePoolEntry& poolEntry, ui32 access) const {
        if (!Context.UserToken || Context.UserToken->GetSerializedToken().empty()) {
            return true;
        }

        if (!poolEntry.SecurityObject) {
            return true;
        }

        return poolEntry.SecurityObject->CheckAccess(access, *Context.UserToken);
    }

    template<typename TStore>
    bool TryResolve(const NResourcePool::TClassifierSettings& classifier, TStore& store) {
        return TryResolve(classifier.ResourcePool, store, TStringBuilder() << "Classifier with rank: " << classifier.Rank);
    }

    ///
    /// Resolves pool by id. Always populates `store`.
    /// Returns true if the resolved result is final (stop searching).
    /// Returns false if the caller should try the next rule.
    ///
    template<typename TStore>
    bool TryResolve(const TString& poolId, TStore& store, const TString& resolver) {
        if (to_lower(poolId) == NResourcePool::REJECT_POOL_ID) {
            store = TReject{
                .Code = Ydb::StatusIds::ABORTED,
                .Message = TStringBuilder() << "Query is rejected, resolved by: " << resolver,
                .Resolver = resolver
            };
            return true;
        }

        auto poolInfo = FindPool(poolId);

        if (!poolInfo) {
            store = TResolvedPoolId{.PoolId = poolId, .Resolver = resolver};
            return false;
        }

        if (!UserHasAccess(*poolInfo, NACLib::DescribeSchema)) {
            store = TReject{
                .Code = Ydb::StatusIds::NOT_FOUND,
                .Message = TStringBuilder()
                    << "Resource pool: " << poolId << " not found or you don't have describe permissions"
                    << ", resolved by: " << resolver,
                .Resolver = resolver
            };
            return false;
        }

        if (!UserHasAccess(*poolInfo, NACLib::SelectRow)) {
            store = TReject{
                .Code = Ydb::StatusIds::UNAUTHORIZED,
                .Message = TStringBuilder()
                    << "No access permissions for resource pool: " << poolId
                    << ", resolved by: " << resolver,
                .Resolver = resolver
            };
            return false;
        }

        if (!NWorkload::IsWorkloadServiceRequired(poolInfo->Config)) {
            store = TBypass{.Resolver = resolver};
        } else {
            store = TResolvedPoolId{.PoolId = poolId, .Resolver = resolver};
        }

        return true;
    }

private:
    const TResourcePoolMapPtr ResourcePoolMap;
    const TClassifierSnapshotPtr ClassifierSnapshot;
    const TString DatabaseId;
    const TClassifyContext Context;
    // Points into ClassifierSnapshot's data; valid as long as ClassifierSnapshot is alive
    const std::map<i64, TResourcePoolClassifierConfig>* Configs;
    std::optional<TPreCompileClassifyResult> PreClassifyResult;
    std::optional<TPostCompileClassifyResult> PostClassifyResult;
    mutable std::unordered_map<TString, bool> MemberNameCache;
};

std::shared_ptr<IWmQueryClassifier> CreateWmQueryClassifier(TResourcePoolMapPtr resourcePoolMap,
                                                            TClassifierSnapshotPtr classifierSnapshot,
                                                            const TString& databaseId,
                                                            TClassifyContext context) {
    return std::make_shared<TWmQueryClassifier>(std::move(resourcePoolMap), std::move(classifierSnapshot), databaseId, std::move(context));
}

} // namespace NKikimr::NKqp
