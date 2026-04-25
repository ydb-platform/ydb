#include "kqp_query_classifier.h"
#include <ydb/core/kqp/workload_service/kqp_workload_service.h>

namespace NKikimr::NKqp {

class TWmQueryClassifier : public IWmQueryClassifier {
public:
    TWmQueryClassifier(TPoolInfoSnapshotPtr poolInfoSnapshot,
                       TClassifierSnapshotPtr classifierSnapshot,
                       TClassifyContext context)
        : PoolInfoSnapshot(std::move(poolInfoSnapshot))
        , ClassifierSnapshot(std::move(classifierSnapshot))
        , Context(std::move(context))
        , Configs(nullptr)
    {
        if (!ClassifierSnapshot) {
            return;
        }

        const auto& dbConfigs = ClassifierSnapshot->GetResourcePoolClassifierConfigsByRank();
        auto it = dbConfigs.find(Context.DatabaseId);

        if (it != dbConfigs.end()) {
            Configs = &it->second;
        }
    }

    TWmQueryClassifier(const TWmQueryClassifier&) = delete;
    TWmQueryClassifier& operator=(const TWmQueryClassifier&) = delete;

    [[nodiscard]]
    TPostClassifyResult PostCompileClassify(const TPreparedQueryHolder& preparedQuery) override {
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

            if (TryResolve(settings.ResourcePool, PostClassifyResult)) {
                return *PostClassifyResult;
            }
        }

        // No suitable classification, use default pool
        Resolve(NResourcePool::DEFAULT_POOL_ID, PostClassifyResult);
        return *PostClassifyResult;
    }

    [[nodiscard]]
    TPreClassifyResult PreCompileClassify() override {
        // User requested an explicit pool
        if (Context.PoolId) {
            Resolve(Context.PoolId, PreClassifyResult);
            return *PreClassifyResult;
        }

        // If no classification, use default pool
        if (!Configs) {
            Resolve(NResourcePool::DEFAULT_POOL_ID, PreClassifyResult);
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

            if (TryResolve(settings.ResourcePool, PreClassifyResult)) {
                return *PreClassifyResult;
            }
        }

        // No suitable classification, use default pool
        Resolve(NResourcePool::DEFAULT_POOL_ID, PreClassifyResult);
        return *PreClassifyResult;
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

    const TPoolInfoSnapshot::TPoolEntry* FindPool(const TString& poolId) const {
        if (!PoolInfoSnapshot) {
            return nullptr;
        }

        return PoolInfoSnapshot->FindPool(Context.DatabaseId, poolId);
    }

    template<typename TStore>
    void Resolve(const TString& poolId, TStore& store) {
        TryResolve(poolId, store);
    }

    ///
    /// Resolves pool by id. Always populates `store`.
    /// Returns true if the resolved result is final (stop searching).
    /// Returns false if the caller should try the next rule.
    ///
    template<typename TStore>
    bool TryResolve(const TString& poolId, TStore& store) {
        if (poolId == NResourcePool::REJECT_POOL_ID) {
            store = TReject{
                .Code = Ydb::StatusIds::ABORTED,
                .Message = TStringBuilder() << "Query is rejected by classifier"
            };
            return true;
        }

        auto poolInfo = FindPool(poolId);

        if (!poolInfo) {
            store = TResolvedPoolId{.PoolId = poolId};
            return false;
        }

        if (!poolInfo->UserHasAccess(Context)) {
            store = TReject{
                .Code = Ydb::StatusIds::UNAUTHORIZED,
                .Message = TStringBuilder()
                    << "No access permissions for resource pool: " << poolId
            };
            return false;
        }

        if (!NWorkload::IsWorkloadServiceRequired(poolInfo->Config)) {
            store = TBypass{};
        } else {
            store = TResolvedPoolId{.PoolId = poolId};
        }

        return true;
    }

private:
    const TPoolInfoSnapshotPtr PoolInfoSnapshot;
    const TClassifierSnapshotPtr ClassifierSnapshot;
    const TClassifyContext Context;
    // Points into ClassifierSnapshot's data; valid as long as ClassifierSnapshot is alive
    const std::map<i64, TResourcePoolClassifierConfig>* Configs;
    std::optional<TPreClassifyResult> PreClassifyResult;
    std::optional<TPostClassifyResult> PostClassifyResult;
    mutable std::unordered_map<TString, bool> MemberNameCache;
};

std::shared_ptr<IWmQueryClassifier> CreateWmQueryClassifier(TPoolInfoSnapshotPtr poolInfoSnapshot,
                                                            TClassifierSnapshotPtr classifierSnapshot,
                                                            TClassifyContext context) {
    return std::make_shared<TWmQueryClassifier>(std::move(poolInfoSnapshot), std::move(classifierSnapshot), std::move(context));
}

} // namespace NKikimr::NKqp
