#include "kqp_query_classifier.h"
#include <ydb/core/kqp/workload_service/kqp_workload_service.h>

namespace NKikimr::NKqp {
namespace {

bool MatchesMemberName(const TString& target, const TClassifyContext& ctx) {
    // Check anonymous user
    if (!ctx.UserToken) {
        return target == NACLib::TSID();
    }

    if (auto it = ctx.MemberNameCache.find(target); it != ctx.MemberNameCache.end()) {
        return it->second;
    }

    bool found = false;

    // Check UserSID only for non-system users.
    if (!ctx.UserToken->IsSystemUser()) {
        found = target == ctx.UserToken->GetUserSID();
    }

    // Check GroupSID for all users
    if (!found) {
        for (const auto& groupSID : ctx.UserToken->GetGroupSIDs()) {
            if (target == groupSID) {
                found = true;
                break;
            }
        }
    }

    return ctx.MemberNameCache[target] = found;
}

///
/// Performs query classification using static query parameters. Static parameters are:
/// - Known before query compilation/execution.
/// - Independent of SQL analysis, plan building, or computations.
/// - Provided as session/connection metadata alongside the query.
///
bool MatchesStatic(const NResourcePool::TClassifierSettings& s, const TClassifyContext& ctx) {
    if (s.MemberName && !MatchesMemberName(*s.MemberName, ctx)) {
        return false;
    }

    return true;
}

bool NeedsPreparedQuery(const NResourcePool::TClassifierSettings&) {
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
bool MatchesDynamic(const NResourcePool::TClassifierSettings&, const TPreparedQueryHolder&) {
    return true;
}

} // anonymous namespace

class TWmQueryClassifier : public IWmQueryClassifier {
public:
    using TClassifierSnapshotPtr = std::shared_ptr<const TResourcePoolClassifierSnapshot>;
    using TPoolInfoSnapshotPtr = std::shared_ptr<const TPoolInfoSnapshot>;

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

    ~TWmQueryClassifier() = default;

public:
    [[nodiscard]]
    TPreClassifyResult PreCompileClassify() override {
        if (PreClassifyResult) {
            return *PreClassifyResult;
        }

        // User requested an explicit pool
        if (Context.PoolId) {
            TryResolve(Context.PoolId, PreClassifyResult);
            return *PreClassifyResult;
        }

        // If no classification use default pool
        if (!Configs) {
            TryResolve(DEFAULT_POOL_ID, PreClassifyResult);
            return *PreClassifyResult;
        }

        for (const auto& [rank, value] : *Configs) {
            const NResourcePool::TClassifierSettings& settings = value.GetClassifierSettings();

            if (!MatchesStatic(settings, Context)) {
                continue;
            }

            if (NeedsPreparedQuery(settings)) {
                return *PreClassifyResult = TPendingCompilation{.ResumeRank = rank};
            }

            if (TryResolve(settings.ResourcePool, PreClassifyResult)) {
                return *PreClassifyResult;
            }
        }

        // No suitable classification use default pool
        TryResolve(DEFAULT_POOL_ID, PreClassifyResult);
        return *PreClassifyResult;
    }

    bool NeedsPostCompileClassify() const override {
        Y_VALIDATE(PreClassifyResult.has_value(), "Pre compile classification does not called");
        return std::holds_alternative<TPendingCompilation>(*PreClassifyResult);
    }

    [[nodiscard]]
    TPostClassifyResult PostCompileClassify(const TPreparedQueryHolder& preparedQuery) override {
        Y_VALIDATE(Configs, "Post compile classify without configuration");
        Y_VALIDATE(PreClassifyResult.has_value() && std::holds_alternative<TPendingCompilation>(*PreClassifyResult),
               "Post compile classify requires TPendingCompilation from pre-classification");

        const auto& pending = std::get<TPendingCompilation>(*PreClassifyResult);
        TPostClassifyResult result;

        for (auto it = Configs->lower_bound(pending.ResumeRank); it != Configs->end(); ++it) {
            const auto& settings = it->second.GetClassifierSettings();

            if (!MatchesStatic(settings, Context)) {
                continue;
            }

            if (!MatchesDynamic(settings, preparedQuery)){
                continue;
            }

            if (TryResolve(settings.ResourcePool, result)){
                return result;
            }
        }

        // No suitable classification use default pool
        TryResolve(DEFAULT_POOL_ID, result);
        return result;
    }

private:
    const TPoolInfoSnapshot::TPoolEntry* FindPool(const TString& poolId) const {
        if (!PoolInfoSnapshot) {
            return nullptr;
        }

        return PoolInfoSnapshot->FindPool(Context.DatabaseId, poolId);
    }

    template<typename TStore>
    bool TryResolve(const TString& poolId, TStore& store) const {
        if (poolId == REJECT_POOL_ID) {
            store = TReject{
                .Code = Ydb::StatusIds::ABORTED,
                .Message = "Query is rejected by classifier"
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
                .Message = TStringBuilder() << "No access permissions for resource pool " << poolId
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
    const std::map<i64, TResourcePoolClassifierConfig>* Configs;
    std::optional<TPreClassifyResult> PreClassifyResult;
};

class TFixedWmQueryClassifier : public IWmQueryClassifier {
public:
    TFixedWmQueryClassifier(TPreClassifyResult preResult, TPostClassifyResult postResult)
        : PreResult(preResult)
        , PostResult(postResult)
    {}

    ~TFixedWmQueryClassifier() = default;

public:
    TPreClassifyResult PreCompileClassify() override {
        return PreResult;
    }

    TPostClassifyResult PostCompileClassify(const TPreparedQueryHolder&) override {
        return PostResult;
    }

    bool NeedsPostCompileClassify() const override {
        return std::holds_alternative<TPendingCompilation>(PreResult);
    }

private:
    TPreClassifyResult PreResult;
    TPostClassifyResult PostResult;
};

std::shared_ptr<IWmQueryClassifier> CreateWmQueryClassifier(TPoolInfoSnapshotPtr poolInfoSnapshot,
                                                            TClassifierSnapshotPtr classifierSnapshot,
                                                            TClassifyContext context) {
    return std::make_unique<TWmQueryClassifier>(poolInfoSnapshot, classifierSnapshot, context);
}

std::shared_ptr<IWmQueryClassifier> CreateWmBypassClassifier() {
    return std::make_unique<TFixedWmQueryClassifier>(IWmQueryClassifier::TBypass{}, IWmQueryClassifier::TBypass{});
}

} // namespace NKikimr::NKqp
