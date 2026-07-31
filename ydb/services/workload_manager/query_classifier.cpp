#include "has_full_scan_matcher.h"
#include "has_path_matcher.h"
#include "has_shared_reading_matcher.h"
#include "has_stream_matcher.h"
#include "query_classifier.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/protos/config.pb.h>


namespace NKikimr::NWorkloadManager {

inline constexpr char RESOLVER_IS_USER[] = "User request";
inline constexpr char DEFAULT_RESOLVER[] = "Default";

class TQueryClassifier : public IQueryClassifier {
public:
    TQueryClassifier(TResourcePoolMapPtr resourcePoolMap,
                     TClassifierConfigsView classifierView,
                     TString databaseId,
                     TClassifyContext context)
        : ResourcePoolMap(std::move(resourcePoolMap))
        , ClassifierView(std::move(classifierView))
        , DatabaseId(std::move(databaseId))
        , Context(std::move(context))
        , ResourcePoolForSharedReading(
              AppData()->QueryServiceConfig.GetStreamingQueries().GetResourcePoolForSharedReading())
    {}

    TQueryClassifier(const TQueryClassifier&) = delete;
    TQueryClassifier& operator=(const TQueryClassifier&) = delete;

    [[nodiscard]]
    TPreCompileClassifyResult PreCompileClassify(const NKqp::TUserRequestContext& userRequestContext) override {
        // Shared reading can only be detected from the physical plan. Defer admission
        // so PostCompileClassify can force ResourcePoolForSharedReading when needed,
        // or reject when an explicit pool conflicts with it.
        if (NeedSharedReadingPoolCheck(userRequestContext)) {
            PreClassifyResult = TPendingCompilation{.ResumeRank = 0};
            return *PreClassifyResult;
        }

        // User requested an explicit pool
        if (Context.PoolId) {
            TryResolve(Context.PoolId, PreClassifyResult, RESOLVER_IS_USER);
            return *PreClassifyResult;
        }

        // If no classification, use default pool
        if (!ClassifierView) {
            TryResolve(NResourcePool::DEFAULT_POOL_ID, PreClassifyResult, DEFAULT_RESOLVER);
            return *PreClassifyResult;
        }

        // Streaming queries always go through post-compile so dynamic predicates
        // (HAS_STREAM / HAS_PATH / HAS_FULL_SCAN) are evaluated from ResumeRank 0.
        if (userRequestContext.IsStreamingQuery) {
            PreClassifyResult = TPendingCompilation{.ResumeRank = 0};
            return *PreClassifyResult;
        }

        for (const auto& [rank, value] : *ClassifierView) {
            const auto& settings = value.GetClassifierSettings();

            if (!MatchesStatic(settings)) {
                continue;
            }

            if (NeedsPreparedQuery(settings)) {
                PreClassifyResult = TPendingCompilation{.ResumeRank = rank};
                return *PreClassifyResult;
            }

            if (settings.Action == NResourcePool::EClassifierAction::Reject) {
                PreClassifyResult = MakeRejectFromClassifier(value);
                return *PreClassifyResult;
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
    TPostCompileClassifyResult PostCompileClassify(const NKqp::TPreparedQueryHolder& preparedQuery, const NKqp::TUserRequestContext& userRequestContext) override {
        Y_VALIDATE(PreClassifyResult.has_value() && std::holds_alternative<TPendingCompilation>(*PreClassifyResult),
               "Post compile classify requires TPendingCompilation from pre-classification");

        if (NeedSharedReadingPoolCheck(userRequestContext)) {
            if (UsesSharedReading(preparedQuery.GetPhysicalQuery())) {
                // Hard guarantee: shared-reading streaming queries must land in
                // ResourcePoolForSharedReading or be rejected — never soft-fallback.
                // An explicit pool that differs from the configured one is a conflict.
                if (Context.PoolId && Context.PoolId != ResourcePoolForSharedReading) {
                    PostClassifyResult = TReject{
                        .Code = Ydb::StatusIds::PRECONDITION_FAILED,
                        .Message = TStringBuilder()
                            << "Explicit resource pool '" << Context.PoolId
                            << "' conflicts with required pool for shared reading '"
                            << ResourcePoolForSharedReading << "'"
                            << ", resolved by: ResourcePoolForSharedReading",
                        .Resolver = "ResourcePoolForSharedReading",
                    };
                    return *PostClassifyResult;
                }
                ForceResolveSharedReadingPool();
                return *PostClassifyResult;
            }

            // No shared reading — apply the original user pool, if any.
            if (Context.PoolId) {
                TryResolve(Context.PoolId, PostClassifyResult, RESOLVER_IS_USER);
                return *PostClassifyResult;
            }
        }

        if (!ClassifierView) {
            TryResolve(NResourcePool::DEFAULT_POOL_ID, PostClassifyResult, DEFAULT_RESOLVER);
            return *PostClassifyResult;
        }

        const auto& pending = std::get<TPendingCompilation>(*PreClassifyResult);

        for (auto it = ClassifierView->lower_bound(pending.ResumeRank); it != ClassifierView->end(); ++it) {
            const auto& settings = it->second.GetClassifierSettings();

            if (!MatchesStatic(settings)) {
                continue;
            }

            if (!MatchesDynamic(settings, preparedQuery, userRequestContext)) {
                continue;
            }

            if (settings.Action == NResourcePool::EClassifierAction::Reject) {
                PostClassifyResult = MakeRejectFromClassifier(it->second);
                return *PostClassifyResult;
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
    bool MatchesMemberName(const std::optional<TString>& target) const {
        if (!target) {
            return true;
        }

        // Check anonymous user
        if (!Context.UserToken) {
            return *target == NACLib::TSID();
        }

        auto [it, inserted] = MemberNameCache.emplace(*target, false);

        if (!inserted) {
            return it->second;
        }

        bool found = false;

        // Check UserSID only for non-system users.
        if (!Context.UserToken->IsSystemUser()) {
            found = *target == Context.UserToken->GetUserSID();
        }

        // Check GroupSID for all users
        if (!found) {
            for (const auto& groupSID : Context.UserToken->GetGroupSIDs()) {
                if (*target == groupSID) {
                    found = true;
                    break;
                }
            }
        }

        return it->second = found;
    }

    ///
    /// Check Predicate HasAppName — strict string equality.
    ///
    static bool MatchesAppName(const std::optional<TString>& expected, const TString& appName) {
        return !expected || *expected == appName;
    }

    ///
    /// Performs query classification using static query parameters. Static parameters are:
    /// - Known before query compilation/execution.
    /// - Independent of SQL analysis, plan building, or computations.
    /// - Provided as session/connection metadata alongside the query.
    ///
    bool MatchesStatic(const NResourcePool::TClassifierSettings& settings) const {
        if (!MatchesAppName(settings.HasAppName, Context.AppName)) {
            return false;
        }

        if (!MatchesMemberName(settings.MemberName)) {
            return false;
        }

        return true;
    }

    bool NeedsPreparedQuery(const NResourcePool::TClassifierSettings& settings) const {
        return settings.HasFullScan.has_value() || settings.HasPath.has_value() || settings.HasStream.has_value();
    }

    ///
    /// Performs query classification based on dynamic query parameters — data that:
    /// - Requires query compilation/execution to be determined.
    /// - Involves SQL analysis, plan building, or computations.
    /// - Depends on actual query structure and execution characteristics.
    ///
    bool MatchesDynamic(const NResourcePool::TClassifierSettings& settings, const NKqp::TPreparedQueryHolder& preparedQuery, const NKqp::TUserRequestContext& userRequestContext) const {
        return MatchesFullScan(settings.HasFullScan, preparedQuery.GetPhysicalQuery())
            && MatchesPath(settings.HasPath, preparedQuery.GetQueryTables(), preparedQuery.GetPhysicalQuery())
            && MatchesStream(settings.HasStream, userRequestContext);
    }

    const TResourcePoolEntry* FindPool(const TString& poolId) const {
        if (!ResourcePoolMap) {
            return nullptr;
        }

        auto it = ResourcePoolMap->find(GetPoolKey(DatabaseId, poolId));
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
        Y_ABORT_UNLESS(classifier.ResourcePool.has_value(),
            "ResourcePool must be set for non-Reject classifiers");
        return TryResolve(*classifier.ResourcePool, store, TStringBuilder() << "Classifier with rank: " << classifier.Rank);
    }

    static TReject MakeRejectFromClassifier(const TResourcePoolClassifierConfig& config) {
        const auto& name = config.GetName();
        const auto rank = config.GetRank();
        return TReject{
            .Code = Ydb::StatusIds::PRECONDITION_FAILED,
            .Message = TStringBuilder() << "Request is rejected by classifier '" << name << "' (rank=" << rank << ")",
            .Resolver = TStringBuilder() << "Classifier with rank: " << rank,
        };
    }

    ///
    /// Resolves pool by id. Always populates `store`.
    /// Returns true if the resolved result is final (stop searching).
    /// Returns false if the caller should try the next rule.
    ///
    template<typename TStore>
    bool TryResolve(const TString& poolId, TStore& store, const TString& resolver) {
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

        if (!poolInfo->Config.IsWorkloadServiceRequired()) {
            store = TBypass{.Resolver = resolver};
        } else if (!poolInfo->Config.IsAdmissionRequired()) {
            store = TResolvedPoolId{
                .PoolId = poolId,
                .Resolver = resolver,
                .SkipAdmission = true,
                .PoolConfig = poolInfo->Config,
            };
        } else {
            store = TResolvedPoolId{.PoolId = poolId, .Resolver = resolver};
        }

        return true;
    }

    ///
    /// Binds a shared-reading streaming query to ResourcePoolForSharedReading.
    /// Unlike TryResolve, never soft-falls back to an unresolved pool id and never
    /// returns TBypass (which would lose the pool identity in the session actor).
    ///
    void ForceResolveSharedReadingPool() {
        static constexpr char resolver[] = "ResourcePoolForSharedReading";
        const TString& poolId = ResourcePoolForSharedReading;

        const auto* poolInfo = FindPool(poolId);
        if (!poolInfo) {
            PostClassifyResult = TReject{
                .Code = Ydb::StatusIds::NOT_FOUND,
                .Message = TStringBuilder()
                    << "Resource pool for shared reading: " << poolId << " not found"
                    << ", resolved by: " << resolver,
                .Resolver = resolver,
            };
            return;
        }

        if (!UserHasAccess(*poolInfo, NACLib::DescribeSchema)) {
            PostClassifyResult = TReject{
                .Code = Ydb::StatusIds::NOT_FOUND,
                .Message = TStringBuilder()
                    << "Resource pool for shared reading: " << poolId
                    << " not found or you don't have describe permissions"
                    << ", resolved by: " << resolver,
                .Resolver = resolver,
            };
            return;
        }

        if (!UserHasAccess(*poolInfo, NACLib::SelectRow)) {
            PostClassifyResult = TReject{
                .Code = Ydb::StatusIds::UNAUTHORIZED,
                .Message = TStringBuilder()
                    << "No access permissions for resource pool for shared reading: " << poolId
                    << ", resolved by: " << resolver,
                .Resolver = resolver,
            };
            return;
        }

        // Always keep PoolId = ResourcePoolForSharedReading. Skip WMS only when the
        // pool has no admission gating (same as TryResolve's SkipAdmission path);
        // unconstrained pools still get an explicit PoolId instead of TBypass.
        if (!poolInfo->Config.IsAdmissionRequired()) {
            PostClassifyResult = TResolvedPoolId{
                .PoolId = poolId,
                .Resolver = resolver,
                .SkipAdmission = true,
                .PoolConfig = poolInfo->Config,
            };
        } else {
            PostClassifyResult = TResolvedPoolId{.PoolId = poolId, .Resolver = resolver};
        }
    }

    bool NeedSharedReadingPoolCheck(const NKqp::TUserRequestContext& userRequestContext) const {
        return userRequestContext.IsStreamingQuery && ResourcePoolForSharedReading;
    }

private:
    const TResourcePoolMapPtr ResourcePoolMap;
    const TClassifierConfigsView ClassifierView;
    const TString DatabaseId;
    const TClassifyContext Context;
    const TString ResourcePoolForSharedReading;
    std::optional<TPreCompileClassifyResult> PreClassifyResult;
    std::optional<TPostCompileClassifyResult> PostClassifyResult;
    mutable std::unordered_map<TString, bool> MemberNameCache;
};

std::shared_ptr<IQueryClassifier> CreateQueryClassifier(TResourcePoolMapPtr resourcePoolMap,
                                                        TClassifierConfigsView classifierView,
                                                        const TString& databaseId,
                                                        TClassifyContext context) {
    return std::make_shared<TQueryClassifier>(std::move(resourcePoolMap), std::move(classifierView), databaseId, std::move(context));
}

} // namespace NKikimr::NWorkloadManager
