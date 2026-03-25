#pragma once

#include <ydb/core/kqp/gateway/behaviour/resource_pool_classifier/snapshot.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/kqp/workload_service/kqp_workload_service.h>
#include <ydb/core/resource_pools/resource_pool_classifier_settings.h>
#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NKqp {

struct TClassifyContext {
    const TString PoolId;
    const TString DatabaseId;
    const TString AppName;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;

    mutable std::unordered_map<TString, bool> MemberNameCache;
};

///
/// Per-query classifier object. Created in the proxy,
/// passed to the session actor with the query request.
///
struct IWmQueryClassifier {
    /// Assignment to a specific resource pool
    struct TResolvedPoolId {
        TString PoolId;
    };

    /// Execution without workload management (bypass)
    struct TBypass {};

    /// Request rejected due to policy or access violation
    struct TReject {
        Ydb::StatusIds::StatusCode Code;
        TString Message;
    };

    ///
    /// Wait for query compilation to evaluate plan-based rules
    ///
    struct TPendingCompilation {};

    using TPreClassifyResult = std::variant<TBypass, TResolvedPoolId, TReject, TPendingCompilation>;
    using TPostClassifyResult = std::variant<TResolvedPoolId, TBypass, TReject>;

    virtual ~IWmQueryClassifier() = default;

    ///
    /// Returns the current classification state
    ///
    virtual TPreClassifyResult GetPreClassifyResult() const = 0;

    ///
    /// Resume classification using the compiled query plan (e.g., FullScan check)
    ///
    [[nodiscard]] virtual TPostClassifyResult PostCompileClassify(const TPreparedQueryHolder& preparedQuery) const = 0;
};

class TPoolInfoSnapshot {
public:
    struct TPoolEntry {
        NResourcePool::TPoolSettings Config;
        std::optional<NACLib::TSecurityObject> SecurityObject;

        bool UserHasAccess(const TClassifyContext& context) const {
            if (!context.UserToken || context.UserToken->GetSerializedToken().empty()) {
                return true;
            }

            if (!SecurityObject) {
                return true;
            }

            return SecurityObject->CheckAccess(NACLib::DescribeSchema
                | NACLib::SelectRow, *context.UserToken);
        }
    };

    using TPoolsMap = std::unordered_map<TString, TPoolEntry>;

public:
    explicit TPoolInfoSnapshot(TPoolsMap pools)
        : Pools(std::move(pools))
    {}

    const TPoolEntry* FindPool(const TString& databaseId, const TString& poolId) const {
        auto it = Pools.find(databaseId + "/" + poolId);
        return it != Pools.end() ? &it->second : nullptr;
    }

private:
    TPoolsMap Pools;
};

class TWmQueryClassifier : public IWmQueryClassifier {
public:
    using TClassifierSnapshotPtr = std::shared_ptr<const TResourcePoolClassifierSnapshot>;
    using TPoolInfoSnapshotPtr = std::shared_ptr<const TPoolInfoSnapshot>;

public:
    TWmQueryClassifier(const TPoolInfoSnapshotPtr poolInfoSnapshot,
                       const TClassifierSnapshotPtr classifierSnapshot,
                       const TClassifyContext context);

    ~TWmQueryClassifier() = default;

public:
    void Reject(const Ydb::StatusIds::StatusCode code, const TString& message);
    void ResolveToDefault();
    void Resolve(const TString& poolId);
    void Bypass();

    const std::unordered_set<TString>& GetMissedPoolIds() const {
        return MissedPoolIds;
    }

    TPreClassifyResult GetPreClassifyResult() const override;

    void PreCompileClassify();
    [[nodiscard]] TPostClassifyResult PostCompileClassify(const TPreparedQueryHolder& preparedQuery) const override;

private:
    void PendingCompile(i64 rank);
    const TPoolInfoSnapshot::TPoolEntry* FindPool(const TString& poolId) const;

    template<typename TStore>
    bool TryResolve(const TString& poolId, TStore& store, std::unordered_set<TString>* missedPoolIds = nullptr) const {
        auto poolInfo = FindPool(poolId);

        if (!poolInfo) {
            store = TResolvedPoolId{.PoolId = poolId};
            if (missedPoolIds) missedPoolIds->emplace(poolId);
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
    std::optional<i64> ResumeRank;
    const std::map<i64, TResourcePoolClassifierConfig>* Configs;
    TPreClassifyResult PreClassifyResult;
    std::unordered_set<TString> MissedPoolIds;
};

} // namespace NKikimr::NKqp
