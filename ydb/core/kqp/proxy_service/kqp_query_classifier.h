#pragma once

#include <ydb/core/kqp/common/simple/helpers.h>
#include <ydb/core/kqp/gateway/behaviour/resource_pool_classifier/snapshot.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/kqp/workload_service/kqp_workload_service.h>
#include <ydb/core/resource_pools/resource_pool_classifier_settings.h>
#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NKqp {

constexpr char DEFAULT_POOL_ID[] = "default";
constexpr char REJECT_POOL_ID[]  = "_reject";

struct TClassifyContext {
    const TString PoolId;
    const TString DatabaseId;
    const TString AppName;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;

    mutable std::unordered_map<TString, bool> MemberNameCache;
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

///
/// Manages per-query workload manager policies
///
struct IWmQueryClassifier {
    static inline const NResourcePool::TPoolSettings EMPTY_POOL{};

    struct TResolvedPoolId {
        TString PoolId;
    };

    struct TBypass {};

    struct TReject {
        Ydb::StatusIds::StatusCode Code;
        TString Message;
    };

    // Need query plan (e.g. FullScan check) to finish classification
    struct TPendingCompilation {};

    using TPreClassifyResult = std::variant<TBypass, TResolvedPoolId, TReject, TPendingCompilation>;
    using TPostClassifyResult = std::variant<TResolvedPoolId, TBypass, TReject>;

    virtual ~IWmQueryClassifier() = default;

    virtual TPreClassifyResult GetPreClassifyResult() const = 0;

    /// Refines classification once the query plan is available
    [[nodiscard]]
    virtual TPostClassifyResult PostCompileClassify(const TPreparedQueryHolder& preparedQuery) const = 0;
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
    // Manual classification overrides
    void Reject(const Ydb::StatusIds::StatusCode code, const TString& message);
    void ResolveToDefault();
    void Resolve(const TString& poolId);
    void Bypass();

    /// Returns IDs of pools that couldn't be found during classification
    const std::unordered_set<TString>& GetMissedPoolIds() const {
        return MissedPoolIds;
    }

    /// Runs classification before the query is compiled
    TPreClassifyResult GetPreClassifyResult() const override;

    void PreCompileClassify();
    [[nodiscard]]
    TPostClassifyResult PostCompileClassify(const TPreparedQueryHolder& preparedQuery) const override;

private:
    void PendingCompile(i64 rank);
    const TPoolInfoSnapshot::TPoolEntry* FindPool(const TString& poolId) const;

    template<typename TStore>
    bool TryResolve(const TString& poolId, TStore& store, std::unordered_set<TString>* missedPoolIds = nullptr) const {
        auto poolInfo = FindPool(poolId);

        if (poolId == REJECT_POOL_ID) {
            store = TReject{
                .Code = Ydb::StatusIds::ABORTED,
                .Message = TStringBuilder() << "Query is rejected by classifier"
            };
            return true;
        }

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
