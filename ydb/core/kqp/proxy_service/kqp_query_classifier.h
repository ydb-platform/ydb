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
class IWmQueryClassifier {
public:
    static inline const NResourcePool::TPoolSettings EMPTY_POOL{};

    struct TResolvedPoolId {
        TString PoolId;
    };

    struct TBypass {};

    struct TReject {
        Ydb::StatusIds::StatusCode Code;
        TString Message;
    };

    struct TPendingCompilation {
        i64 ResumeRank;
    };

    using TPreClassifyResult = std::variant<TBypass, TResolvedPoolId, TReject, TPendingCompilation>;
    using TPostClassifyResult = std::variant<TResolvedPoolId, TBypass, TReject>;

    virtual ~IWmQueryClassifier() = default;

    /// Pre compile classification
    [[nodiscard]]
    virtual TPreClassifyResult PreCompileClassify() = 0;

    [[nodiscard]]
    virtual bool NeedsPostCompileClassify() const = 0;

    /// Refines classification once the query plan is available
    [[nodiscard]]
    virtual TPostClassifyResult PostCompileClassify(const TPreparedQueryHolder& preparedQuery) = 0;
};

using TClassifierSnapshotPtr = std::shared_ptr<const TResourcePoolClassifierSnapshot>;
using TPoolInfoSnapshotPtr = std::shared_ptr<const TPoolInfoSnapshot>;

std::shared_ptr<IWmQueryClassifier> CreateWmQueryClassifier(TPoolInfoSnapshotPtr poolInfoSnapshot,
                                                            TClassifierSnapshotPtr classifierSnapshot,
                                                            TClassifyContext context);

std::shared_ptr<IWmQueryClassifier> CreateWmBypassClassifier();

} // namespace NKikimr::NKqp
