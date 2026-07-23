#pragma once

#include <ydb/core/kqp/common/simple/helpers.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/resource_pools/resource_pool_classifier_settings.h>
#include <ydb/services/workload_manager/common/helpers.h>
#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/snapshot.h>
#include <ydb/library/aclib/aclib.h>


namespace NKikimr::NKqp {
struct TUserRequestContext;
}  // namespace NKikimr::NKqp

namespace NKikimr::NWorkloadManager {

struct TClassifyContext {
    const TString PoolId;
    const TString AppName;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

struct TResourcePoolEntry {
    NResourcePool::TPoolSettings Config;
    std::optional<NACLib::TSecurityObject> SecurityObject;
};

using TResourcePoolMap = std::unordered_map<TString, TResourcePoolEntry>;
using TResourcePoolMapPtr = std::shared_ptr<const TResourcePoolMap>;

///
/// Manages per-query workload manager policies
///
class IQueryClassifier {
public:
    enum class EState {
        None,               // Initial state, no classification performed
        PreCompileDone,     // Classified without query plan, final result available
        WaitCompile,        // Awaiting query plan for final classification
        PostCompileDone,    // Classified with query plan, final result available
    };

    struct TResolvedPoolId {
        TString PoolId;
        TString Resolver;
        // If true, session actor should apply PoolConfig directly and skip WMS admission actor.
        // Used when the pool has per-node caps (TotalCpu/TotalMemory) but no admission gating.
        bool SkipAdmission = false;
        NResourcePool::TPoolSettings PoolConfig;
    };

    struct TBypass {
        TString Resolver;
    };

    struct TReject {
        Ydb::StatusIds::StatusCode Code;
        TString Message;
        TString Resolver;
    };

    struct TPendingCompilation {
        i64 ResumeRank;
    };

    using TPreCompileClassifyResult = std::variant<TBypass, TResolvedPoolId, TReject, TPendingCompilation>;
    using TPostCompileClassifyResult = std::variant<TBypass, TResolvedPoolId, TReject>;

    virtual ~IQueryClassifier() = default;

    /// Pre compile classification
    [[nodiscard]]
    virtual TPreCompileClassifyResult PreCompileClassify() = 0;

    /// Refines classification once the query plan is available
    [[nodiscard]]
    virtual TPostCompileClassifyResult PostCompileClassify(const NKqp::TPreparedQueryHolder& preparedQuery, const NKqp::TUserRequestContext& userRequestContext) = 0;

    /// Get the current classification state
    [[nodiscard]]
    virtual EState GetState() const = 0;
};

std::shared_ptr<IQueryClassifier> CreateQueryClassifier(TResourcePoolMapPtr resourcePoolMap,
                                                        TClassifierConfigsView classifierView,
                                                        const TString& databaseId,
                                                        TClassifyContext context);

} // namespace NKikimr::NWorkloadManager
