#pragma once

#include <ydb/core/kqp/common/simple/helpers.h>
#include <ydb/core/kqp/gateway/behaviour/resource_pool_classifier/snapshot.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/core/kqp/workload_service/kqp_workload_service.h>
#include <ydb/core/resource_pools/resource_pool_classifier_settings.h>
#include <ydb/library/aclib/aclib.h>

#include <util/string/builder.h>

namespace NKikimr::NKqp {

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

inline TString GetPoolKey(const TString& databaseId, const TString& poolId) {
    return TStringBuilder() << databaseId << "/" << poolId;
}

namespace NWorkload {

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
    virtual TPostCompileClassifyResult PostCompileClassify(const TPreparedQueryHolder& preparedQuery) = 0;

    /// Get the current classification state
    [[nodiscard]]
    virtual EState GetState() const = 0;
};

std::shared_ptr<IQueryClassifier> CreateQueryClassifier(TResourcePoolMapPtr resourcePoolMap,
                                                        TClassifierConfigsView classifierView,
                                                        const TString& databaseId,
                                                        TClassifyContext context);

} // namespace NWorkload

} // namespace NKikimr::NKqp
