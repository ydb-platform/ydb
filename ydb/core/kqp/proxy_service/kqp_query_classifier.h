#pragma once

#include <ydb/core/kqp/gateway/behaviour/resource_pool_classifier/snapshot.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
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
/// Per-query classifier object. Created by TResourcePoolsCache in the proxy,
/// passed to the session actor with the query request.
///
struct IWmQueryClassifier {
    struct TResolvedPoolId {
        TString PoolId;
    };

    struct TBypass {};
    struct TReject {
        Ydb::StatusIds::StatusCode Code;
        TString Message;
    };

    struct TPendingCompilation {
    };

    using TPreClassifyResult = std::variant<TBypass, TResolvedPoolId, TReject, TPendingCompilation>;
    using TPostClassifyResult = std::variant<TResolvedPoolId, TBypass, TReject>;

    virtual ~IWmQueryClassifier() = default;

    /// Current classification 
    virtual TPreClassifyResult GetPreClassifyResult() const = 0;

    /// Called by session actor after compilation.
    /// Evaluates rules that need compiled query plan.
    [[nodiscard]] virtual TPostClassifyResult PostCompileClassify(const TPreparedQueryHolder& preparedQuery) const = 0;
};

class TWmQueryClassifier : public IWmQueryClassifier {
public:
    using TSnapshotPtr = std::shared_ptr<const TResourcePoolClassifierSnapshot>;

public:
    TWmQueryClassifier(TSnapshotPtr snapshot, const TClassifyContext context);
    ~TWmQueryClassifier() = default;

public:
    void Reject(const Ydb::StatusIds::StatusCode code, const TString message);
    void Resolve(const TString& poolId);
    void Bypass();

    TPreClassifyResult GetPreClassifyResult() const override;

    void PreCompileClassify();
    [[nodiscard]] TPostClassifyResult PostCompileClassify(const TPreparedQueryHolder& preparedQuery) const override;

private:
    void PendingCompile(i64 rank);

private:
    TPreClassifyResult PreClassifyResult;
    const TSnapshotPtr Snapshot;
    const TClassifyContext Context;
    std::optional<i64> ResumeRank;
    const std::map<i64, TResourcePoolClassifierConfig>* Configs = nullptr;
};

} // namespace NKikimr::NKqp
