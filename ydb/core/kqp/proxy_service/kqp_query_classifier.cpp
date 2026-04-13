#include "kqp_query_classifier.h"
#include <ydb/core/kqp/workload_service/kqp_workload_service.h>

namespace NKikimr::NKqp {
namespace {

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[Classifier] " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[Classifier] " << stream)

bool MatchesMemberName(const TString& target, const TClassifyContext& ctx) {
    // Check anonymous user
    if (!ctx.UserToken) {
        return (target == NACLib::TSID());
    }

    if (auto it = ctx.MemberNameCache.find(target); it != ctx.MemberNameCache.end()) {
        return it->second;
    }

    bool found = false;

    // Check UserSID only for non-system users.
    if (!ctx.UserToken->IsSystemUser()) {
        found = (target == ctx.UserToken->GetUserSID());
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

bool MatchesStatic(const NResourcePool::TClassifierSettings& s, const TClassifyContext& ctx) {
    if (s.MemberName && !MatchesMemberName(*s.MemberName, ctx)) {
        return false;
    }

    return true;
}

bool NeedsPreparedQuery(const NResourcePool::TClassifierSettings&) {
    return false;
}

bool MatchesDynamic(const NResourcePool::TClassifierSettings&, const TPreparedQueryHolder&) {
    return true;
}

} // namespace anonymous

TWmQueryClassifier::TWmQueryClassifier(const TPoolInfoSnapshotPtr poolInfoSnapshot,
    const TClassifierSnapshotPtr classifierSnapshot, const TClassifyContext context)
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

const TPoolInfoSnapshot::TPoolEntry* TWmQueryClassifier::FindPool(const TString& poolId) const {
    if (!PoolInfoSnapshot) {
        return nullptr;
    }

    return PoolInfoSnapshot->FindPool(Context.DatabaseId, poolId);
}

void TWmQueryClassifier::PreCompileClassify() {
    // User requested an explicit pool
    if (Context.PoolId) {
        TryResolve(Context.PoolId, PreClassifyResult, &MissedPoolIds);
        return;
    }

    // If no classification use default pool
    if (!Configs) {
        TryResolve(DEFAULT_POOL_ID, PreClassifyResult, &MissedPoolIds);
        return;
    }

    for (const auto& [rank, value] : *Configs) {
        const NResourcePool::TClassifierSettings& settings = value.GetClassifierSettings();

        if (!MatchesStatic(settings, Context)) {
            continue;
        }

        if (NeedsPreparedQuery(settings)) {
            PendingCompile(rank);
            return;
        }

        if (TryResolve(settings.ResourcePool, PreClassifyResult, &MissedPoolIds)) {
            return;
        }
    }

    // No suitable classification use default pool
    TryResolve(DEFAULT_POOL_ID, PreClassifyResult, &MissedPoolIds);
}

IWmQueryClassifier::TPostClassifyResult TWmQueryClassifier::PostCompileClassify(const TPreparedQueryHolder& preparedQuery) const {
    Y_ENSURE(Configs, "Post compile classify without configuration");
    Y_ENSURE(ResumeRank, "Post compile classify without next rank");

    for (auto it = Configs->lower_bound(*ResumeRank); it != Configs->end(); ++it) {
        const auto& settings = it->second.GetClassifierSettings();

        if (!MatchesStatic(settings, Context)) {
            continue;
        }

        if (!MatchesDynamic(settings, preparedQuery)){
            continue;
        }

        TPostClassifyResult result;

        if (TryResolve(settings.ResourcePool, result)){
            return result;
        }
    }

    // No suitable classification use default pool
    return TResolvedPoolId{.PoolId = DEFAULT_POOL_ID};
}

IWmQueryClassifier::TPreClassifyResult TWmQueryClassifier::GetPreClassifyResult() const {
    return PreClassifyResult;
}

void TWmQueryClassifier::Reject(const Ydb::StatusIds::StatusCode code, const TString& message) {
    PreClassifyResult = TReject{.Code = code, .Message = message};
}

void TWmQueryClassifier::Resolve(const TString& poolId) {
    TryResolve(poolId, PreClassifyResult);
}

void TWmQueryClassifier::ResolveToDefault() {
    Resolve(DEFAULT_POOL_ID);
}

void TWmQueryClassifier::Bypass() {
    PreClassifyResult = TBypass{};
}

void TWmQueryClassifier::PendingCompile(i64 rank) {
    ResumeRank = rank;
    PreClassifyResult = TPendingCompilation();
}

} // namespace NKikimr::NKqp
