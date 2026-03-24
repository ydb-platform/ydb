#include "kqp_query_classifier.h"

namespace NKikimr::NKqp {
namespace {

bool MatchesMemberName(const TString& target, const TClassifyContext& ctx) {
    if (auto it = ctx.MemberNameCache.find(target); it != ctx.MemberNameCache.end()) {
        return it->second;
    }

    if (!ctx.UserToken) {
        return false;
    }

    bool found = (target == ctx.UserToken->GetUserSID());

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
    if (s.AppName && *s.AppName != ctx.AppName) {
        return false;
    }

    if (s.MemberName && !MatchesMemberName(*s.MemberName, ctx)) {
        return false;
    }

    return true;
}

bool NeedsPreparedQuery(const NResourcePool::TClassifierSettings& s) {
    return s.FullScan.has_value();
}

bool MatchesDynamic(const NResourcePool::TClassifierSettings& s, const TPreparedQueryHolder&) {
    if (!s.FullScan) {
        return true;
    }

    // TODO: the implementation
    return true;
}

} // namespace anonymous

TWmQueryClassifier::TWmQueryClassifier(TSnapshotPtr snapshot, const TClassifyContext context)
    : Snapshot(std::move(snapshot))
    , Context(std::move(context))
{
    const auto& dbConfigs = Snapshot->GetResourcePoolClassifierConfigsByRank();
    auto it = dbConfigs.find(Context.DatabaseId);

    if (it != dbConfigs.end()) {
        Configs = &it->second;
    }
}

void TWmQueryClassifier::PreCompileClassify() {
    if (Context.PoolId) {
        Resolve(Context.PoolId);
        return;
    }
    
    if (!Snapshot || !Configs) {
        Bypass();
        return;
    }

    for (auto it = Configs->begin(); it != Configs->end(); ++it) {
        const auto& [rank, value] = *it;
        const NResourcePool::TClassifierSettings& settings = value.GetClassifierSettings();

        if (MatchesStatic(settings, Context)) {
            if (!NeedsPreparedQuery(settings)) {
                Resolve(settings.ResourcePool);
                return;
            }

            PendingCompile(rank);
            return;
        }
    }

    Bypass();
}

IWmQueryClassifier::TPostClassifyResult TWmQueryClassifier::PostCompileClassify(const TPreparedQueryHolder& preparedQuery) const {
    if (!Configs) {
        return TBypass{};
    }

    Y_ENSURE(ResumeRank);
    
    for (auto it = Configs->lower_bound(*ResumeRank); it != Configs->end(); ++it) {
        const auto& settings = it->second.GetClassifierSettings();

        if (MatchesStatic(settings, Context) && MatchesDynamic(settings, preparedQuery)) {
            return TResolvedPoolId{.PoolId = settings.ResourcePool};
        }
    }

    return TBypass{};
}

IWmQueryClassifier::TPreClassifyResult TWmQueryClassifier::GetPreClassifyResult() const {
    return PreClassifyResult;
}

void TWmQueryClassifier::Reject(const Ydb::StatusIds::StatusCode code, const TString message) {
    PreClassifyResult = TReject{.Code = code, .Message = message};
}

void TWmQueryClassifier::Resolve(const TString& poolId) {
    PreClassifyResult = TResolvedPoolId{poolId};
}

void TWmQueryClassifier::Bypass() {
    PreClassifyResult = TBypass();
}

void TWmQueryClassifier::PendingCompile(i64 rank) {
    ResumeRank = rank;
    PreClassifyResult = TPendingCompilation();
}

} // namespace NKikimr::NKqp
