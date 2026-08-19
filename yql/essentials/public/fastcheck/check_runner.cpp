#include "check_runner.h"
#include "check_state.h"

#include <yql/essentials/core/langver/yql_core_langver.h>

namespace NYql::NFastCheck {

const THashSet<ECheckName>& TCheckRunnerBase::Requirements() const {
    static const THashSet<ECheckName> Requirements;
    return Requirements;
}

TCheckResponse TCheckRunnerBase::Run(const TChecksRequest& request, TCheckState& state) {
    for (const auto& requirement : Requirements()) {
        if (state.IsDefinitelyFailed(requirement)) {
            return TCheckResponse{.CheckName = ToString(GetCheckName())};
        }
    }

    TMaybe<TIssue> verIssue;
    if (!CheckLangVersion(request.LangVer, GetMaxReleasedLangVersion(), verIssue)) {
        TCheckResponse response;
        response.Success = false;
        response.CheckName = ToString(GetCheckName());
        response.Issues.AddIssue(*verIssue);
        return response;
    }

    auto ret = DoRun(request, state);
    if (!verIssue) {
        return ret;
    }

    TCheckResponse response;
    response.Success = ret.Success;
    response.CheckName = ToString(GetCheckName());
    response.Issues.AddIssue(*verIssue);
    response.Issues.AddIssues(ret.Issues);
    return response;
}

} // namespace NYql::NFastCheck
