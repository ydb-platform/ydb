#include "check_runner.h"
#include <yql/essentials/core/langver/yql_core_langver.h>

namespace NYql::NFastCheck {

TCheckResponse TCheckRunnerBase::Run(const TChecksRequest& request, TCheckState& state) {
    TMaybe<TIssue> verIssue;
    if (!CheckLangVersion(request.LangVer, GetMaxReleasedLangVersion(), verIssue)) {
        TCheckResponse response;
        response.Success = false;
        response.CheckName = GetCheckName();
        response.Issues.AddIssue(*verIssue);
        return response;
    }

    auto ret = DoRun(request, state);
    if (!verIssue) {
        return ret;
    }

    TCheckResponse response;
    response.Success = ret.Success;
    response.CheckName = GetCheckName();
    response.Issues.AddIssue(*verIssue);
    response.Issues.AddIssues(ret.Issues);
    return response;
}

} // namespace NYql::NFastCheck
