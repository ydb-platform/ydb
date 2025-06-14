#include "check_runner.h"
#include <yql/essentials/core/langver/yql_core_langver.h>

namespace NYql {
namespace NFastCheck {

TCheckResponse TCheckRunnerBase::Run(const TChecksRequest& request) {
    TMaybe<TIssue> verIssue;
    if (!CheckLangVersion(request.LangVer, GetMaxReleasedLangVersion(), verIssue)) {
        TCheckResponse response;
        response.Success = false;
        response.CheckName = GetCheckName();
        response.Issues.AddIssue(*verIssue);
        return response;
    }

    auto ret = DoRun(request);
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

}
}
