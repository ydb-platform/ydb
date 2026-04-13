#include "yql_issue.h"

#include <util/string/builder.h>

namespace NYql {

const std::array<char, 14> IssueMapResource = {"yql_issue.txt"};

static_assert(DEFAULT_ERROR == TIssuesIds::DEFAULT_ERROR,
              "value of particular and common error mismatched for \"DEFAULT_ERROR\"");
static_assert(UNEXPECTED_ERROR == TIssuesIds::UNEXPECTED,
              "value of particular and common error mismatched for \"UNEXPECTED_ERROR\"");

void CheckFatalIssues(TIssues& issues, const TString& reportTarget) {
    bool isFatal = false;
    auto checkIssue = [&](const TIssue& issue) {
        if (issue.GetSeverity() == TSeverityIds::S_FATAL) {
            isFatal = true;
        }
    };

    std::function<void(const TIssuePtr& issue)> recursiveCheck = [&](const TIssuePtr& issue) {
        if (isFatal) {
            return;
        }

        checkIssue(*issue);
        for (const auto& subissue : issue->GetSubIssues()) {
            recursiveCheck(subissue);
        }
    };

    for (const auto& issue : issues) {
        if (isFatal) {
            break;
        }

        checkIssue(issue);
        // check subissues
        for (const auto& subissue : issue.GetSubIssues()) {
            recursiveCheck(subissue);
        }
    }

    if (isFatal) {
        TIssue result;
        result.SetMessage(
            TStringBuilder()
            << "An abnormal situation found, so consider opening a bug report" << reportTarget
            << ", because more detailed information is only available in server side logs and/or "
            << "coredumps.");
        result.SetCode(TIssuesIds::UNEXPECTED, TSeverityIds::S_FATAL);
        issues.AddIssue(result);
    }
}

} // namespace NYql
