#include "yql_provider_gateway.h"

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {
namespace NCommon {

void TOperationResult::AddIssue(const TIssue& issue) {
    WalkThroughIssues(issue, false, [&](const TIssue& err, ui16 level) {
        Y_UNUSED(level);
        YQL_CLOG(NOTICE, ProviderCommon) << err;
    });
    Issues_.AddIssue(issue);
}

void TOperationResult::AddIssues(const TIssues& issues) {
    for (auto& topIssue: issues) {
        WalkThroughIssues(topIssue, false, [&](const TIssue& err, ui16 level) {
            Y_UNUSED(level);
            YQL_CLOG(NOTICE, ProviderCommon) << err;
        });
    }
    Issues_.AddIssues(issues);
}

void TOperationResult::SetException(const std::exception& e, const TPosition& pos) {
    YQL_CLOG(ERROR, ProviderCommon) << e.what();

    auto issue = ExceptionToIssue(e, pos);
    Status_ = static_cast<NYql::EYqlIssueCode>(issue.GetCode());
    Issues_.AddIssue(issue);
}

void TOperationResult::ReportIssues(TIssueManager& issueManager) const {
    issueManager.RaiseIssues(Issues_);
}

} // NCommon
} // NYql
