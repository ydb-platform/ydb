#pragma once

#include <yql/essentials/core/issue/protos/issue_id.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_id.h>

#ifdef _win_
    #ifdef GetMessage
        #undef GetMessage
    #endif
#endif

namespace NYql {

extern const std::array<char, 14> IssueMapResource;
using EYqlIssueCode = TIssuesIds::EIssueCode;

inline ESeverity GetSeverity(EYqlIssueCode id) {
    return GetSeverity<TIssuesIds, IssueMapResource.data()>(id);
}

inline TString GetMessage(EYqlIssueCode id) {
    return GetMessage<TIssuesIds, IssueMapResource.data()>(id);
}

inline TIssue& SetIssueCode(EYqlIssueCode id, TIssue& issue) {
    issue.SetCode(id, GetSeverity(id));
    return issue;
}

inline TString IssueCodeToString(EYqlIssueCode id) {
    const TString& message = GetMessage(id);
    if (message) {
        return message;
    } else {
        return IssueCodeToString<TIssuesIds>(id);
    }
}

inline TIssue YqlIssue(const TPosition& position, EYqlIssueCode id, const TString& message) {
    TIssue issue(position, message);
    SetIssueCode(id, issue);

    return issue;
}

inline TIssue YqlIssue(const TPosition& position, EYqlIssueCode id) {
    return YqlIssue(position, id, IssueCodeToString(id));
}

// reportTarget allows to describe a way to report bugs (e.g. a GitHub issue)
void CheckFatalIssues(TIssues& issues, const TString& reportTarget);

} // namespace NYql
