#pragma once

#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_id.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

namespace NKikimr {

extern const char IssueMapResource[17];

inline NYql::ESeverity GetSeverity(NYql::TIssueCode id) {
    return NYql::GetSeverity<NKikimrIssues::TIssuesIds, IssueMapResource>(id);
}

inline TString GetMessage(NYql::TIssueCode id) {
    return NYql::GetMessage<NKikimrIssues::TIssuesIds, IssueMapResource>(id);
}

inline NYql::TIssue& SetIssueCode(NYql::TIssueCode id, NYql::TIssue& issue) {
    issue.SetCode(id, GetSeverity(id));
    return issue;
}

inline TString IssueCodeToString(NYql::TIssueCode id) {
    const TString& message = GetMessage(id);
    if (message) {
        return message;
    } else {
        return NYql::IssueCodeToString<NKikimrIssues::TIssuesIds>(id);
    }
}

inline NYql::TIssue MakeIssue(NKikimrIssues::TIssuesIds::EIssueCode id, const TString& message) {
    NYql::TIssue issue;

    SetIssueCode(id, issue);
    issue.SetMessage(message);

    return issue;
}

inline NYql::TIssue MakeIssue(NKikimrIssues::TIssuesIds::EIssueCode id) {
    return MakeIssue(id, IssueCodeToString(id));
}

inline TString SerializeIssues(const NYql::TIssues& in) {
    NYql::TIssue rootIssue;
    for(const auto& i : in) {
        rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
    }
    return NYql::IssueToBinaryMessage(rootIssue);
}

inline NYql::TIssues DeserializeIssues(const TString& in) {
    NYql::TIssues result;
    NYql::TIssue issue = NYql::IssueFromBinaryMessage(in);
    for (const auto& i : issue.GetSubIssues()) {
        result.AddIssue(*i);
    }
    return result;
}

}
