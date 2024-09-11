#include "yq_issue.h"
#include <ydb/library/yql/public/issue/protos/issue_severity.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_id.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

namespace NFq {

NYql::TIssue MakeFatalIssue(NYql::TIssueCode id, const TString& message) {
    NYql::TIssue issue;
    issue.SetCode(id, NYql::TSeverityIds::S_FATAL);
    issue.SetMessage(message);
    return issue;
}

NYql::TIssue MakeErrorIssue(NYql::TIssueCode id, const TString& message) {
    NYql::TIssue issue;
    issue.SetCode(id, NYql::TSeverityIds::S_ERROR);
    issue.SetMessage(message);
    return issue;
}

NYql::TIssue MakeWarningIssue(NYql::TIssueCode id, const TString& message) {
    NYql::TIssue issue;
    issue.SetCode(id, NYql::TSeverityIds::S_WARNING);
    issue.SetMessage(message);
    return issue;
}

NYql::TIssue MakeInfoIssue(NYql::TIssueCode id, const TString& message) {
    NYql::TIssue issue;
    issue.SetCode(id, NYql::TSeverityIds::S_INFO);
    issue.SetMessage(message);
    return issue;
}

}
