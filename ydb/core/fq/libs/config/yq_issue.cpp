#include "yq_issue.h"
#include <yql/essentials/public/issue/protos/issue_severity.pb.h>
#include <yql/essentials/public/issue/yql_issue_id.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NFq {

NYql::TIssue MakeFatalIssue(ui32 id, const TString& message) {
    NYql::TIssue issue;
    issue.SetCode(id, NYql::TSeverityIds::S_FATAL);
    issue.SetMessage(message);
    return issue;
}

NYql::TIssue MakeErrorIssue(ui32 id, const TString& message) {
    NYql::TIssue issue;
    issue.SetCode(id, NYql::TSeverityIds::S_ERROR);
    issue.SetMessage(message);
    return issue;
}

NYql::TIssue MakeWarningIssue(ui32 id, const TString& message) {
    NYql::TIssue issue;
    issue.SetCode(id, NYql::TSeverityIds::S_WARNING);
    issue.SetMessage(message);
    return issue;
}

NYql::TIssue MakeInfoIssue(ui32 id, const TString& message) {
    NYql::TIssue issue;
    issue.SetCode(id, NYql::TSeverityIds::S_INFO);
    issue.SetMessage(message);
    return issue;
}

}
