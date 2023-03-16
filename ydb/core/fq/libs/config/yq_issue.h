#pragma once

#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NFq {

NYql::TIssue MakeFatalIssue(TIssuesIds::EIssueCode id, const TString& message);

NYql::TIssue MakeErrorIssue(TIssuesIds::EIssueCode id, const TString& message);

NYql::TIssue MakeWarningIssue(TIssuesIds::EIssueCode id, const TString& message);

NYql::TIssue MakeInfoIssue(TIssuesIds::EIssueCode id, const TString& message);

}
