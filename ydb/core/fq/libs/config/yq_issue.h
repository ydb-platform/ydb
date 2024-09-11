#pragma once

#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NFq {

NYql::TIssue MakeFatalIssue(NYql::TIssueCode id, const TString& message);

NYql::TIssue MakeErrorIssue(NYql::TIssueCode id, const TString& message);

NYql::TIssue MakeWarningIssue(NYql::TIssueCode id, const TString& message);

NYql::TIssue MakeInfoIssue(NYql::TIssueCode id, const TString& message);

}
