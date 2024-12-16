#pragma once

#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>

namespace NFq {

NYql::TIssue MakeFatalIssue(ui32 id, const TString& message);

NYql::TIssue MakeErrorIssue(ui32 id, const TString& message);

NYql::TIssue MakeWarningIssue(ui32 id, const TString& message);

NYql::TIssue MakeInfoIssue(ui32 id, const TString& message);

}
