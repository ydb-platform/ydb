#pragma once

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <util/generic/string.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

TString IssuesToYtErrorYson(const NYql::TIssues& issues);

TString MessageToYtErrorYson(const TString& message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
