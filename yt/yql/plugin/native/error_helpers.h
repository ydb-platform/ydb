#pragma once

#include <util/generic/string.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

TString IssuesToYtErrorYson(const NYql::TIssues& issues);

TString ExceptionToYtErrorYson(const std::exception& exception);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
