#pragma once
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/langver/yql_langver.h>

namespace NYql {

bool CheckLangVersion(TLangVersion ver, TLangVersion max, TMaybe<TIssue>& issue);

} // namespace NYql
