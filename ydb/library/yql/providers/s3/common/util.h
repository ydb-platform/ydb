#pragma once

#include <util/string/builder.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NYql::NS3Util {

TIssues AddParentIssue(const TStringBuilder& prefix, TIssues&& issues);

}
