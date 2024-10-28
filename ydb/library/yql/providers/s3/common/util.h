#pragma once

#include <util/string/builder.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NYql::NS3Util {

TIssues AddParentIssue(const TStringBuilder& prefix, TIssues&& issues);

// Like UrlEscape with forceEscape = true
// from ydb/library/cpp/string_utils/quote/quote.h, but also escapes:
// '#', '?'
TString UrlEscapeRet(const TStringBuf from);

}
