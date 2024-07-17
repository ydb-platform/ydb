#pragma once

#include <util/string/builder.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <library/cpp/string_utils/quote/quote.h>

namespace NYql::NS3Util {

TIssues AddParentIssue(const TStringBuilder& prefix, TIssues&& issues);
char* UrlEscape(char* to, const char* from);
TString UrlEscape(const TString& url);
TString UrlEscapeRet(const TStringBuf from);

}
