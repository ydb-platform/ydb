#pragma once
#include <yql/essentials/ast/yql_errors.h>
#include <util/generic/hash.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/public/langver/yql_langver.h>

namespace NYql::NFastCheck {

struct TOptions {
    bool IsSql = true;
    bool ParseOnly = false;
    THashMap<TString, TString> ClusterMapping;
    ui16 SyntaxVersion = 1;
    TLangVersion LangVer = MinLangVersion;
    bool IsLibrary = false;
    THashMap<TString, TString> SqlLibs = {}; // mapping file name => SQL
};

bool CheckProgram(const TString& program, const TOptions& options, TIssues& errors);

} // namespace NYql::NFastCheck
