#pragma once

#include <yql/essentials/ast/yql_errors.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/set.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/public/udf_meta/udf_meta.h>

namespace NYql::NFastCheck {

enum class ESyntax {
    SExpr,
    YQL,
    PG
};

enum class EMode {
    Default,
    Main,
    Library,
    View
};

enum EClusterMode {
    Many,
    Single,
    Unknown
};

struct TCheckFilter {
    bool Include = true;
    TString CheckNameGlob;
};

struct TChecksRequest {
    TString Program;
    TString File;
    EClusterMode ClusterMode = Many;
    TString ClusterSystem;
    THashMap<TString, TString> ClusterMapping;
    ESyntax Syntax = ESyntax::YQL;
    ui16 SyntaxVersion = 1;
    TLangVersion LangVer = MinLangVersion;
    bool IsAnsiLexer = false;
    EMode Mode = EMode::Default;
    const IUdfMeta* UdfMeta = nullptr;
    TMaybe<TVector<TCheckFilter>> Filters;
    TString IssueReportTarget;
};

struct TCheckResponse {
    TString CheckName;
    bool Success = false;
    TIssues Issues;
};

struct TChecksResponse {
    TVector<TCheckResponse> Checks;
};

TVector<TCheckFilter> ParseChecks(const TString& checks);
TSet<TString> ListChecks(const TMaybe<TVector<TCheckFilter>>& filters = Nothing());
TChecksResponse RunChecks(const TChecksRequest& request);

} // namespace NYql::NFastCheck
