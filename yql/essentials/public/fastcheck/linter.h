#pragma once

#include <yql/essentials/ast/yql_errors.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql {
namespace NFastCheck {

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
    bool IsAnsiLexer = false;
    EMode Mode = EMode::Default;
    TMaybe<TVector<TCheckFilter>> Filters;
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

}
}
