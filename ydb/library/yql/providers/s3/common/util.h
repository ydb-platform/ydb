#pragma once

#include <util/string/builder.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NYql::NS3Util {

TIssues AddParentIssue(const TString& prefix, TIssues&& issues);
TIssues AddParentIssue(const TStringBuilder& prefix, TIssues&& issues);

// Like UrlEscape with forceEscape = true
// from ydb/library/cpp/string_utils/quote/quote.h, but also escapes:
// '#', '?'
TString UrlEscapeRet(const TStringBuf from);

bool ValidateS3ReadSchema(TPositionHandle pos, std::string_view format, const TStructExprType* schemaStructRowType, bool enableCoroReadActor, TExprContext& ctx);
bool ValidateS3WriteSchema(TPositionHandle pos, std::string_view format, const TStructExprType* schemaStructRowType, TExprContext& ctx);

class TUrlBuilder {
    struct TParam {
        TString Name;
        TString Value;
    };

public:
    explicit TUrlBuilder(const TString& uri);

    TUrlBuilder& AddUrlParam(const TString& name, const TString& value = "");

    TString Build() const;

private:
    std::vector<TParam> Params;
    TString MainUri;
};

}
