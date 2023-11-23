#pragma once
#include "query_ast.h"
#include "settings.h"

namespace NKikimr::NKqp {

struct TKqpQueryRef {
    TKqpQueryRef(const TString& text, std::shared_ptr<std::map<TString, Ydb::Type>> parameterTypes = {}, const TMaybe<TQueryAst>& astResult = {})
        : Text(text)
        , ParameterTypes(parameterTypes)
        , AstResult(astResult)
    {}

    // Text is owned by TKqpQueryId
    const TString& Text;
    std::shared_ptr<std::map<TString, Ydb::Type>> ParameterTypes;
    TMaybe<TQueryAst> AstResult;
};

} // namespace NKikimr::NKqp
