#pragma once

#include "servlet.h"

#include <yql/essentials/sql/v1/ide/completion/name/service/name_service.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>

namespace NYql {
namespace NHttp {

///////////////////////////////////////////////////////////////////////////////
// TSqlCompleteServlet
///////////////////////////////////////////////////////////////////////////////
class TSqlCompleteServlet: public IServlet {
public:
    TSqlCompleteServlet();

    void DoPost(const TRequest& req, TResponse& resp) const override final;

private:
    NSQLComplete::INameService::TPtr MakeRequestNameService(const std::string_view tableAttr, const std::string_view outputTable) const;

    NSQLTranslationV1::TLexers Lexers_;
    NSQLComplete::IRanking::TPtr Ranking_;
    NSQLComplete::INameService::TPtr StaticNameService_;
    NSQLComplete::INameService::TPtr ClusterNameService_;
};

} // namespace NHttp
} // namespace NYql
