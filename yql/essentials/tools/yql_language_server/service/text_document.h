#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/support/synchronization.h>

#include <yql/essentials/sql/v1/ide/pure_ast/parse_tree.h>
#include <yql/essentials/sql/v1/ide/pure_ast/parser.h>

namespace NLsp::NYql {

using NSQLPureAST::IParser;
using NSQLPureAST::IParseTree;
using NSQLPureAST::SQLv1;

class TTextDocument final: public ITextDocument, public IParseTree {
public:
    using TPtr = TIntrusivePtr<TTextDocument>;

    TTextDocument(TTextDocumentItem item, IParser::TPtr parser);

    void Change(TTextDocumentVersion v, TMaybe<TString> t) override;
    TTextDocumentVersion Version() const override;

    TDocumentUri Uri() const;
    TStringBuf Text() const override;
    const antlr4::CommonTokenStream& Tokens() const override;
    const SQLv1& Parser() const override;
    SQLv1::Sql_queryContext* Root() override;

private:
    IParser::TPtr Parser_;
    TTextDocumentItem Item_;
    IParseTree::TPtr ParseTree_;
};

using TTextDocuments = NLsp::TTextDocuments<TTextDocument>;

TTextDocuments::TPtr MakeTextDocuments(IParser::TPtr parser);

} // namespace NLsp::NYql
