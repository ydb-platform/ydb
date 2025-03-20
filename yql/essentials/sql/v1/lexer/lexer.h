#pragma once

#include <yql/essentials/parser/lexer_common/lexer.h>

namespace NSQLTranslationV1 {

struct TLexers {
    NSQLTranslation::TLexerFactoryPtr Antlr3;
    NSQLTranslation::TLexerFactoryPtr Antlr3Ansi;
    NSQLTranslation::TLexerFactoryPtr Antlr4;
    NSQLTranslation::TLexerFactoryPtr Antlr4Ansi;
    NSQLTranslation::TLexerFactoryPtr Antlr4Pure;
    NSQLTranslation::TLexerFactoryPtr Antlr4PureAnsi;
};

NSQLTranslation::ILexer::TPtr MakeLexer(const TLexers& lexers, bool ansi, bool antlr4, bool pure = false);

// "Probably" because YQL keyword can be an identifier
// depending on a query context. For example
// in SELECT * FROM group - group is an identifier, but
// in SELECT * FROM ... GROUP BY ... - group is a keyword.
bool IsProbablyKeyword(const NSQLTranslation::TParsedToken& token);

bool SplitQueryToStatements(const TString& query, NSQLTranslation::ILexer::TPtr& lexer,
    TVector<TString>& statements, NYql::TIssues& issues, const TString& file = "");
}
