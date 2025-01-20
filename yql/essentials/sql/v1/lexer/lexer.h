#pragma once

#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/sql/settings/translation_settings.h>

namespace NSQLTranslationV1 {

NSQLTranslation::ILexer::TPtr MakeLexer(bool ansi, bool antlr4);

// "Probably" because YQL keyword can be an identifier
// depending on a query context. For example
// in SELECT * FROM group - group is an identifier, but
// in SELECT * FROM ... GROUP BY ... - group is a keyword.
bool IsProbablyKeyword(const NSQLTranslation::TParsedToken& token);

bool SplitQueryToStatements(const TString& query, TVector<TString>& statements, NYql::TIssues& issues,
    const NSQLTranslation::TTranslationSettings& settings);
bool SplitQueryToStatements(const TString& query, NSQLTranslation::ILexer::TPtr& lexer,
    TVector<TString>& statements, NYql::TIssues& issues);

}
