#pragma once

#include <ydb/library/yql/parser/lexer_common/lexer.h>

namespace NSQLTranslationV1 {

NSQLTranslation::ILexer::TPtr MakeLexer(bool ansi, bool antlr4);

// "Probably" because YQL keyword can be an identifier
// depending on a query context. For example
// in SELECT * FROM group - group is an identifier, but
// in SELECT * FROM ... GROUP BY ... - group is a keyword.
bool IsProbablyKeyword(const NSQLTranslation::TParsedToken& token);

}
