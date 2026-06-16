#pragma once

#include <yql/essentials/sql/v1/ide/completion/core/input.h>
#include <yql/essentials/sql/v1/ide/pure_ast/parse_tree.h>

namespace NSQLComplete {

using namespace NSQLPureAST;

struct TParsedInput {
    TCompletionInput Original; // with CursorPosition in UTF8 runes, not bytes
    antlr4::CommonTokenStream* Tokens;
    SQLv1* Parser;
    SQLv1::Sql_queryContext* SqlQuery;
};

} // namespace NSQLComplete
