#pragma once

#include "parse_tree.h"

#include <yql/essentials/sql/v1/complete/core/input.h>

namespace NSQLComplete {

struct TParsedInput {
    TCompletionInput Original; // with CursorPosition in UTF8 runes, not bytes
    antlr4::CommonTokenStream* Tokens;
    SQLv1* Parser;
    SQLv1::Sql_queryContext* SqlQuery;
};

} // namespace NSQLComplete
