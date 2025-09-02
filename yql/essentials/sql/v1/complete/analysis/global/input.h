#pragma once

#include "parse_tree.h"

#include <yql/essentials/sql/v1/complete/core/input.h>

namespace NSQLComplete {

    struct TParsedInput {
        TCompletionInput Original;
        antlr4::CommonTokenStream* Tokens;
        SQLv1* Parser;
        SQLv1::Sql_queryContext* SqlQuery;
    };

} // namespace NSQLComplete
