#pragma once

#include "parse_tree.h"

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    TMaybe<TString> EnclosingFunction(
        SQLv1::Sql_queryContext* ctx,
        antlr4::TokenStream* tokens,
        size_t cursorPosition);

} // namespace NSQLComplete
