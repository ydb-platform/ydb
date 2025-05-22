#pragma once

#include "global.h"
#include "parse_tree.h"

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    TMaybe<TUseContext> FindUseStatement(
        SQLv1::Sql_queryContext* ctx,
        antlr4::TokenStream* tokens,
        size_t cursorPosition);

} // namespace NSQLComplete
