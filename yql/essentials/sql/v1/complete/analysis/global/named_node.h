#pragma once

#include "parse_tree.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    TVector<TString> CollectNamedNodes(
        SQLv1::Sql_queryContext* ctx,
        antlr4::TokenStream* tokens,
        size_t cursorPosition);

} // namespace NSQLComplete
