#pragma once

#include <yql/essentials/sql/v1/ide/pure_ast/cursor_text.h>

#include <util/generic/string.h>

namespace NSQLComplete {

using TCompletionInput = NSQLPureAST::TCursorText;

struct TMaterializedInput {
    TString Text;
    size_t CursorPosition = Text.length();
};

TCompletionInput SharpedInput(TString& text Y_LIFETIME_BOUND);

} // namespace NSQLComplete
