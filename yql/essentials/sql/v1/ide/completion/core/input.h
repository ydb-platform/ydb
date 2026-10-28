#pragma once

#include <yql/essentials/sql/v1/ide/pure_ast/cursor_text.h>
#include <yql/essentials/sql/v1/ide/pure_ast/parse_tree.h>

#include <util/generic/string.h>

namespace NSQLComplete {

struct TCompletionInput: NSQLPureAST::TCursorText {
    NSQLPureAST::IParseTree::TPtr ParseTree = nullptr;
};

struct TMaterializedInput {
    TString Text;
    size_t CursorPosition = Text.length();
};

TCompletionInput SharpedInput(TString& text Y_LIFETIME_BOUND);

} // namespace NSQLComplete
