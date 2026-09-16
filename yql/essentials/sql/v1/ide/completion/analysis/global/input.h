#pragma once

#include <yql/essentials/sql/v1/ide/completion/core/input.h>
#include <yql/essentials/sql/v1/ide/pure_ast/parse_tree.h>

#include <util/generic/maybe.h>

namespace NSQLComplete {

using namespace NSQLPureAST;

struct TParsedInput {
    TMaybe<TString> RecoveredText;
    size_t CursorPosition = 0; // with CursorPosition in UTF8 runes, not bytes
    IParseTree::TPtr ParseTree;
};

} // namespace NSQLComplete
