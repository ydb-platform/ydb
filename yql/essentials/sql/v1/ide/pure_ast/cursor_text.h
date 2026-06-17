#pragma once

#include <util/generic/string.h>

namespace NSQLPureAST {

struct TCursorText {
    TStringBuf Text;
    size_t CursorPosition = Text.length();

    static TCursorText FromSharped(TString& text Y_LIFETIME_BOUND);
};

} // namespace NSQLPureAST
