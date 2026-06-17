#include "cursor_text.h"

#include <util/generic/yexception.h>

namespace NSQLPureAST {

TCursorText TCursorText::FromSharped(TString& text Y_LIFETIME_BOUND) {
    constexpr char delim = '#';

    size_t pos = text.find_first_of(delim);
    if (pos == TString::npos) {
        return {
            .Text = text,
        };
    }

    Y_ENSURE(!TStringBuf(text).Tail(pos + 1).Contains(delim));
    text.erase(std::begin(text) + pos);
    return {
        .Text = text,
        .CursorPosition = pos,
    };
}

} // namespace NSQLPureAST
