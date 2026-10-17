#include "input.h"

#include <util/generic/yexception.h>

namespace NSQLComplete {

TCompletionInput SharpedInput(TString& text) {
    using NSQLPureAST::TCursorText;

    TCompletionInput input;
    static_cast<TCursorText&>(input) = TCursorText::FromSharped(text);
    return input;
}

} // namespace NSQLComplete
