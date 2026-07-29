#include "input.h"

#include <util/generic/yexception.h>

namespace NSQLComplete {

TCompletionInput SharpedInput(TString& text) {
    return NSQLPureAST::TCursorText::FromSharped(text);
}

} // namespace NSQLComplete
