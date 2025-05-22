#pragma once

#include <util/generic/string.h>

namespace NSQLComplete {

    struct TCompletionInput {
        TStringBuf Text;
        size_t CursorPosition = Text.length();
    };

    struct TMaterializedInput {
        TString Text;
        size_t CursorPosition = Text.length();
    };

    TCompletionInput SharpedInput(TString& text Y_LIFETIME_BOUND);

} // namespace NSQLComplete
