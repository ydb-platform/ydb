#pragma once

#include <util/generic/string.h>

namespace NSQLComplete {

    struct TCompletionInput {
        TStringBuf Text;
        size_t CursorPosition = Text.length();
    };

} // namespace NSQLComplete
