#include "string_util.h"

#include <util/generic/strbuf.h>

namespace NSQLComplete {

    bool IsWordBoundary(char ch) { // Is optimized into table lookup by clang
        for (size_t i = 0; i < sizeof(WordBreakCharacters) - 1; ++i) {
            if (WordBreakCharacters[i] == ch) {
                return true;
            }
        }
        return false;
    }

    size_t LastWordIndex(TStringBuf text) {
        for (auto it = std::rbegin(text); it != std::rend(text); std::advance(it, 1)) {
            if (IsWordBoundary(*it)) {
                return std::distance(it, std::rend(text));
            }
        }
        return 0;
    }

    TStringBuf LastWord(TStringBuf text) {
        return text.SubStr(LastWordIndex(text));
    }

} // namespace NSQLComplete
