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
        const auto length = static_cast<int64_t>(text.size());
        for (int64_t i = length - 1; 0 <= i; --i) {
            if (IsWordBoundary(text[i])) {
                return i + 1;
            }
        }
        return 0;
    }

    TStringBuf LastWord(TStringBuf text) {
        return text.SubStr(LastWordIndex(text));
    }

} // namespace NSQLComplete
