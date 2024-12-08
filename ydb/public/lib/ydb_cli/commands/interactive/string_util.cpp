#include "string_util.h"

#include <util/generic/strbuf.h>

namespace NYdb {
    namespace NConsoleClient {

        bool IsWordBoundary(char ch) {
            return WordBreakCharacters.contains(ch);
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

    } // namespace NConsoleClient
} // namespace NYdb
