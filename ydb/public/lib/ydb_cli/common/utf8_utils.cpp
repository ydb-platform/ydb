#include "utf8_utils.h"

#include <util/charset/utf8.h>

#include <algorithm>

namespace NYdb::NConsoleClient {

std::pair<size_t, size_t> WidenToUtf8CharBoundaries(TStringBuf text, size_t begin, size_t end) {
    const size_t size = text.size();
    begin = std::min(begin, size);
    end = std::min(end, size);
    begin = std::min(begin, end);

    // Move begin left to the lead byte of the character it is inside of.
    while (begin > 0 && begin < size && IsUTF8ContinuationByte(static_cast<unsigned char>(text[begin]))) {
        --begin;
    }
    // Move end right past the continuation bytes of a character straddling the boundary.
    while (end < size && IsUTF8ContinuationByte(static_cast<unsigned char>(text[end]))) {
        ++end;
    }
    return {begin, end};
}

} // namespace NYdb::NConsoleClient
