#pragma once

#include <util/generic/strbuf.h>

#include <utility>

namespace NYdb::NConsoleClient {

// Widen the half-open byte range [begin, end) of `text` outward to UTF-8 character boundaries:
// `begin` is moved left to the lead byte of the character it lands inside, and `end` is moved
// right past the continuation bytes of a character straddling it. This guarantees a substring
// taken over the returned range never splits a multibyte character (which would produce invalid
// UTF-8). Offsets are clamped to [0, text.size()]; `text` is assumed to be valid UTF-8.
std::pair<size_t, size_t> WidenToUtf8CharBoundaries(TStringBuf text, size_t begin, size_t end);

} // namespace NYdb::NConsoleClient
