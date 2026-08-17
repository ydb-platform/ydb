#include "utf8_utils.h"

#include <util/charset/utf8.h>

#include <algorithm>

namespace NYdb::NConsoleClient {

namespace {

struct TTerminalCharacter {
    TStringBuf Bytes;
    size_t InputLength;
    bool IsWhitespace;
};

TTerminalCharacter ReadTerminalCharacter(const unsigned char* current, const unsigned char* end) {
    wchar32 rune = 0;
    size_t runeLength = 0;
    if (SafeReadUTF8Char<StrictUTF8::Yes>(rune, runeLength, current, end) != RECODE_OK) {
        return {"?", 1, false};
    }

    if (rune < 0x20 || (rune >= 0x7f && rune <= 0x9f)) {
        return {" ", runeLength, true};
    }

    return {
        TStringBuf(reinterpret_cast<const char*>(current), runeLength),
        runeLength,
        rune == ' ',
    };
}

} // anonymous namespace

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

TString SanitizeUtf8ForTerminal(TStringBuf text) {
    if (text.empty()) {
        return {};
    }

    TString sanitized;
    sanitized.reserve(text.size());

    const auto* current = reinterpret_cast<const unsigned char*>(text.data());
    const auto* end = current + text.size();
    while (current != end) {
        const auto character = ReadTerminalCharacter(current, end);
        sanitized.append(character.Bytes);
        current += character.InputLength;
    }

    return sanitized;
}

TString CompactUtf8ForTerminal(TStringBuf text, size_t maxCharacters) {
    if (text.empty() || !maxCharacters) {
        return {};
    }

    constexpr TStringBuf Ellipsis = "...";
    const size_t ellipsisLength = std::min(Ellipsis.size(), maxCharacters);
    const size_t charactersBeforeEllipsis = maxCharacters - ellipsisLength;

    TString compact;
    compact.reserve(std::min(text.size(), maxCharacters));
    size_t characterCount = 0;
    size_t ellipsisPosition = 0;

    const auto append = [&](TStringBuf character) {
        if (characterCount == maxCharacters) {
            compact.resize(ellipsisPosition);
            compact.append(Ellipsis.data(), ellipsisLength);
            return false;
        }
        if (characterCount == charactersBeforeEllipsis) {
            ellipsisPosition = compact.size();
        }
        compact.append(character);
        ++characterCount;
        return true;
    };

    bool afterWhitespace = false;
    const auto* current = reinterpret_cast<const unsigned char*>(text.data());
    const auto* end = current + text.size();
    while (current != end) {
        const auto character = ReadTerminalCharacter(current, end);
        current += character.InputLength;

        if (character.IsWhitespace) {
            afterWhitespace = !compact.empty();
            continue;
        }
        if (afterWhitespace && !append(" ")) {
            break;
        }
        afterWhitespace = false;
        if (!append(character.Bytes)) {
            break;
        }
    }

    return compact;
}

} // namespace NYdb::NConsoleClient
