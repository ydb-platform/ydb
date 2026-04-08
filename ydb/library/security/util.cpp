#include "util.h"

#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/string/ascii.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/string/split.h>

#include <library/cpp/digest/crc32c/crc32c.h>

#include <algorithm>
#include <cctype>
#include <vector>

namespace NKikimr {

namespace {

struct TSensitivePairedWords {
    TStringBuf First;
    TStringBuf Second;
};

static const std::vector<TString> SensitiveWords = {
    "password",
};
static const TSensitivePairedWords SensitivePairedWords[] = {
    {"create", "secret"},
    {"alter", "secret"},
};

bool ContainsCaseInsensitive(const TString& text, TStringBuf pattern) {
    return std::search(text.begin(), text.end(), pattern.begin(), pattern.end(),
        [](char a, char b) { return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b)); }) != text.end();
}

bool MatchPrefixIgnoreCase(const TString& text, size_t pos, TStringBuf word) {
    if (pos + word.size() > text.size()) {
        return false;
    }
    for (size_t k = 0; k < word.size(); ++k) {
        if (std::tolower(static_cast<unsigned char>(text[pos + k])) !=
            std::tolower(static_cast<unsigned char>(word[k]))) {
            return false;
        }
    }
    return true;
}

// Two consecutive keywords; only ASCII whitespace between them; case-insensitive.
bool ContainsAdjacentWordsIgnoreSpaces(const TString& text, TStringBuf w1, TStringBuf w2) {
    for (size_t i = 0; i + w1.size() <= text.size(); ++i) {
        if (!MatchPrefixIgnoreCase(text, i, w1)) {
            continue;
        }
        size_t p = i + w1.size();
        while (p < text.size() && IsAsciiSpace(static_cast<unsigned char>(text[p]))) {
            ++p;
        }
        if (p + w2.size() <= text.size() && MatchPrefixIgnoreCase(text, p, w2)) {
            return true;
        }
    }
    return false;
}

TMaybe<TString> FindSensitiveQueryMarker(const TString& text) {
    for (const TString& word : SensitiveWords) {
        if (ContainsCaseInsensitive(text, word)) {
            return word;
        }
    }
    for (const auto& pair : SensitivePairedWords) {
        if (ContainsAdjacentWordsIgnoreSpaces(text, pair.First, pair.Second)) {
            return TStringBuilder() << pair.First << ' ' << pair.Second;
        }
    }
    return Nothing();
}

} // namespace

bool IsQueryWithSensitiveInfo(const TString& text) {
    return FindSensitiveQueryMarker(text).Defined();
}

TString ProtectQueryForLoggingIfSensitive(const TString& text) {
    if (const auto marker = FindSensitiveQueryMarker(text)) {
        return TStringBuilder() << "Query text is hidden due to a sensitive marker: " << *marker;
    }
    return text;
}

TString MaskTicket(TStringBuf token) {
    TStringBuilder mask;
    if (token.size() >= 16) {
        mask << token.substr(0, 4);
        mask << "****";
        mask << token.substr(token.size() - 4, 4);
    } else {
        mask << "****";
    }
    mask << " (";
    mask << Sprintf("%08X", Crc32c(token.data(), token.size()));
    mask << ")";
    return mask;
}

TString MaskTicket(const TString& token) {
    return MaskTicket(TStringBuf(token));
}

TString MaskIAMTicket(const TString& token) {
    static constexpr TStringBuf hiddenValue = "*** hidden ***";
    static constexpr TStringBuf id = "t1";

    if (token.empty()) {
        return "";
    }

    TVector<TString> parts;
    StringSplitter(token).Split('.').AddTo(&parts);
    parts.erase(
        std::remove_if(parts.begin(), parts.end(), 
                    [](const TString& value) { return value.empty(); }),
        parts.end()
    );

    if (parts.size() != 3 || parts[0] != id) {
        return TString(hiddenValue);
    }

    TStringBuilder mask;
    mask << parts[0];
    mask << '.';
    mask << parts[1];
    mask << ".**** (";
    mask << Sprintf("%08X", Crc32c(token.data(), token.size()));
    mask << ")";

    return mask;
}

namespace {

// Ticket is like ne1<token>.<signature>
// Finds pos of '.'
size_t FindNebiusTokenSignaturePos(const TString& token) {
    if (!token.StartsWith("ne1")) {
        return TString::npos;
    }
    size_t pos = token.find('.');
    if (pos == TString::npos) {
        return pos;
    }
    if (pos < token.size() - 1) { // '.' is not the last symbol
        return pos;
    }
    return TString::npos;
}

} // namespace

TString SanitizeNebiusTicket(const TString& token) {
    const size_t signaturePos = FindNebiusTokenSignaturePos(token);
    if (signaturePos == TString::npos) {
        return MaskTicket(token);
    }
    return TStringBuilder() << TStringBuf(token).SubString(0, signaturePos) << ".**"; // <token>.**
}

TString MaskNebiusTicket(const TString& token) {
    const size_t signaturePos = FindNebiusTokenSignaturePos(token);
    if (signaturePos == TString::npos) {
        return MaskTicket(token);
    }
    return MaskTicket(TStringBuf(token).SubString(0, signaturePos));
}

} // namespace NKikimr
