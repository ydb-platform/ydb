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

using TWordSequence = std::vector<TStringBuf>;

static const std::vector<TString> SensitiveWords = {
    "password",
};
// Each sequence is a list of keywords that must appear consecutively (only ASCII
// whitespace allowed between neighboring words, any amount of it, including
// newlines/tabs/multiple spaces).
// The first matching sequence is reported as the marker, so the plain verb pairs
// come first: they never match the variants with keywords in between.
static const std::vector<TWordSequence> SensitiveWordSequences = {
    {"create", "secret"},
    {"alter", "secret"},
    {"create", "or", "replace", "secret"},
    {"create", "if", "not", "exists", "secret"},
    {"alter", "if", "exists", "secret"},
};

bool ContainsCaseInsensitive(TStringBuf text, TStringBuf pattern) {
    return std::search(text.begin(), text.end(), pattern.begin(), pattern.end(),
        [](char a, char b) { return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b)); }) != text.end();
}

bool MatchPrefixIgnoreCase(TStringBuf text, size_t pos, TStringBuf word) {
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

// A sequence of consecutive keywords; only ASCII whitespace (any amount) is
// allowed between neighboring words; case-insensitive.
bool ContainsWordSequenceIgnoreSpaces(TStringBuf text, const TWordSequence& words) {
    if (words.empty()) {
        return false;
    }
    const TStringBuf& first = words.front();
    for (size_t i = 0; i + first.size() <= text.size(); ++i) {
        if (!MatchPrefixIgnoreCase(text, i, first)) {
            continue;
        }
        size_t p = i + first.size();
        bool matched = true;
        for (size_t w = 1; w < words.size(); ++w) {
            size_t spaceStart = p;
            while (p < text.size() && IsAsciiSpace(static_cast<unsigned char>(text[p]))) {
                ++p;
            }
            if (p == spaceStart) {
                matched = false;
                break;
            }
            const TStringBuf& word = words[w];
            if (p + word.size() > text.size() || !MatchPrefixIgnoreCase(text, p, word)) {
                matched = false;
                break;
            }
            p += word.size();
        }
        if (matched) {
            return true;
        }
    }
    return false;
}

TMaybe<TString> FindSensitiveQueryMarker(TStringBuf text) {
    for (const TString& word : SensitiveWords) {
        if (ContainsCaseInsensitive(text, word)) {
            return word;
        }
    }
    for (const auto& seq : SensitiveWordSequences) {
        if (ContainsWordSequenceIgnoreSpaces(text, seq)) {
            TStringBuilder marker;
            for (size_t i = 0; i < seq.size(); ++i) {
                if (i > 0) {
                    marker << ' ';
                }
                marker << seq[i];
            }
            return TString(marker);
        }
    }
    return Nothing();
}

} // namespace

bool IsQueryWithSensitiveInfo(TStringBuf text) {
    return FindSensitiveQueryMarker(text).Defined();
}

TString ProtectQueryForLoggingIfSensitive(const TString& text) {
    TString protectedText;
    if (ProtectQueryForLoggingIfSensitive(TStringBuf(text), protectedText)) {
        return protectedText;
    }
    return text;
}

bool ProtectQueryForLoggingIfSensitive(TStringBuf text, TString& protectedText) {
    if (const auto marker = FindSensitiveQueryMarker(text)) {
        protectedText = TStringBuilder() << "Query text is hidden due to a sensitive marker: " << *marker;
        return true;
    }
    protectedText.clear();
    return false;
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

// Same as the default branch of TTokenRecordBase::GetSanitizedTicket() for opaque tokens.
TString SanitizeTicket(TStringBuf token) {
    return MaskTicket(token);
}

TString SanitizeTicket(const TString& token) {
    return SanitizeTicket(TStringBuf(token));
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
