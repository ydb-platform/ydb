#include "scheme_query_utils.h"

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <cctype>

namespace NYdb::NConsoleClient {

namespace {

TVector<TString> TokenizeUpper(TStringBuf line) {
    TVector<TString> tokens;
    TString current;
    auto flush = [&]() {
        if (!current.empty()) {
            tokens.push_back(std::move(current));
            current.clear();
        }
    };
    for (char c : line) {
        const unsigned char uc = static_cast<unsigned char>(c);
        if (std::isspace(uc) || c == ';') {
            flush();
        } else {
            current.push_back(static_cast<char>(std::toupper(uc)));
        }
    }
    flush();
    return tokens;
}

const THashSet<TString>& SchemeQueryLeadingKeywords() {
    static const THashSet<TString> kKeywords = {
        "ALTER",
        "ANALYZE",
        "BACKUP",
        "CREATE",
        "DISCARD",
        "DROP",
        "EXPORT",
        "GRANT",
        "IMPORT",
        "RESTORE",
        "REVOKE",
        "SHOW",
        "TRUNCATE",
        "USE",
    };
    return kKeywords;
}

size_t SkipExplainPrefix(const TVector<TString>& tokens) {
    size_t i = 0;
    if (i < tokens.size() && tokens[i] == "EXPLAIN") {
        ++i;
        if (i < tokens.size() && tokens[i] == "QUERY") {
            ++i;
            if (i < tokens.size() && tokens[i] == "PLAN") {
                ++i;
            }
        }
    }
    return i;
}

const THashSet<TString>& SchemeQueryTopLevelCompletionKeywords() {
    static const THashSet<TString> kKeywords = {
        "ALTER",
        "ANALYZE",
        "BACKUP",
        "CREATE",
        "DISCARD",
        "DROP",
        "EXPORT",
        "GRANT",
        "IMPORT",
        "RESTORE",
        "REVOKE",
        "SHOW CREATE",
        "TRUNCATE TABLE",
        "USE",
    };
    return kKeywords;
}

TString ToUpperAscii(TStringBuf text) {
    TString result{text};
    for (char& c : result) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return result;
}

} // anonymous namespace

TStringBuf GetCurrentStatementPrefix(TStringBuf textBeforeCursor) {
    const size_t lastSemi = textBeforeCursor.rfind(';');
    if (lastSemi == TStringBuf::npos) {
        return textBeforeCursor;
    }
    return textBeforeCursor.SubStr(lastSemi + 1);
}

bool LooksLikeSchemeQuery(TStringBuf line) {
    const auto tokens = TokenizeUpper(line);
    const size_t i = SkipExplainPrefix(tokens);
    if (i >= tokens.size()) {
        return false;
    }
    return SchemeQueryLeadingKeywords().contains(tokens[i]);
}

bool IsSchemeQueryCompletionContext(TStringBuf textBeforeCursor) {
    return LooksLikeSchemeQuery(GetCurrentStatementPrefix(textBeforeCursor));
}

bool IsExcludedSchemeQueryCompletionKeyword(TStringBuf keywordContent, TStringBuf textBeforeCursor) {
    if (IsSchemeQueryCompletionContext(textBeforeCursor)) {
        return true;
    }
    return SchemeQueryTopLevelCompletionKeywords().contains(ToUpperAscii(keywordContent));
}

} // namespace NYdb::NConsoleClient
