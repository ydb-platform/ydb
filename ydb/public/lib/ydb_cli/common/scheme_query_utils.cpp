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

TStringBuf StripOuterWhitespace(TStringBuf text) {
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front()))) {
        text.Skip(1);
    }
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back()))) {
        text.Chop(1);
    }
    return text;
}

bool LooksLikeSchemeQueryStatement(TStringBuf statement) {
    statement = StripOuterWhitespace(statement);
    if (statement.empty()) {
        return false;
    }
    const auto tokens = TokenizeUpper(statement);
    const size_t i = SkipExplainPrefix(tokens);
    if (i >= tokens.size()) {
        return false;
    }
    if (tokens[i] == "SHOW" && i + 1 < tokens.size() && tokens[i + 1] == "CREATE") {
        return false;
    }
    return SchemeQueryLeadingKeywords().contains(tokens[i]);
}

bool IsShowCreateStatementPrefix(TStringBuf textBeforeCursor) {
    const size_t lastSemi = textBeforeCursor.rfind(';');
    if (lastSemi != TStringBuf::npos) {
        textBeforeCursor = textBeforeCursor.SubStr(lastSemi + 1);
    }
    textBeforeCursor = StripOuterWhitespace(textBeforeCursor);
    if (textBeforeCursor.empty()) {
        return false;
    }
    const auto tokens = TokenizeUpper(textBeforeCursor);
    const size_t i = SkipExplainPrefix(tokens);
    if (i >= tokens.size() || tokens[i] != "SHOW") {
        return false;
    }
    return i + 1 == tokens.size() || (i + 2 <= tokens.size() && tokens[i + 1] == "CREATE");
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
    TStringBuf rest = line;
    while (true) {
        const size_t semi = rest.find(';');
        const TStringBuf segment = semi == TStringBuf::npos ? rest : rest.SubStr(0, semi);
        if (LooksLikeSchemeQueryStatement(segment)) {
            return true;
        }
        if (semi == TStringBuf::npos) {
            break;
        }
        rest = rest.SubStr(semi + 1);
    }
    return false;
}

bool IsSchemeQueryCompletionContext(TStringBuf textBeforeCursor) {
    return LooksLikeSchemeQuery(GetCurrentStatementPrefix(textBeforeCursor));
}

bool IsExcludedSchemeQueryCompletionKeyword(TStringBuf keywordContent, TStringBuf textBeforeCursor) {
    if (IsShowCreateStatementPrefix(textBeforeCursor)) {
        return false;
    }
    if (IsSchemeQueryCompletionContext(textBeforeCursor)) {
        return true;
    }
    return SchemeQueryTopLevelCompletionKeywords().contains(ToUpperAscii(keywordContent));
}

} // namespace NYdb::NConsoleClient
