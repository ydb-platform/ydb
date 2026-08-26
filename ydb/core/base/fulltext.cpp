#include "fulltext.h"
#include "fulltext_query.h"

#include <contrib/libs/snowball/include/libstemmer.h>

#include <util/charset/unidata.h>
#include <util/charset/utf8.h>
#include <util/generic/xrange.h>

#include <algorithm>

namespace NKikimr::NFulltext {

namespace {

    bool ValidateSettingInRange(const TString& name, i32 value, i32 minValue, i32 maxValue, TString& error) {
        if (minValue <= value && value <= maxValue) {
            return true;
        }

        error = TStringBuilder() << "Invalid " << name << ": " << value << " should be between " << minValue << " and " << maxValue;
        return false;
    };

    Ydb::Table::FulltextIndexSettings::Tokenizer ParseTokenizer(const TString& tokenizer_, TString& error) {
        const TString tokenizer = to_lower(tokenizer_);
        if (tokenizer == "whitespace")
            return Ydb::Table::FulltextIndexSettings::WHITESPACE;
        else if (tokenizer == "standard")
            return Ydb::Table::FulltextIndexSettings::STANDARD;
        else if (tokenizer == "alphanumeric")
            return Ydb::Table::FulltextIndexSettings::ALPHANUMERIC;
        else if (tokenizer == "keyword")
            return Ydb::Table::FulltextIndexSettings::KEYWORD;
        else {
            error = TStringBuilder() << "Invalid tokenizer: " << tokenizer_;
            return Ydb::Table::FulltextIndexSettings::TOKENIZER_UNSPECIFIED;
        }
    };

    i32 ParseInt32(const TString& name, const TString& value, TString& error) {
        i32 result = 0;
        if (!TryFromString(value, result) || result < 0) { // proto int32 fields with [(Ydb.value) = ">= 0"] annotation
            error = TStringBuilder() << "Invalid " << name << ": " << value;
        }
        return result;
    }

    bool ParseBool(const TString& name, const TString& value, TString& error) {
        bool result = false;
        if (!TryFromString(value, result)) {
            error = TStringBuilder() << "Invalid " << name << ": " << value;
        }
        return result;
    }

    inline bool IsNonStandard(wchar32 c) {
        return !IsAlphabetic(c) && !IsDecdigit(c);
    }

    void Tokenize(const TStringBuf text, TVector<TString>& tokens, auto isDelimiter) {
        const unsigned char* ptr = (const unsigned char*)text.data();
        const unsigned char* end = ptr + text.size();

        while (ptr < end) {
            wchar32 symbol;
            size_t symbolBytes = 0;

            while (ptr < end) { // skip delimiters
                if (SafeReadUTF8Char(symbol, symbolBytes, ptr, end) != RECODE_OK) {
                    tokens.clear();
                    return;
                }
                if (!isDelimiter(symbol)) {
                    break;
                }
                ptr += symbolBytes;
            }
            if (ptr >= end) {
                break;
            }

            const unsigned char* tokenPtr = ptr;
            while (ptr < end) { // read token
                if (SafeReadUTF8Char(symbol, symbolBytes, ptr, end) != RECODE_OK) {
                    tokens.clear();
                    return;
                }
                if (isDelimiter(symbol)) {
                    break;
                }
                ptr += symbolBytes;
            }
            tokens.emplace_back((const char*)tokenPtr, ptr - tokenPtr);
        }
    }

    // --- Lucene StandardTokenizer character classification (UAX#29-based) ---

    inline bool IsHangulScript(wchar32 c) {
        return IsHangulLeading(c) || IsHangulVowel(c) || IsHangulTrailing(c)
            || (c >= 0xAC00 && c <= 0xD7A3)     // Hangul Syllables
            || (c >= 0x3130 && c <= 0x318F)     // Hangul Compatibility Jamo
            || (c >= 0xA960 && c <= 0xA97F)     // Hangul Jamo Extended-A
            || (c >= 0xD7B0 && c <= 0xD7FF);    // Hangul Jamo Extended-B
    }

    inline bool IsSouthEastAsian(wchar32 c) {
        // LB:Complex_Context — Thai, Lao, Myanmar, Khmer
        return (c >= 0x0E01 && c <= 0x0E5B)     // Thai
            || (c >= 0x0E81 && c <= 0x0EDF)     // Lao
            || (c >= 0x1000 && c <= 0x109F)     // Myanmar
            || (c >= 0x1780 && c <= 0x17FF)     // Khmer
            || (c >= 0x19E0 && c <= 0x19FF)     // Khmer Symbols
            || (c >= 0xAA60 && c <= 0xAA7F)     // Myanmar Extended-A
            || (c >= 0xA9E0 && c <= 0xA9FF);    // Myanmar Extended-B
    }

    inline bool IsExtendNumLet(wchar32 c) {
        // WB:ExtendNumLet — Pc (Connector_Punctuation) category
        return c == '_'
            || c == 0x203F || c == 0x2040 || c == 0x2054
            || c == 0xFE33 || c == 0xFE34
            || (c >= 0xFE4D && c <= 0xFE4F)
            || c == 0xFF3F;
    }

    inline bool IsExtendOrFormat(wchar32 c) {
        // WB:Extend + WB:Format + ZWJ — transparent inside tokens
        return IsJoinCntrl(c) || IsCombining(c) || IsFormatCntrl(c) || c == 0x200D;
    }

    inline bool IsMidLetterChar(wchar32 c) {
        // WB:MidLetter ∪ WB:MidNumLet ∪ WB:SingleQuote (valid between letters)
        switch (c) {
            case 0x0027: // APOSTROPHE
            case 0x002E: // FULL STOP
            case 0x003A: // COLON
            case 0x00B7: // MIDDLE DOT
            case 0x0387: // GREEK ANO TELEIA
            case 0x05F4: // HEBREW PUNCTUATION GERSHAYIM
            case 0x2018: // LEFT SINGLE QUOTATION MARK
            case 0x2019: // RIGHT SINGLE QUOTATION MARK
            case 0x2024: // ONE DOT LEADER
            case 0x2027: // HYPHENATION POINT
            case 0xFE13: // PRESENTATION FORM FOR VERTICAL COLON
            case 0xFE52: // SMALL FULL STOP
            case 0xFE55: // SMALL COLON
            case 0xFF07: // FULLWIDTH APOSTROPHE
            case 0xFF0E: // FULLWIDTH FULL STOP
            case 0xFF1A: // FULLWIDTH COLON
                return true;
            default:
                return false;
        }
    }

    inline bool IsMidNumberChar(wchar32 c) {
        // WB:MidNum ∪ WB:MidNumLet ∪ WB:SingleQuote (valid between digits)
        switch (c) {
            case 0x0027: // APOSTROPHE
            case 0x002C: // COMMA
            case 0x002E: // FULL STOP
            case 0x003B: // SEMICOLON
            case 0x037E: // GREEK QUESTION MARK
            case 0x0589: // ARMENIAN FULL STOP
            case 0x060C: // ARABIC COMMA
            case 0x060D: // ARABIC DATE SEPARATOR
            case 0x066C: // ARABIC THOUSANDS SEPARATOR
            case 0x07F8: // NKO COMMA
            case 0x2018: // LEFT SINGLE QUOTATION MARK
            case 0x2019: // RIGHT SINGLE QUOTATION MARK
            case 0x2024: // ONE DOT LEADER
            case 0x2044: // FRACTION SLASH
            case 0xFE10: // PRESENTATION FORM FOR VERTICAL COMMA
            case 0xFE50: // SMALL COMMA
            case 0xFE52: // SMALL FULL STOP
            case 0xFF07: // FULLWIDTH APOSTROPHE
            case 0xFF0C: // FULLWIDTH COMMA
            case 0xFF0E: // FULLWIDTH FULL STOP
            case 0xFF1B: // FULLWIDTH SEMICOLON
                return true;
            default:
                return false;
        }
    }

    inline bool IsHebrew(wchar32 c) {
        return c >= 0x0590 && c <= 0x05FF; // Primary Hebrew Block
    }

    inline bool IsKatakanaOrSymbol(wchar32 c) {
        return IsKatakana(c) ||
            c >= 0x3000 && c <= 0x303F; // CJK Symbols and Punctuation
    }

    // Alphabetic letter excluding script-specific types handled separately
    inline bool IsALetter(wchar32 c) {
        return IsAlphabetic(c)
            && !IsKatakanaOrSymbol(c) && !IsHiragana(c) && !IsIdeographic(c)
            && !IsHangulScript(c) && !IsSouthEastAsian(c);
    }

    // Simplified Lucene StandardTokenizer (UAX#29 word break rules).
    // Token types: ALPHANUM (letters+digits with mid-connectors), NUM (digits),
    //   KATAKANA (sequences), HANGUL (sequences), IDEOGRAPHIC (single),
    //   HIRAGANA (single), SOUTHEAST_ASIAN (sequences).
    // Emoji sequences are not tokenized (skipped).
    void TokenizeStandard(const TStringBuf text, TVector<TString>& tokens, const std::unordered_set<wchar32>& ignoredDelimiter) {
        const ui8* p = (const ui8*)text.data();
        const ui8* end = p + text.size();

        auto tryRead = [&](const ui8* at, wchar32& c, size_t& n) -> bool {
            if (at >= end) {
                return false;
            }
            return SafeReadUTF8Char(c, n, at, end) == RECODE_OK;
        };

        auto isLetter = [&](wchar32 c) {
            return IsALetter(c) || !ignoredDelimiter.empty() && ignoredDelimiter.contains(c);
        };

        wchar32 c;
        size_t n;

        auto trySingleChar = [&](auto check) {
            if (check(c)) {
                const ui8* s = p;
                p += n;
                while (tryRead(p, c, n) && IsExtendOrFormat(c)) {
                    p += n;
                }
                tokens.emplace_back((const char*)s, p - s);
                return true;
            }
            return false;
        };

        auto tryMultiChar = [&](auto check) {
            if (check(c)) {
                const ui8* s = p;
                p += n;
                while (tryRead(p, c, n) && (check(c) || IsExtendOrFormat(c))) {
                    p += n;
                }
                tokens.emplace_back((const char*)s, p - s);
                return true;
            }
            return false;
        };

        while (p < end) {
            if (!tryRead(p, c, n)) {
                tokens.clear();
                return;
            }
            if (IsExtendOrFormat(c)) {
                p += n;
                continue;
            }

            // Alphanumeric token (with mid-letter/mid-number connectors)
            if (isLetter(c) || IsDecdigit(c) || IsExtendNumLet(c)) {
                enum EPrev { LETTER, DIGIT, NEITHER, MID_LETTER, MID_DIGIT, HEBREW };
                const ui8* s = p;
                EPrev prev = IsHebrew(c) ? HEBREW : isLetter(c) ? LETTER : IsDecdigit(c) ? DIGIT : NEITHER;
                bool hasContent = (prev != NEITHER);
                bool joined = false;
                p += n;
                const ui8* safeEnd = p;

                while (p < end) {
                    if (!tryRead(p, c, n)) {
                        tokens.clear();
                        return;
                    }
                    if (IsExtendOrFormat(c)) {
                        p += n;
                        safeEnd = p;
                        joined = c == 0x200D; // ZWJ
                        continue;
                    }
                    if (isLetter(c)) {
                        p += n;
                        prev = IsHebrew(c) ? HEBREW : LETTER;
                        safeEnd = p;
                        hasContent = true;
                    } else if (prev == HEBREW && c == '\'') {
                        p += n;
                        safeEnd = p;
                        prev = LETTER;
                    } else if (prev == HEBREW && c == '\"') {
                        size_t n2;
                        if (tryRead(p + n, c, n2) && IsHebrew(c)) {
                            p += n + n2;
                            safeEnd = p;
                        }
                        prev = LETTER;
                    } else if (IsDecdigit(c)) {
                        p += n;
                        prev = DIGIT;
                        safeEnd = p;
                        hasContent = true;
                    } else if (IsExtendNumLet(c)) {
                        p += n;
                        safeEnd = p;
                        if (prev == DIGIT) {
                            prev = MID_DIGIT;
                        } else if (prev == LETTER) {
                            prev = MID_LETTER;
                        }
                    } else if (prev != HEBREW && prev != LETTER && prev != DIGIT && IsKatakanaOrSymbol(c)) {
                        p += n;
                        safeEnd = p;
                        prev = NEITHER;
                        hasContent = true;
                    } else if ((prev == LETTER || prev == MID_LETTER) && IsMidLetterChar(c)) {
                        // Lookahead past connector + extend/format chars
                        const ui8* q = p + n;
                        wchar32 c2;
                        size_t n2;
                        while (tryRead(q, c2, n2) && IsExtendOrFormat(c2)) {
                            q += n2;
                        }
                        if (tryRead(q, c2, n2) && isLetter(c2)) {
                            p = q + n2;
                            prev = LETTER;
                            safeEnd = p;
                        } else {
                            break;
                        }
                    } else if ((prev == DIGIT || prev == MID_DIGIT) && IsMidNumberChar(c)) {
                        const ui8* q = p + n;
                        wchar32 c2;
                        size_t n2;
                        while (tryRead(q, c2, n2) && IsExtendOrFormat(c2)) {
                            q += n2;
                        }
                        if (tryRead(q, c2, n2) && IsDecdigit(c2)) {
                            p = q + n2;
                            prev = DIGIT;
                            safeEnd = p;
                        } else {
                            break;
                        }
                    } else if (joined && !IsWhitespace(c)) {
                        p += n;
                        safeEnd = p;
                    } else {
                        break;
                    }
                    joined = false;
                }

                if (hasContent) {
                    tokens.emplace_back((const char*)s, safeEnd - s);
                }
                p = safeEnd;
                continue;
            }

            // Single-character token types
            if (trySingleChar(IsIdeographic)) {
                continue;
            }
            if (trySingleChar(IsHiragana)) {
                continue;
            }

            // Sequence token types
            if (IsKatakanaOrSymbol(c)) {
                const ui8* s = p;
                p += n;
                while (tryRead(p, c, n) && (IsKatakanaOrSymbol(c) || IsExtendOrFormat(c) || IsExtendNumLet(c))) {
                    p += n;
                }
                tokens.emplace_back((const char*)s, p - s);
                continue;
            }
            if (tryMultiChar(IsHangulScript)) {
                continue;
            }
            if (tryMultiChar(IsSouthEastAsian)) {
                continue;
            }

            // Other characters (whitespace, punctuation, emoji, etc.) — skip
            p += n;
        }
    }

    TVector<TString> Tokenize(const TStringBuf text, const Ydb::Table::FulltextIndexSettings::Tokenizer& tokenizer, const std::unordered_set<wchar32> ignoredDelimiter) {
        TVector<TString> tokens;
        switch (tokenizer) {
            case Ydb::Table::FulltextIndexSettings::WHITESPACE:
                Tokenize(text, tokens, [&ignoredDelimiter](const wchar32 c) {
                    return !ignoredDelimiter.empty() && ignoredDelimiter.contains(c)
                        ? false
                        : IsWhitespace(c);
                });
                break;
            case Ydb::Table::FulltextIndexSettings::ALPHANUMERIC:
                Tokenize(text, tokens, [&ignoredDelimiter](const wchar32 c) {
                    return !ignoredDelimiter.empty() && ignoredDelimiter.contains(c)
                        ? false
                        : IsNonStandard(c);
                });
                break;
            case Ydb::Table::FulltextIndexSettings::STANDARD:
                TokenizeStandard(text, tokens, ignoredDelimiter);
                break;
            case Ydb::Table::FulltextIndexSettings::KEYWORD:
                if (UTF8Detect(text) != NotUTF8) {
                    tokens.push_back(TString(text));
                }
                break;
            default:
                Y_ENSURE(false, TStringBuilder() << "Invalid tokenizer: " << static_cast<int>(tokenizer));
        }
        return tokens;
    }

    size_t GetLengthUTF8(const TString& token) {
        const unsigned char* ptr = (const unsigned char*)token.data();
        const unsigned char* end = ptr + token.size();
        size_t length = 0;
        wchar32 symbol;
        size_t symbolBytes = 0;

        while (ptr < end) {
            if (SafeReadUTF8Char(symbol, symbolBytes, ptr, end) != RECODE_OK) {
                Y_ASSERT(false); // should be dropped during tokenize
                return 0;
            }
            length++;
            ptr += symbolBytes;
        }

        return length;
    }

    bool ValidateSettings(const Ydb::Table::FulltextIndexSettings::Analyzers& settings, TString& error) {
        if (!settings.has_tokenizer() || settings.tokenizer() == Ydb::Table::FulltextIndexSettings::TOKENIZER_UNSPECIFIED) {
            error = "tokenizer should be set";
            return false;
        }

        if (settings.use_filter_snowball()) {
            if (settings.use_filter_ngram() || settings.use_filter_edge_ngram()) {
                error = "cannot set use_filter_snowball with use_filter_ngram or use_filter_edge_ngram at the same time";
                return false;
            }

            if (!settings.has_language()) {
                error = "language required when use_filter_snowball is set";
                return false;
            }

            bool supportedLanguage = false;
            for (auto ptr = sb_stemmer_list(); *ptr != nullptr; ++ptr) {
                if (settings.language() == *ptr) {
                    supportedLanguage = true;
                    break;
                }
            }
            if (!supportedLanguage) {
                error = "language is not supported by snowball";
                return false;
            }
        } else if (settings.has_language()) {
            // Currently, language is only used for stemming (use_filter_snowball).
            // In the future, it may be used for other language-sensitive operations (e.g., stopword filtering).
            error = "language setting is only supported with use_filter_snowball at present; other uses may be supported in the future";
            return false;
        }

        if (settings.use_filter_stopwords()) {
            error = "Unsupported use_filter_stopwords setting";
            return false;
        }

        if (settings.use_filter_ngram() || settings.use_filter_edge_ngram()) {
            if (settings.use_filter_ngram() && settings.use_filter_edge_ngram()) {
                error = "only one of use_filter_ngram or use_filter_edge_ngram should be set, not both";
                return false;
            }
            if (!settings.has_filter_ngram_min_length()) {
                error = "filter_ngram_min_length should be set with use_filter_ngram/use_filter_edge_ngram";
                return false;
            }
            if (!settings.has_filter_ngram_max_length()) {
                error = "filter_ngram_max_length should be set with use_filter_ngram/use_filter_edge_ngram";
                return false;
            }
            if (!ValidateSettingInRange("filter_ngram_min_length", settings.filter_ngram_min_length(), 1, 20, error)) {
                return false;
            }
            if (!ValidateSettingInRange("filter_ngram_max_length", settings.filter_ngram_max_length(), 1, 20, error)) {
                return false;
            }
            if (settings.filter_ngram_min_length() > settings.filter_ngram_max_length()) {
                error = "Invalid filter_ngram_min_length: should be less than or equal to filter_ngram_max_length";
                return false;
            }
        } else {
            if (settings.has_filter_ngram_min_length()) {
                error = "use_filter_ngram or use_filter_edge_ngram should be set with filter_ngram_min_length";
                return false;
            }
            if (settings.has_filter_ngram_max_length()) {
                error = "use_filter_ngram or use_filter_edge_ngram should be set with filter_ngram_max_length";
                return false;
            }
        }

        if (settings.use_filter_length()) {
            if (!settings.has_filter_length_min() && !settings.has_filter_length_max()) {
                error = "either filter_length_min or filter_length_max should be set with use_filter_length";
                return false;
            }
            if (settings.has_filter_length_min() && !ValidateSettingInRange("filter_length_min", settings.filter_length_min(), 1, 1000, error)) {
                return false;
            }
            if (settings.has_filter_length_max() && !ValidateSettingInRange("filter_length_max", settings.filter_length_max(), 1, 1000, error)) {
                return false;
            }
            if (settings.has_filter_length_min() && settings.has_filter_length_max() && settings.filter_length_min() > settings.filter_length_max()) {
                error = "Invalid filter_length_min: should be less than or equal to filter_length_max";
                return false;
            }
        } else {
            if (settings.has_filter_length_min()) {
                error = "use_filter_length should be set with filter_length_min";
                return false;
            }
            if (settings.has_filter_length_max()) {
                error = "use_filter_length should be set with filter_length_max";
                return false;
            }
        }

        return true;
    }
}

void BuildNgrams(const TString& token, size_t lengthMin, size_t lengthMax, bool edge, TVector<TString>& ngrams) {
    const unsigned char* ngram_begin_ptr = (const unsigned char*)token.data();
    const unsigned char* end = ngram_begin_ptr + token.size();
    wchar32 symbol;
    size_t symbolBytes;

    while (ngram_begin_ptr < end) {
        const unsigned char* ngram_end_ptr = ngram_begin_ptr;
        size_t ngram_length = 0;
        while (ngram_end_ptr < end) {
            if (SafeReadUTF8Char(symbol, symbolBytes, ngram_end_ptr, end) != RECODE_OK) {
                Y_ASSERT(false); // should already be validated during tokenization
                return;
            }
            ngram_length++;
            ngram_end_ptr += symbolBytes;
            if (lengthMin <= ngram_length && ngram_length <= lengthMax) {
                ngrams.emplace_back((const char*)ngram_begin_ptr, ngram_end_ptr - ngram_begin_ptr);
            }
        }
        if (edge) {
            break; // only prefixes
        }
        if (SafeReadUTF8Char(symbol, symbolBytes, ngram_begin_ptr, end) != RECODE_OK) {
            Y_ASSERT(false); // should already be validated during tokenization
            return;
        }
        ngram_begin_ptr += symbolBytes;
    }
}

Ydb::Table::FulltextIndexSettings::Analyzers GetAnalyzersForQuery(Ydb::Table::FulltextIndexSettings::Analyzers analyzers) {
    // Prevent splitting tokens into ngrams
    analyzers.set_use_filter_ngram(false);
    analyzers.set_use_filter_edge_ngram(false);
    // Prevent dropping patterns by length
    analyzers.set_use_filter_length(false);

    return analyzers;
}

TVector<TString> Analyze(const TStringBuf text, const Ydb::Table::FulltextIndexSettings::Analyzers& settings, const std::unordered_set<wchar32>& ignoredDelimiters) {
    TVector<TString> tokens = Tokenize(text, settings.tokenizer(), ignoredDelimiters);

    if (settings.use_filter_lowercase()) {
        for (auto i : xrange(tokens.size())) {
            tokens[i] = ToLowerUTF8(tokens[i]);
        }
    }

    if (settings.use_filter_length() && (settings.has_filter_length_min() || settings.has_filter_length_max())) {
        tokens.erase(std::remove_if(tokens.begin(), tokens.end(), [&](const TString& token){
            auto length = GetLengthUTF8(token);
            if (settings.has_filter_length_min() && length < static_cast<size_t>(settings.filter_length_min())) {
                return true;
            }
            if (settings.has_filter_length_max() && length > static_cast<size_t>(settings.filter_length_max())) {
                return true;
            }
            return false;
        }), tokens.end());
    }

    if (settings.use_filter_snowball()) {
        struct sb_stemmer* stemmer = sb_stemmer_new(settings.language().c_str(), nullptr);
        if (Y_UNLIKELY(stemmer == nullptr)) {
            ythrow yexception() << "sb_stemmer_new returned nullptr";
        }
        Y_DEFER { sb_stemmer_delete(stemmer); };

        for (auto& token : tokens) {
            const sb_symbol* stemmed = sb_stemmer_stem(
                stemmer,
                reinterpret_cast<const sb_symbol*>(token.data()),
                token.size()
            );
            if (Y_UNLIKELY(stemmed == nullptr)) {
                ythrow yexception() << "unable to allocate memory for sb_stemmer_stem result";
            }

            const size_t resultLength = sb_stemmer_length(stemmer);
            token = std::string(reinterpret_cast<const char*>(stemmed), resultLength);
        }
    }

    if (settings.use_filter_ngram() || settings.use_filter_edge_ngram()) {
        TVector<TString> ngrams;
        for (const auto& token : tokens) {
            BuildNgrams(token, settings.filter_ngram_min_length(), settings.filter_ngram_max_length(), settings.use_filter_edge_ngram(), ngrams);
        }
        tokens.swap(ngrams);
    }

    return tokens;
}

TVector<TString> BuildSearchTerms(const TString& query, const Ydb::Table::FulltextIndexSettings::Analyzers& settings) {
    const bool expectWildcard = settings.use_filter_ngram() || settings.use_filter_edge_ngram();
    const bool edge = settings.use_filter_edge_ngram();

    if (!expectWildcard) {
        return Analyze(query, settings);
    }

    const Ydb::Table::FulltextIndexSettings::Analyzers analyzersForQuery = GetAnalyzersForQuery(settings);

    TVector<TString> searchTerms;
    for (const TString& pattern : Analyze(query, analyzersForQuery, std::unordered_set<wchar32>{'%', '_'})) {
        for (const auto& term : StringSplitter(pattern).SplitBySet("%_")) {
            const TString token(term.Token());
            const i64 tokenLength = GetLengthUTF8(token);

            if (tokenLength != 0 && analyzersForQuery.filter_ngram_min_length() <= tokenLength) {
                const size_t upper = std::min(static_cast<i64>(analyzersForQuery.filter_ngram_max_length()), tokenLength);
                BuildNgrams(token, upper, upper, edge, searchTerms);
            }

            if (edge) {
                break;
            }
        }
    }
    return searchTerms;
}

namespace {
    // True if the query uses the `+term` required-term syntax: a `+` that begins
    // the query or follows ASCII whitespace (i.e. starts a term). A `+` inside a
    // term (e.g. "c++") is not an operator and does not trigger the per-term path.
    bool HasRequiredOperator(const TString& query) {
        bool atTermStart = true;
        for (const char c : query) {
            const bool ws = (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v');
            if (ws) {
                atTermStart = true;
                continue;
            }
            if (c == '+' && atTermStart) {
                return true;
            }
            atTermStart = false;
        }
        return false;
    }
}

TVector<TSearchTerm> BuildSearchTermsStructured(const TString& query, const Ydb::Table::FulltextIndexSettings::Analyzers& settings) {
    // Fast path: no `+term` syntax -> tokenize exactly like BuildSearchTerms over
    // the whole query, every term optional. Keeps full backward compatibility
    // (e.g. keyword-tokenizer queries stay a single token).
    if (!HasRequiredOperator(query)) {
        TVector<TSearchTerm> result;
        for (auto& token : BuildSearchTerms(query, settings)) {
            result.push_back({std::move(token), /* required */ false});
        }
        return result;
    }

    // Query-parser layer: split into whitespace-delimited raw terms, strip a single
    // leading `+` (required), then run the analyzer over each term body and tag every
    // resulting token with the term's required flag.
    TVector<TSearchTerm> result;
    for (const auto& part : StringSplitter(query).SplitBySet(" \t\n\r\f\v").SkipEmpty()) {
        TStringBuf raw = part.Token();
        bool required = false;
        if (raw.StartsWith('+')) {
            required = true;
            raw.Skip(1);
        }
        if (raw.empty()) {
            continue; // bare `+`
        }
        for (auto& token : BuildSearchTerms(TString(raw), settings)) {
            result.push_back({std::move(token), required});
        }
    }
    return result;
}

bool ValidateColumnsMatches(const NProtoBuf::RepeatedPtrField<TString>& columns, const Ydb::Table::FulltextIndexSettings& settings, TString& error) {
    return ValidateColumnsMatches(TVector<TString>{columns.begin(), columns.end()}, settings, error);
}

bool ValidateColumnsMatches(const TVector<TString>& columns, const Ydb::Table::FulltextIndexSettings& settings, TString& error) {
    TVector<TString> settingsColumns(::Reserve(settings.columns().size()));
    for (auto column : settings.columns()) {
        settingsColumns.push_back(column.column());
    }

    // The indexed (text) columns must be the suffix of the index key columns;
    // any leading key columns are prefix columns.
    if (settingsColumns.size() > columns.size() ||
        !std::equal(settingsColumns.begin(), settingsColumns.end(), columns.end() - settingsColumns.size()))
    {
        error = TStringBuilder() << "indexed columns " << settingsColumns << " should be the suffix of index columns " << columns;
        return false;
    }

    error = "";
    return true;
}

bool ValidateSettings(const Ydb::Table::FulltextIndexSettings& settings, TString& error) {
    // layout is set automatically based on index type (fulltext_plain vs fulltext_relevance)

    if (settings.columns().empty()) {
        error = "columns should be set";
        return false;
    }

    // current implementation limitation:
    if (settings.columns().size() != 1) {
        error = "columns should have a single value";
        return false;
    }

    for (auto column : settings.columns()) {
        if (!column.has_column()) {
            error = "column name should be set";
            return false;
        }

        // current implementation limitation:
        if (!settings.columns().at(0).has_analyzers()) {
            error = "column analyzers should be set";
            return false;
        }
        if (!ValidateSettings(column.analyzers(), error)) {
            return false;
        }
    }

    error = "";
    return true;
}

bool FillSetting(Ydb::Table::FulltextIndexSettings& settings, const TString& nameLower, const TString& value, TString& error) {
    error = "";

    Ydb::Table::FulltextIndexSettings::Analyzers* analyzers = settings.columns().empty()
        ? settings.add_columns()->mutable_analyzers()
        : settings.mutable_columns()->rbegin()->mutable_analyzers();

    if (nameLower == "tokenizer") {
        analyzers->set_tokenizer(ParseTokenizer(value, error));
    } else if (nameLower == "language") {
        analyzers->set_language(value);
    } else if (nameLower == "use_filter_lowercase") {
        analyzers->set_use_filter_lowercase(ParseBool(nameLower, value, error));
    } else if (nameLower == "use_filter_stopwords") {
        analyzers->set_use_filter_stopwords(ParseBool(nameLower, value, error));
    } else if (nameLower == "use_filter_ngram") {
        analyzers->set_use_filter_ngram(ParseBool(nameLower, value, error));
    } else if (nameLower == "use_filter_edge_ngram") {
        analyzers->set_use_filter_edge_ngram(ParseBool(nameLower, value, error));
    } else if (nameLower == "filter_ngram_min_length") {
        analyzers->set_filter_ngram_min_length(ParseInt32(nameLower, value, error));
    } else if (nameLower == "filter_ngram_max_length") {
        analyzers->set_filter_ngram_max_length(ParseInt32(nameLower, value, error));
    } else if (nameLower == "use_filter_length") {
        analyzers->set_use_filter_length(ParseBool(nameLower, value, error));
    } else if (nameLower == "filter_length_min") {
        analyzers->set_filter_length_min(ParseInt32(nameLower, value, error));
    } else if (nameLower == "filter_length_max") {
        analyzers->set_filter_length_max(ParseInt32(nameLower, value, error));
    } else if (nameLower == "use_filter_snowball") {
        analyzers->set_use_filter_snowball(ParseBool(nameLower, value, error));
    } else {
        error = TStringBuilder() << "Unknown index setting: " << nameLower;
        return false;
    }

    return !error;
}

void AddVarint(TVector<ui8>& buf, ui64 num) {
    while (true) {
        if (num < 0x80) {
            buf.push_back((ui8)num);
            break;
        } else {
            buf.push_back(0x80 | (ui8)(num & 0x7F));
            num >>= 7;
        }
    }
}

// regular varint but with an additional flag in the second most-significant bit
void AddVarintWithFlag(TVector<ui8>& buf, ui64 num, bool flag) {
    if (num < 0x40) {
        buf.push_back(((ui8)num | (flag ? 0x40 : 0)));
        return;
    } else {
        buf.push_back(0x80 | (flag ? 0x40 : 0) | (ui8)(num & 0x3F));
        num >>= 6;
        AddVarint(buf, num);
    }
}

ui64 ReadVarint(TConstArrayRef<ui8> buf, size_t& pos) {
    ui64 r = 0;
    ui32 o = 0;
    while (pos < buf.size()) {
        ui64 c = buf[pos++];
        r |= ((c & 0x7F) << o);
        if (!(c & 0x80)) {
            break;
        }
        o += 7;
        Y_ENSURE(o < 64);
    }
    return r;
}

ui64 ReadVarintWithFlag(TConstArrayRef<ui8> buf, size_t& pos, bool& flag) {
    flag = false;
    if (pos >= buf.size()) {
        return 0;
    }
    ui8 c = buf[pos++];
    flag = !!(c & 0x40);
    ui64 r = c & 0x3F;
    if (c & 0x80) {
        r |= ReadVarint(buf, pos) << 6;
    }
    return r;
}

TDeltaReader::TDeltaReader(TConstArrayRef<ui8> buf, bool withFreq, bool sign) {
    Buf = buf;
    Pos = 0;
    LastId = 0;
    WithFreq = withFreq;
    Sign = sign;
    MaxId = (sign ? INT64_MAX : UINT64_MAX);
}

bool TDeltaReader::Read(ui64& docId, ui32& freq) {
    if (Pos >= Buf.size()) {
        return false;
    }
    ui64 prevPos = Pos;
    bool hasFreq = false;
    if (WithFreq) {
        docId = LastId + ReadVarintWithFlag(Buf, Pos, hasFreq);
    } else {
        docId = LastId + ReadVarint(Buf, Pos);
    }
    if (!prevPos && Sign) {
        // Decode first item as zigzag
        docId = (docId >> 1) ^ -(docId & 1);
    }
    freq = hasFreq ? ReadVarint(Buf, Pos) : 1;
    if (Sign ? ((i64)docId > (i64)MaxId) : (docId > MaxId)) {
        Pos = prevPos;
        return false;
    }
    LastId = docId;
    return true;
}

void TDeltaReader::Save() {
    SavedPos = Pos;
    SavedLastId = LastId;
}

void TDeltaReader::Restore() {
    Pos = SavedPos;
    LastId = SavedLastId;
}

void TDeltaReader::SetMaxId(ui64 maxId) {
    MaxId = maxId;
}

void TDeltaWriter::Reset(bool withFreq, bool sign) {
    Buf.clear();
    MaxId = 0;
    Count = 0;
    WithFreq = withFreq;
    Sign = sign;
}

void TDeltaWriter::Add(ui64 DocId, ui32 Freq) {
    ui64 diff = DocId - MaxId;
    Y_ENSURE(!Count || (Sign ? ((i64)DocId > (i64)MaxId) : (DocId > MaxId)));
    if (!Count && Sign) {
        // Encode first item as zigzag, same as in protobuf
        i64 sdiff = (i64)diff;
        diff = (sdiff << 1) ^ (sdiff >> 63);
    }
    if (WithFreq) {
        AddVarintWithFlag(Buf, diff, Freq > 1);
        if (Freq > 1) {
            AddVarint(Buf, Freq);
        }
    } else {
        AddVarint(Buf, diff);
    }
    MaxId = DocId;
    Count++;
}

ui64 TDeltaWriter::GetMaxId() const {
    return MaxId;
}

ui64 TDeltaWriter::GetCount() const {
    return Count;
}

TConstArrayRef<ui8> TDeltaWriter::GetBuf() const {
    return Buf;
}

void TMultiDeltaReader::Reset(bool withFreq, bool sign) {
    Readers.clear();
    OwnedReaders.clear();
    Items.clear();
    WithFreq = withFreq;
    Sign = sign;
    OneLeft = false;
    Started = false;
}

void TMultiDeltaReader::Add(bool added, TDeltaReader* rdr) {
    Y_ENSURE(!Started);
    Readers.push_back({ rdr, added, false });
}

void TMultiDeltaReader::Add(bool added, TConstArrayRef<ui8> buf) {
    Y_ENSURE(!Started);
    if (!buf.size()) {
        return;
    }
    auto rdr = std::make_unique<TDeltaReader>(buf, WithFreq, Sign);
    Readers.push_back({ rdr.get(), added, true });
    OwnedReaders.push_back(std::move(rdr));
}

void TMultiDeltaReader::Pop() {
    if (!Readers.size()) {
        return;
    }
    auto& last = Readers.back();
    for (auto& item: Items) {
        Y_ENSURE(item.RdrId != Readers.size());
    }
    if (last.Owned) {
        Y_ENSURE(OwnedReaders.size() > 0 && OwnedReaders.back().get() == last.Reader);
        OwnedReaders.pop_back();
    }
    Readers.pop_back();
}

void TMultiDeltaReader::SetMaxId(ui64 maxId) {
    Y_ENSURE(!Items.size());
    for (auto& rdr: Readers) {
        rdr.Reader->SetMaxId(maxId);
    }
}

void TMultiDeltaReader::Start() {
    Y_ENSURE(!Started);
    Started = true;
    if (Readers.size() == 1) {
        OneLeft = true;
    } else {
        for (size_t i = 0; i < Readers.size(); i++) {
            Consume(i + 1, Readers[i]);
        }
        if (Items.size() > 0) {
            SelectNext();
        }
    }
}

void TMultiDeltaReader::Stop() {
    Y_ENSURE(!Items.size()); // restart is allowed, for example after changing MaxId
    OneLeft = false;
    Started = false;
}

void TMultiDeltaReader::SelectNext() {
    std::pop_heap(Items.begin(), Items.end(), Sign ? CompareSigned : CompareItems);
    NextItem = Items.back();
    Items.pop_back();
    auto& rdr = Readers[NextItem.RdrId - 1];
    Consume(NextItem.RdrId, rdr);
}

void TMultiDeltaReader::Consume(ui32 rdrId, TReaderRef& rdr) {
    ui64 docId = 0;
    ui32 freq = 1;
    if (rdr.Reader->Read(docId, freq)) {
        Items.push_back(TItem{docId, (rdr.Added ? (i32)freq : -(i32)freq), rdrId});
        std::push_heap(Items.begin(), Items.end(), Sign ? CompareSigned : CompareItems);
    }
}

bool TMultiDeltaReader::Read(ui64& docId, ui32& freq) {
    Y_ENSURE(Started);
    if (OneLeft) {
        if (!Readers[0].Added) {
            // Single deleted segment = should not happen, but empty
            ui64 unusedDoc = 0;
            ui32 unusedFreq = 1;
            while (Readers[0].Reader->Read(unusedDoc, unusedFreq)) {
            }
        }
        return Readers[0].Reader->Read(docId, freq);
    }
    TItem cur = NextItem;
    while (Items.size() > 0) {
        SelectNext();
        if (cur.DocId == NextItem.DocId) {
            // Add frequencies
            cur.Freq += NextItem.Freq;
        } else {
            if (cur.Freq > 0) {
                // Finished, item has positive frequency (not canceled by updates)
                // Leave NextItem as is
                docId = cur.DocId;
                freq = cur.Freq;
                return true;
            } else {
                // Scan the next item
                cur = NextItem;
            }
        }
    }
    NextItem = {};
    if (cur.Freq > 0) {
        // Finished, item has positive frequency (not canceled by updates)
        docId = cur.DocId;
        freq = cur.Freq;
        return true;
    }
    return false;
}

}

template<> inline
void Out<TVector<TString>>(IOutputStream& o, const TVector<TString> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}
