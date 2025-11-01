#include "fulltext.h"
#include <util/charset/utf8.h>
#include <util/generic/xrange.h>

namespace NKikimr::NFulltext {

namespace {

    bool ValidateSettingInRange(const TString& name, i32 value, i32 minValue, i32 maxValue, TString& error) {
        if (minValue <= value && value <= maxValue) {
            return true;
        }

        error = TStringBuilder() << "Invalid " << name << ": " << value << " should be between " << minValue << " and " << maxValue;
        return false;
    };

    Ydb::Table::FulltextIndexSettings::Layout ParseLayout(const TString& layout_, TString& error) {
        const TString layout = to_lower(layout_);
        if (layout == "flat")
            return Ydb::Table::FulltextIndexSettings::FLAT;
        else {
            error = TStringBuilder() << "Invalid layout: " << layout_;
            return Ydb::Table::FulltextIndexSettings::LAYOUT_UNSPECIFIED;
        }
    };

    Ydb::Table::FulltextIndexSettings::Tokenizer ParseTokenizer(const TString& tokenizer_, TString& error) {
        const TString tokenizer = to_lower(tokenizer_);
        if (tokenizer == "whitespace")
            return Ydb::Table::FulltextIndexSettings::WHITESPACE;
        else if (tokenizer == "standard")
            return Ydb::Table::FulltextIndexSettings::STANDARD;
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

    void Tokenize(const TString& text, TVector<TString>& tokens, auto isDelimiter) {
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

    TVector<TString> Tokenize(const TString& text, const Ydb::Table::FulltextIndexSettings::Tokenizer& tokenizer) {
        TVector<TString> tokens;
        switch (tokenizer) {
            case Ydb::Table::FulltextIndexSettings::WHITESPACE:
                Tokenize(text, tokens, IsWhitespace);
                break;
            case Ydb::Table::FulltextIndexSettings::STANDARD:
                Tokenize(text, tokens, IsNonStandard);
                break;
            case Ydb::Table::FulltextIndexSettings::KEYWORD:
                if (UTF8Detect(text) != NotUTF8) {
                    tokens.push_back(text);
                }
                break;
            default:
                Y_ENSURE(TStringBuilder() << "Invalid tokenizer: " << static_cast<int>(tokenizer));
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

        if (settings.has_language()) {
            error = "Unsupported language setting";
            return false;
        }

        if (settings.use_filter_stopwords()) {
            error = "Unsupported use_filter_stopwords setting";
            return false;
        }

        if (settings.use_filter_ngram()) {
            error = "Unsupported use_filter_ngram setting";
            return false;
        }
        if (settings.use_filter_edge_ngram()) {
            error = "Unsupported use_filter_edge_ngram setting";
            return false;
        }
        if (settings.has_filter_ngram_min_length()) {
            error = "Unsupported filter_ngram_min_length setting";
            return false;
        }
        if (settings.has_filter_ngram_max_length()) {
            error = "Unsupported filter_ngram_max_length setting";
            return false;
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
                error = "Invalid filter_length_min: should be less or equal than filter_length_max";
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

TVector<TString> Analyze(const TString& text, const Ydb::Table::FulltextIndexSettings::Analyzers& settings) {
    TVector<TString> tokens = Tokenize(text, settings.tokenizer());

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

    return tokens;
}

bool ValidateColumnsMatches(const NProtoBuf::RepeatedPtrField<TString>& columns, const Ydb::Table::FulltextIndexSettings& settings, TString& error) {
    return ValidateColumnsMatches(TVector<TString>{columns.begin(), columns.end()}, settings, error);
}

bool ValidateColumnsMatches(const TVector<TString>& columns, const Ydb::Table::FulltextIndexSettings& settings, TString& error) {
    TVector<TString> settingsColumns(::Reserve(settings.columns().size()));
    for (auto column : settings.columns()) {
        settingsColumns.push_back(column.column());
    }

    if (columns != settingsColumns) {
        error = TStringBuilder() << "columns " << settingsColumns << " should be " << columns;
        return false;
    }

    error = "";
    return true;
}

bool ValidateSettings(const Ydb::Table::FulltextIndexSettings& settings, TString& error) {
    if (!settings.has_layout() || settings.layout() == Ydb::Table::FulltextIndexSettings::LAYOUT_UNSPECIFIED) {
        error = "layout should be set";
        return false;
    }

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

bool FillSetting(Ydb::Table::FulltextIndexSettings& settings, const TString& name, const TString& value, TString& error) {
    error = "";

    Ydb::Table::FulltextIndexSettings::Analyzers* analyzers = settings.columns().empty()
        ? settings.add_columns()->mutable_analyzers()
        : settings.mutable_columns()->rbegin()->mutable_analyzers();

    const TString nameLower = to_lower(name);
    if (nameLower == "layout") {
        settings.set_layout(ParseLayout(value, error));
    } else if (nameLower == "tokenizer") {
        analyzers->set_tokenizer(ParseTokenizer(value, error));
    } else if (nameLower == "language") {
        analyzers->set_language(value);
    } else if (nameLower == "use_filter_lowercase") {
        analyzers->set_use_filter_lowercase(ParseBool(name, value, error));
    } else if (nameLower == "use_filter_stopwords") {
        analyzers->set_use_filter_stopwords(ParseBool(name, value, error));
    } else if (nameLower == "use_filter_ngram") {
        analyzers->set_use_filter_ngram(ParseBool(name, value, error));
    } else if (nameLower == "use_filter_edge_ngram") {
        analyzers->set_use_filter_edge_ngram(ParseBool(name, value, error));
    } else if (nameLower == "filter_ngram_min_length") {
        analyzers->set_filter_ngram_min_length(ParseInt32(name, value, error));
    } else if (nameLower == "filter_ngram_max_length") {
        analyzers->set_filter_ngram_max_length(ParseInt32(name, value, error));
    } else if (nameLower == "use_filter_length") {
        analyzers->set_use_filter_length(ParseBool(name, value, error));
    } else if (nameLower == "filter_length_min") {
        analyzers->set_filter_length_min(ParseInt32(name, value, error));
    } else if (nameLower == "filter_length_max") {
        analyzers->set_filter_length_max(ParseInt32(name, value, error));
    } else {
        error = TStringBuilder() << "Unknown index setting: " << name;
        return false;
    }

    return !error;
}


}

template<> inline
void Out<TVector<TString>>(IOutputStream& o, const TVector<TString> &vec) {
    o << "[ ";
    for (const auto &x : vec)
        o << x << ' ';
    o << "]";
}
