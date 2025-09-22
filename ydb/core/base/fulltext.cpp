#include "fulltext.h"
#include <regex>

namespace NKikimr::NFulltext {

namespace {

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

    // Note: written by llm, can be optimized a lot later
    TVector<TString> Tokenize(const TString& text, const Ydb::Table::FulltextIndexSettings::Tokenizer& tokenizer) {
        TVector<TString> tokens;
        switch (tokenizer) {
            case Ydb::Table::FulltextIndexSettings::WHITESPACE: {
                std::istringstream stream(text);
                TString token;
                while (stream >> token) {
                    tokens.push_back(token);
                }
                break;
            }
            case Ydb::Table::FulltextIndexSettings::STANDARD: {
                std::regex word_regex(R"(\b\w+\b)"); // match alphanumeric words
                std::sregex_iterator it(text.begin(), text.end(), word_regex);
                std::sregex_iterator end;
                while (it != end) {
                    tokens.push_back(it->str());
                    ++it;
                }
                break;
            }
            case Ydb::Table::FulltextIndexSettings::KEYWORD:
                tokens.push_back(text);
                break;
            default:
                Y_ENSURE(TStringBuilder() << "Invalid tokenizer: " << static_cast<int>(tokenizer));
        }

        return tokens;
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
            error = "Unsupported use_filter_length setting";
            return false;
        }
        if (settings.has_filter_length_min()) {
            error = "Unsupported filter_length_min setting";
            return false;
        }
        if (settings.has_filter_length_max()) {
            error = "Unsupported filter_length_max setting";
            return false;
        }

        return true;
    }
}

TVector<TString> Analyze(const TString& text, const Ydb::Table::FulltextIndexSettings::Analyzers& settings) {
    TVector<TString> tokens = Tokenize(text, settings.tokenizer());

    if (settings.use_filter_lowercase()) {
        for (auto& token : tokens) {
            token.to_lower();
        }
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
