#include "fulltext.h"

namespace NKikimr::NFulltext {

namespace {

    Ydb::Table::FulltextIndexSettings::Layout ParseLayout(const TString& layout, TString& error) {
        if (layout == "flat")
            return Ydb::Table::FulltextIndexSettings::FLAT;
        else {
            error = TStringBuilder() << "Invalid layout: " << layout;
            return Ydb::Table::FulltextIndexSettings::LAYOUT_UNSPECIFIED;
        }
    };

    Ydb::Table::FulltextIndexSettings::Tokenizer ParseTokenizer(const TString& tokenizer, TString& error) {
        if (tokenizer == "whitespace")
            return Ydb::Table::FulltextIndexSettings::WHITESPACE;
        else if (tokenizer == "standard")
            return Ydb::Table::FulltextIndexSettings::STANDARD;
        else if (tokenizer == "keyword")
            return Ydb::Table::FulltextIndexSettings::KEYWORD;
        else {
            error = TStringBuilder() << "Invalid tokenizer: " << tokenizer;
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

bool ValidateSettings(const Ydb::Table::FulltextIndexSettings& settings, TString& error) {
    if (!settings.has_layout() || settings.layout() == Ydb::Table::FulltextIndexSettings::LAYOUT_UNSPECIFIED) {
        error = "layout should be set";
        return false;
    }

    if (settings.columns().size() != 1) {
        error = TStringBuilder() << "fulltext index should have single column settings"
            << " but have " << settings.columns().size() << " of them";
        return false;
    }

    for (auto column : settings.columns()) {
        if (!column.has_column()) {
            error = "column should be set";
            return false;
        }
        if (!column.has_analyzers()) {
            error = "column analyzers should be set";
            return false;
        }
        if (!ValidateSettings(column.analyzers(), error)) {
            return false;
        }
    }

    return true;
}

Ydb::Table::FulltextIndexSettings FillSettings(const TString& column, const TVector<std::pair<TString, TString>>& settings, TString& error) {
    Ydb::Table::FulltextIndexSettings result;
    Ydb::Table::FulltextIndexSettings::Analyzers resultAnalyzers;

    for (const auto& [name, value] : settings) {
        if (name == "layout") {
            result.set_layout(ParseLayout(value, error));
        } else if (name == "tokenizer") {
            resultAnalyzers.set_tokenizer(ParseTokenizer(value, error));
        } else if (name == "language") {
            resultAnalyzers.set_language(value);
        } else if (name == "use_filter_lowercase") {
            resultAnalyzers.set_use_filter_lowercase(ParseBool(name, value, error));
        } else if (name == "use_filter_stopwords") {
            resultAnalyzers.set_use_filter_stopwords(ParseBool(name, value, error));
        } else if (name == "use_filter_ngram") {
            resultAnalyzers.set_use_filter_ngram(ParseBool(name, value, error));
        } else if (name == "use_filter_edge_ngram") {
            resultAnalyzers.set_use_filter_edge_ngram(ParseBool(name, value, error));
        } else if (name == "filter_ngram_min_length") {
            resultAnalyzers.set_filter_ngram_min_length(ParseInt32(name, value, error));
        } else if (name == "filter_ngram_max_length") {
            resultAnalyzers.set_filter_ngram_max_length(ParseInt32(name, value, error));
        } else if (name == "use_filter_length") {
            resultAnalyzers.set_use_filter_length(ParseBool(name, value, error));
        } else if (name == "filter_length_min") {
            resultAnalyzers.set_filter_length_min(ParseInt32(name, value, error));
        } else if (name == "filter_length_max") {
            resultAnalyzers.set_filter_length_max(ParseInt32(name, value, error));
        } else {
            error = TStringBuilder() << "Unknown index setting: " << name;
            return result;
        }

        if (error) {
            return result;
        }
    }

    {
        // only single-columned index is supported for now
        auto columnAnalyzers = result.add_columns();
        columnAnalyzers->set_column(column);
        columnAnalyzers->CopyFrom(resultAnalyzers);
    }

    ValidateSettings(result, error);

    return result;
}


}
