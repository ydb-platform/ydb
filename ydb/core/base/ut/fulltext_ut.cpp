#include "fulltext.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/xrange.h>

namespace NKikimr::NFulltext {

Y_UNIT_TEST_SUITE(NFulltext) {

    Y_UNIT_TEST(ValidateColumnsMatches) {
        TString error;
        
        Ydb::Table::FulltextIndexSettings settings;
        settings.add_columns()->set_column("column1");
        settings.add_columns()->set_column("column2");

        UNIT_ASSERT(!ValidateColumnsMatches(TVector<TString>{"column2"}, settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "columns [ column1 column2 ] should be [ column2 ]");

        UNIT_ASSERT(!ValidateColumnsMatches(TVector<TString>{"column2", "column1"}, settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "columns [ column1 column2 ] should be [ column2 column1 ]");

        UNIT_ASSERT(ValidateColumnsMatches(TVector<TString>{"column1", "column2"}, settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "");
    }

    Y_UNIT_TEST(ValidateSettings) {
        Ydb::Table::FulltextIndexSettings settings;
        TString error;

        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "layout should be set");

        settings.set_layout(Ydb::Table::FulltextIndexSettings::FLAT);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "columns should be set");
        
        auto columnSettings = settings.add_columns();
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "column name should be set");

        columnSettings->set_column("text");
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "column analyzers should be set");

        auto columnAnalyzers = columnSettings->mutable_analyzers();
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "tokenizer should be set");

        columnAnalyzers->set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);
        UNIT_ASSERT_C(ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        columnAnalyzers->set_use_filter_length(false);
        UNIT_ASSERT_C(ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        columnAnalyzers->set_use_filter_length(true);
        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "either filter_length_min or filter_length_max should be set with use_filter_length");

        columnAnalyzers->set_filter_length_min(5);
        UNIT_ASSERT_C(ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        columnAnalyzers->set_filter_length_max(6);
        UNIT_ASSERT_C(ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        columnAnalyzers->set_filter_length_max(3);
        UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "Invalid filter_length_min: should be less or equal than filter_length_max");

        columnAnalyzers->set_filter_length_min(-5);
        UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "Invalid filter_length_min: -5 should be between 1 and 1000");

        columnAnalyzers->set_filter_length_min(3);
        columnAnalyzers->set_filter_length_max(3000);
        UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "Invalid filter_length_max: 3000 should be between 1 and 1000");

        columnSettings = settings.add_columns();
        columnSettings->set_column("text2");
        UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "columns should have a single value");
    }

    Y_UNIT_TEST(FillSetting) {
        TString error;
        Ydb::Table::FulltextIndexSettings settings;
        settings.add_columns()->set_column("text");
        UNIT_ASSERT_VALUES_EQUAL(settings.columns().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.columns().at(0).column(), "text");
        
        UNIT_ASSERT_C(FillSetting(settings, "layout", "flat", error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");
        UNIT_ASSERT_EQUAL(settings.layout(), Ydb::Table::FulltextIndexSettings::FLAT);

        UNIT_ASSERT_C(FillSetting(settings, "tokenizer", "standard", error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");
        UNIT_ASSERT_EQUAL(settings.columns().at(0).analyzers().tokenizer(), Ydb::Table::FulltextIndexSettings::STANDARD);

        UNIT_ASSERT_C(FillSetting(settings, "use_filter_lowercase", "true", error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");
        UNIT_ASSERT_VALUES_EQUAL(settings.columns().at(0).analyzers().use_filter_lowercase(), true);

        UNIT_ASSERT_C(FillSetting(settings, "use_filter_length", "true", error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");
        UNIT_ASSERT_VALUES_EQUAL(settings.columns().at(0).analyzers().use_filter_length(), true);

        UNIT_ASSERT_C(FillSetting(settings, "filter_length_min", "4", error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");
        UNIT_ASSERT_VALUES_EQUAL(settings.columns().at(0).analyzers().filter_length_min(), 4);

        UNIT_ASSERT_C(FillSetting(settings, "filter_length_max", "5", error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");
        UNIT_ASSERT_VALUES_EQUAL(settings.columns().at(0).analyzers().filter_length_max(), 5);
    }

    Y_UNIT_TEST(FillSettingInvalid) {
        {
            Ydb::Table::FulltextIndexSettings settings;
            settings.add_columns()->set_column("text");

            TString error;
            UNIT_ASSERT_C(!FillSetting(settings, "asdf", "qwer", error), error);
            UNIT_ASSERT_VALUES_EQUAL(error, "Unknown index setting: asdf");
        }

        {
            Ydb::Table::FulltextIndexSettings settings;
            settings.add_columns()->set_column("text");

            TString error;
            UNIT_ASSERT_C(!FillSetting(settings, "use_filter_lowercase", "asdf", error), error);
            UNIT_ASSERT_VALUES_EQUAL(error, "Invalid use_filter_lowercase: asdf");
        }
    }

    Y_UNIT_TEST(Analyze) {
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;
        TString text = "apple WaLLet  spaced-dog_cat 0123,456@";
        
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"apple", "WaLLet", "spaced-dog_cat", "0123,456@"}));

        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"apple", "WaLLet", "spaced", "dog", "cat", "0123", "456"}));

        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::KEYWORD);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{text}));

        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        analyzers.set_use_filter_lowercase(true);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"apple", "wallet", "spaced-dog_cat", "0123,456@"}));
    }

    Y_UNIT_TEST(AnalyzeRu) {
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;
        TString text = "Привет, это test123 и слово Ёлка   ёль!";
        
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"Привет,", "это", "test123", "и", "слово", "Ёлка", "ёль!"}));

        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"Привет", "это", "test123", "и", "слово", "Ёлка", "ёль"}));

        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::KEYWORD);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{text}));

        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);
        analyzers.set_use_filter_lowercase(true);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"привет", "это", "test123", "и", "слово", "ёлка", "ёль"}));
    }

    Y_UNIT_TEST(AnalyzeInvalid) {
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;

        TVector<TString> texts = {
            "\xC2\x41", // Invalid continuation byte
            "\xC0\x81", // Overlong encoding
            "\x80", // Lone continuation byte
            "\xF4\x90\x80\x80", // Outside Unicode range
            "\xE3\x81", // Truncated (incomplete)
        };

        for (auto i : xrange(texts.size())) {
            TString testCase = TStringBuilder() << "case #" << i;
            auto& text = texts[i];

            analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
            UNIT_ASSERT_VALUES_EQUAL_C(Analyze(text, analyzers), (TVector<TString>{}), testCase);

            analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);
            UNIT_ASSERT_VALUES_EQUAL_C(Analyze(text, analyzers), (TVector<TString>{}), testCase);

            analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::KEYWORD);
            UNIT_ASSERT_VALUES_EQUAL_C(Analyze(text, analyzers), (TVector<TString>{}), testCase);

            analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::KEYWORD);
            analyzers.set_use_filter_lowercase(true);
            UNIT_ASSERT_VALUES_EQUAL_C(Analyze(text, analyzers), (TVector<TString>{}), testCase);
        }
    }

    Y_UNIT_TEST(AnalyzeFilterLength) {
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        TString text = "cat eats mice every day";

        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"cat", "eats", "mice", "every", "day"}));

        analyzers.set_use_filter_length(true);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"cat", "eats", "mice", "every", "day"}));

        analyzers.set_filter_length_min(4);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"eats", "mice", "every"}));

        analyzers.set_filter_length_max(4);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"eats", "mice"}));

        analyzers.clear_filter_length_min();
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"cat", "eats", "mice", "day"}));
    }

    Y_UNIT_TEST(AnalyzeFilterLengthRu) {
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        TString text = "кот ест мышей каждый день";

        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"кот", "ест", "мышей", "каждый", "день"}));

        analyzers.set_use_filter_length(true);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"кот", "ест", "мышей", "каждый", "день"}));

        analyzers.set_filter_length_min(4);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"мышей", "каждый", "день"}));

        analyzers.set_filter_length_max(4);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"день"}));

        analyzers.clear_filter_length_min();
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"кот", "ест", "день"}));
    }

    Y_UNIT_TEST(AnalyzeFilterNgram) {
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        TString text = "это текст";

        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"это", "текст"}));

        analyzers.set_use_filter_ngram(true);
        analyzers.set_filter_ngram_min_length(2);
        analyzers.set_filter_ngram_max_length(3);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"эт", "то", "это", "те", "ек", "кс", "ст", "тек", "екс", "кст"}));

        analyzers.set_use_filter_ngram(false);
        analyzers.set_use_filter_edge_ngram(true);
        analyzers.set_filter_ngram_min_length(2);
        analyzers.set_filter_ngram_max_length(3);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"эт", "это", "те", "тек"}));
    }
}

}
