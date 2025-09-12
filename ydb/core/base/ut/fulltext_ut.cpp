#include "fulltext.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NFulltext {

Y_UNIT_TEST_SUITE(NFulltext) {

    Y_UNIT_TEST(ValidateSettings) {
        Ydb::Table::FulltextIndexSettings settings;
        TString error;

        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "layout should be set");
        settings.set_layout(Ydb::Table::FulltextIndexSettings::FLAT);

        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "fulltext index should have single column settings but have 0 of them");
        auto columnSettings = settings.add_columns();

        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "column should be set");
        columnSettings->set_column("text");

        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "column analyzers should be set");
        auto columnAnalyzers = columnSettings->mutable_analyzers();

        UNIT_ASSERT(!ValidateSettings(settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "tokenizer should be set");
        columnAnalyzers->set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);

        UNIT_ASSERT_C(ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");
    }

    Y_UNIT_TEST(FillSettings) {
        TVector<std::pair<TString, TString>> list{
            {"layout", "flat"},
            {"tokenizer", "standard"},
            {"use_filter_lowercase", "true"}
        };

        TString error;
        auto settings = FillSettings("text", list, error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        UNIT_ASSERT_EQUAL(settings.layout(), Ydb::Table::FulltextIndexSettings::FLAT);
        UNIT_ASSERT_VALUES_EQUAL(settings.columns().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.columns().at(0).column(), "text");
        UNIT_ASSERT_EQUAL(settings.columns().at(0).analyzers().tokenizer(), Ydb::Table::FulltextIndexSettings::STANDARD);
        UNIT_ASSERT_VALUES_EQUAL(settings.columns().at(0).analyzers().use_filter_lowercase(), true);
    }

    Y_UNIT_TEST(FillSettingsInvalid) {
        {
            TVector<std::pair<TString, TString>> list{
                {"asdf", "qwer"}
            };
            TString error;
            auto settings = FillSettings("text", list, error);
            UNIT_ASSERT_VALUES_EQUAL(error, "Unknown index setting: asdf");
        }

        {
            TVector<std::pair<TString, TString>> list{
                {"layout", "flat"},
                {"tokenizer", "standard"},
                {"use_filter_lowercase", "asdf"}
            };
            TString error;
            auto settings = FillSettings("text", list, error);
            UNIT_ASSERT_VALUES_EQUAL(error, "Invalid use_filter_lowercase: asdf");
        }

        {
            TVector<std::pair<TString, TString>> list{
                {"layout", "flat"},
            };
            TString error;
            auto settings = FillSettings("text", list, error);
            UNIT_ASSERT_VALUES_EQUAL(error, "tokenizer should be set");
        }
    }

    Y_UNIT_TEST(Analyze) {
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;
        TString text = "apple WaLLet  spaced-dog";
        
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"apple", "WaLLet", "spaced-dog"}));

        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"apple", "WaLLet", "spaced", "dog"}));

        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::KEYWORD);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{text}));

        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        analyzers.set_use_filter_lowercase(true);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"apple", "wallet", "spaced-dog"}));
    }
}

}
