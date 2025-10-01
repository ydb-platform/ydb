#include "fulltext.h"

#include <library/cpp/testing/unittest/registar.h>

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

        columnSettings = settings.add_columns();
        columnSettings->set_column("text2");
        UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "columns should have a single value");
    }

    Y_UNIT_TEST(FillSetting) {
        Ydb::Table::FulltextIndexSettings settings;
        settings.add_columns()->set_column("text");

        TString error;
        
        UNIT_ASSERT_C(FillSetting(settings, "layout", "flat", error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        UNIT_ASSERT_C(FillSetting(settings, "tokenizer", "standard", error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        UNIT_ASSERT_C(FillSetting(settings, "use_filter_lowercase", "true", error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        UNIT_ASSERT_EQUAL(settings.layout(), Ydb::Table::FulltextIndexSettings::FLAT);
        UNIT_ASSERT_VALUES_EQUAL(settings.columns().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.columns().at(0).column(), "text");
        UNIT_ASSERT_EQUAL(settings.columns().at(0).analyzers().tokenizer(), Ydb::Table::FulltextIndexSettings::STANDARD);
        UNIT_ASSERT_VALUES_EQUAL(settings.columns().at(0).analyzers().use_filter_lowercase(), true);
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
