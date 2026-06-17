#include "fulltext.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/xrange.h>

namespace NKikimr::NFulltext {

Y_UNIT_TEST_SUITE(NFulltext) {

    Y_UNIT_TEST(MultiDeltaReader1) {
        TDeltaWriter wr;
        wr.Reset(false, false);
        for (ui64 i = 1; i <= 100; i++) {
            wr.Add(i, 1);
        }
        TDeltaWriter wr2;
        for (ui64 i = 5; i <= 25; i += 2) {
            wr2.Add(i, 1);
        }
        TMultiDeltaReader rdr;
        rdr.Reset(false, false);
        rdr.Add(true, wr.GetBuf());
        rdr.Add(false, wr2.GetBuf());
        rdr.Start();
        ui64 docId;
        ui32 freq;
        for (ui64 i = 1; i <= 100; i++) {
            if (i >= 5 && i <= 25 && !((i - 5) % 2)) {
                continue;
            }
            UNIT_ASSERT(rdr.Read(docId, freq));
            UNIT_ASSERT_VALUES_EQUAL(docId, i);
            UNIT_ASSERT_VALUES_EQUAL(freq, 1);
        }
        UNIT_ASSERT(!rdr.Read(docId, freq));
    }

    Y_UNIT_TEST(MultiDeltaReader2) {
        TDeltaReader r1(TConstArrayRef<ui8>((const ui8*)"2\x0A", 2), false, false);
        r1.SetMaxId(203);
        TMultiDeltaReader rdr;
        rdr.Reset(false, false);
        rdr.Add(true, &r1);
        rdr.Add(false, TConstArrayRef<ui8>((const ui8*)"dd", 2));
        rdr.Add(true, TConstArrayRef<ui8>((const ui8*)"\x0A\x0A\x0A", 3));
        rdr.Add(true, TConstArrayRef<ui8>((const ui8*)"dd\x01\x02", 4));
        rdr.Start();
        ui64 docId;
        ui32 freq;
        auto check = [&](ui64 expectedDoc) {
            UNIT_ASSERT(rdr.Read(docId, freq));
            Cerr << "Read: " << docId << " == " << expectedDoc << "\n";
            UNIT_ASSERT_VALUES_EQUAL(docId, expectedDoc);
            UNIT_ASSERT_VALUES_EQUAL(freq, 1);
        };
        check(10);
        check(20);
        check(30);
        check(50);
        check(60);
        check(201);
        check(203);
        UNIT_ASSERT(!rdr.Read(docId, freq));
    }

    Y_UNIT_TEST(SignedDelta) {
        TDeltaWriter wr;
        wr.Reset(false, true);
        wr.Add(-1, 1);
        UNIT_ASSERT_VALUES_EQUAL(wr.GetBuf().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(wr.GetBuf()[0], 1);
        wr.Reset(true, true);
        for (i64 i = -50; i <= 50; i++) {
            wr.Add(i, 1 + ((i + 50) % 3));
        }
        TDeltaWriter wr2;
        wr2.Reset(true, true);
        for (i64 i = -25; i <= 25; i += 2) {
            wr2.Add(i, 1 + ((i + 50) % 3));
        }
        TMultiDeltaReader rdr;
        rdr.Reset(true, true);
        rdr.Add(true, wr.GetBuf());
        rdr.Add(false, wr2.GetBuf());
        rdr.Start();
        ui64 docId;
        ui32 freq;
        for (i64 i = -50; i <= 50; i++) {
            if (i >= -25 && i <= 25 && !((i + 25) % 2)) {
                continue;
            }
            UNIT_ASSERT(rdr.Read(docId, freq));
            Cerr << "Read: " << (i64)docId << " " << freq << "\n";
            UNIT_ASSERT_VALUES_EQUAL((i64)docId, i);
            UNIT_ASSERT_VALUES_EQUAL(freq, 1 + ((i + 50) % 3));
        }
        UNIT_ASSERT(!rdr.Read(docId, freq));
    }

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
        UNIT_ASSERT_VALUES_EQUAL(error, "Invalid filter_length_min: should be less than or equal to filter_length_max");

        columnAnalyzers->set_filter_length_min(-5);
        UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "Invalid filter_length_min: -5 should be between 1 and 1000");

        columnAnalyzers->set_filter_length_min(3);
        columnAnalyzers->set_filter_length_max(3000);
        UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "Invalid filter_length_max: 3000 should be between 1 and 1000");

        columnAnalyzers->set_use_filter_snowball(true);
        columnAnalyzers->clear_language();
        UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "language required when use_filter_snowball is set");

        columnAnalyzers->set_language("klingon");
        UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "language is not supported by snowball");

        columnAnalyzers->set_language("english");
        columnAnalyzers->set_use_filter_ngram(true);
        UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
        UNIT_ASSERT_VALUES_EQUAL(error, "cannot set use_filter_snowball with use_filter_ngram or use_filter_edge_ngram at the same time");

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
            UNIT_ASSERT_C(!FillSetting(settings, "layout", "flat", error), error);
            UNIT_ASSERT_VALUES_EQUAL(error, "Unknown index setting: layout");
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
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"эт", "это", "то", "те", "тек", "ек", "екс", "кс", "кст", "ст"}));

        analyzers.set_filter_ngram_min_length(4);
        analyzers.set_filter_ngram_max_length(10);
        UNIT_ASSERT_VALUES_EQUAL(Analyze("слово", analyzers), (TVector<TString>{"слов", "слово", "лово"}));

        analyzers.set_filter_ngram_min_length(10);
        analyzers.set_filter_ngram_max_length(10);
        UNIT_ASSERT_VALUES_EQUAL(Analyze("слово", analyzers), (TVector<TString>{}));

        analyzers.set_use_filter_ngram(false);
        analyzers.set_use_filter_edge_ngram(true);
        analyzers.set_filter_ngram_min_length(2);
        analyzers.set_filter_ngram_max_length(3);
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"эт", "это", "те", "тек"}));
    }

    Y_UNIT_TEST(AnalyzeFilterSnowball) {
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::WHITESPACE);
        const TString russianText = "машины ездят по дорогам исправно";

        UNIT_ASSERT_VALUES_EQUAL(Analyze(russianText, analyzers), (TVector<TString>{"машины", "ездят", "по", "дорогам", "исправно"}));

        analyzers.set_use_filter_snowball(true);
        analyzers.set_language("russian");
        UNIT_ASSERT_VALUES_EQUAL(Analyze(russianText, analyzers), (TVector<TString>{"машин", "езд", "по", "дорог", "исправн"}));

        const TString englishText = "cars are driving properly on the roads";
        analyzers.set_language("english");
        UNIT_ASSERT_VALUES_EQUAL(Analyze(englishText, analyzers), (TVector<TString>{"car", "are", "drive", "proper", "on", "the", "road"}));

        analyzers.set_language("klingon");
        UNIT_ASSERT_EXCEPTION(Analyze(englishText, analyzers), yexception);

        analyzers.clear_language();
        UNIT_ASSERT_EXCEPTION(Analyze(englishText, analyzers), yexception);
    }

    Y_UNIT_TEST(BuildNgramsUtf8) {
        {
            TVector<TString> ngrams;
            BuildNgrams("abc023", 3, 3, false, ngrams);
            UNIT_ASSERT_VALUES_EQUAL(ngrams, (TVector<TString>{"abc", "bc0", "c02", "023"}));
        }

        {
            TVector<TString> ngrams;
            BuildNgrams("◌̧◌̇◌̣", 3, 3, false, ngrams);
            UNIT_ASSERT_VALUES_EQUAL(ngrams, (TVector<TString>{"◌̧◌", "\u0327◌̇", "◌̇◌", "\u0307◌̣"}));
        }

        {
            TVector<TString> ngrams;
            BuildNgrams("﷽‎؈ۻ", 2, 2, false, ngrams);
            UNIT_ASSERT_VALUES_EQUAL(ngrams, (TVector<TString>{"﷽‎", "‎؈", "؈ۻ"}));
        }

        {
            TVector<TString> ngrams;
            BuildNgrams("异体字異體字", 3, 3, false, ngrams);
            UNIT_ASSERT_VALUES_EQUAL(ngrams, (TVector<TString>{"异体字", "体字異", "字異體", "異體字"}));
        }

        {
            TVector<TString> ngrams;
            BuildNgrams("ä̸̱b̴̪͛", 3, 3, false, ngrams);
            UNIT_ASSERT_VALUES_EQUAL(ngrams, (TVector<TString>{"a\u0338\u0308", "\u0338\u0308\u0331", "\u0308\u0331b", "\u0331b\u0334", "b\u0334\u035B", "\u0334\u035B\u032A"}));
        }

        {
            TVector<TString> ngrams;
            BuildNgrams("😢🐶🐕🐈", 2, 2, false, ngrams);
            UNIT_ASSERT_VALUES_EQUAL(ngrams, (TVector<TString>{"😢🐶", "🐶🐕", "🐕🐈"}));
        }

        {
            TVector<TString> ngrams;
            BuildNgrams("4️⃣🐕‍🦺🐈‍⬛", 3, 3, false, ngrams);
            UNIT_ASSERT_VALUES_EQUAL(ngrams, (TVector<TString>{"4️⃣", "\uFE0F\u20E3🐕", "\u20E3🐕\u200D", "🐕‍🦺", "\u200D\U0001F9BA🐈", "\U0001F9BA🐈\u200D", "🐈‍⬛"}));
        }

        {
            TVector<TString> ngrams;
            BuildNgrams("👨‍👩‍👧‍👦🇦🇨", 2, 2, false, ngrams);
            UNIT_ASSERT_VALUES_EQUAL(ngrams, (TVector<TString>{"👨\u200D", "\u200D👩", "👩\u200D", "\u200D👧", "👧\u200D", "\u200D👦", "👦🇦", "🇦🇨"}));
        }
    }
}

}
