#include "fulltext.h"
#include "fulltext_query.h"
#include "table_index.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/xrange.h>

namespace NKikimr::NFulltext {

namespace {

struct TDeltaItem {
    ui64 DocId;
    ui32 Freq;
};

TVector<TDeltaItem> RoundTrip(const TVector<TDeltaItem>& items, bool withFreq, bool sign = false) {
    TDeltaWriter writer;
    writer.Reset(withFreq, sign);
    for (const auto& item : items) {
        writer.Add(item.DocId, item.Freq);
    }

    UNIT_ASSERT_VALUES_EQUAL(writer.GetCount(), items.size());
    UNIT_ASSERT_VALUES_EQUAL(writer.GetMaxId(), items.empty() ? 0 : items.back().DocId);

    TDeltaReader reader(writer.GetBuf(), withFreq, sign);
    TVector<TDeltaItem> result;
    ui64 docId = 0;
    ui32 freq = 0;
    while (reader.Read(docId, freq)) {
        result.push_back({docId, freq});
    }
    return result;
}

void AssertDeltaItemsEqual(const TVector<TDeltaItem>& actual, const TVector<TDeltaItem>& expected) {
    UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(actual[i].DocId, expected[i].DocId, "item " << i);
        UNIT_ASSERT_VALUES_EQUAL_C(actual[i].Freq, expected[i].Freq, "item " << i);
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(NFulltext) {

    // The compact rowid-mode doc-id layout: __ydb_row_id carries a dense seq in its low bits and a
    // bit-reversed spread bucket in its high bits. RowIdFromSeq must be a bijection (SeqFromRowId is its
    // left inverse) and consecutive seq values must spread across distinct high-bit buckets.
    Y_UNIT_TEST(RowIdSeqRoundTrip) {
        using namespace NKikimr::NTableIndex::NFulltext;

        // Round-trip across a range, around the bucket-cycle boundary, and for large/high-bit seq values.
        for (ui64 seq : xrange<ui64>(0, 5000)) {
            UNIT_ASSERT_VALUES_EQUAL(SeqFromRowId(RowIdFromSeq(seq)), seq);
        }
        for (ui64 seq : {ui64(0), ui64(1), ui64((1ull << RowIdSpreadBits) - 1), ui64(1ull << RowIdSpreadBits),
                         ui64(123456789), RowIdSeqMask - 1, RowIdSeqMask}) {
            ui64 rowId = RowIdFromSeq(seq);
            UNIT_ASSERT_VALUES_EQUAL_C(SeqFromRowId(rowId), seq, "seq=" << seq << " rowId=" << rowId);
            // seq lives strictly in the low (64 - RowIdSpreadBits) bits.
            UNIT_ASSERT_VALUES_EQUAL(seq & ~RowIdSeqMask, 0u);
        }

        // Injectivity + spread: a run of consecutive seq must map to distinct row ids, and the high-bit
        // bucket must take many different values (not a monotonic tail) over a full bucket cycle.
        THashSet<ui64> rowIds;
        THashSet<ui64> buckets;
        const ui64 cycle = 1ull << RowIdSpreadBits;
        for (ui64 seq : xrange<ui64>(0, cycle)) {
            ui64 rowId = RowIdFromSeq(seq);
            UNIT_ASSERT(rowIds.insert(rowId).second);
            buckets.insert(rowId >> (64 - RowIdSpreadBits));
        }
        // bit-reversal of the low RowIdSpreadBits is itself a bijection over [0, cycle), so every bucket appears.
        UNIT_ASSERT_VALUES_EQUAL(buckets.size(), cycle);
    }

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

    Y_UNIT_TEST(DeltaCodecEmptyAndSingle) {
        AssertDeltaItemsEqual(RoundTrip({}, false), {});
        AssertDeltaItemsEqual(RoundTrip({{0, 1}}, false), {{0, 1}});
        AssertDeltaItemsEqual(RoundTrip({{Max<ui64>(), 1}}, false), {{Max<ui64>(), 1}});

        // Frequency one is implicit in relevance segments, while larger values are stored explicitly.
        AssertDeltaItemsEqual(RoundTrip({{0, 1}}, true), {{0, 1}});
        AssertDeltaItemsEqual(RoundTrip({{Max<ui64>(), Max<ui32>()}}, true),
            {{Max<ui64>(), Max<ui32>()}});
    }

    Y_UNIT_TEST(DeltaCodecVarintBoundaries) {
        // Exercise one-byte/multi-byte transitions for regular delta varints (7 payload bits).
        const TVector<TDeltaItem> plain = {
            {0, 1},
            {127, 1},
            {255, 1},       // delta 128
            {16638, 1},     // delta 16383
            {33022, 1},     // delta 16384
            {Max<ui64>(), 1},
        };
        AssertDeltaItemsEqual(RoundTrip(plain, false), plain);

        // Relevance doc-id varints reserve a flag bit, so their first transition is 63 -> 64.
        // Frequencies independently cross the regular 127 -> 128 transition.
        const TVector<TDeltaItem> relevance = {
            {0, 1},
            {63, 2},
            {127, 127},     // delta 64
            {8254, 128},    // delta 8127
            {16446, Max<ui32>()}, // delta 8192
            {Max<ui64>(), 1},
        };
        AssertDeltaItemsEqual(RoundTrip(relevance, true), relevance);
    }

    Y_UNIT_TEST(DeltaCodecSignedExtremes) {
        const TVector<TDeltaItem> items = {
            {static_cast<ui64>(Min<i64>()), 1},
            {static_cast<ui64>(-1), 2},
            {0, 127},
            {static_cast<ui64>(Max<i64>()), Max<ui32>()},
        };
        AssertDeltaItemsEqual(RoundTrip(items, true, true), items);
    }

    Y_UNIT_TEST(DeltaCodecManyAndRandomizedRoundTrip) {
        TVector<TDeltaItem> many;
        many.reserve(10000);
        for (ui64 i = 0; i < 10000; ++i) {
            many.push_back({i * i + i, 1 + static_cast<ui32>(i % 257)});
        }
        AssertDeltaItemsEqual(RoundTrip(many, true), many);

        // Fixed-seed xorshift64 property test. Generate strictly increasing ids with deltas spanning
        // every varint width normally encountered by compact posting segments.
        ui64 random = 0x9e3779b97f4a7c15ULL;
        auto nextRandom = [&]() {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            return random;
        };

        for (ui32 iteration = 0; iteration < 100; ++iteration) {
            TVector<TDeltaItem> items;
            ui64 docId = nextRandom() & 0xFFFF;
            const size_t count = 1 + nextRandom() % 500;
            items.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                const ui32 shift = nextRandom() % 28;
                const ui64 delta = 1 + (nextRandom() & ((1ULL << shift) - 1));
                if (Max<ui64>() - docId < delta) {
                    break;
                }
                docId += delta;
                const ui32 freq = 1 + static_cast<ui32>(nextRandom() % 100000);
                items.push_back({docId, freq});
            }
            AssertDeltaItemsEqual(RoundTrip(items, true), items);
            AssertDeltaItemsEqual(RoundTrip(items, false), [&] {
                TVector<TDeltaItem> result = items;
                for (auto& item : result) {
                    item.Freq = 1;
                }
                return result;
            }());
        }
    }

    Y_UNIT_TEST(MultiDeltaReaderGenerationMerge) {
        auto encode = [](std::initializer_list<TDeltaItem> items) {
            TDeltaWriter writer;
            writer.Reset(true, false);
            for (const auto& item : items) {
                writer.Add(item.DocId, item.Freq);
            }
            return TVector<ui8>(writer.GetBuf().begin(), writer.GetBuf().end());
        };

        const auto base = encode({{1, 2}, {2, 1}, {4, 5}, {8, 1}});
        const auto deleted = encode({{1, 1}, {2, 1}, {4, 7}, {6, 1}});
        const auto added = encode({{1, 3}, {3, 1}, {4, 2}, {6, 2}});

        TMultiDeltaReader reader;
        reader.Reset(true, false);
        reader.Add(true, base);
        reader.Add(false, deleted);
        reader.Add(true, added);
        reader.Start();

        TVector<TDeltaItem> actual;
        ui64 docId = 0;
        ui32 freq = 0;
        while (reader.Read(docId, freq)) {
            actual.push_back({docId, freq});
        }

        // Frequencies from add generations are summed, deletes are subtracted, and non-positive
        // totals disappear. A delete of an absent id is canceled if a later generation adds it.
        const TVector<TDeltaItem> expected = {{1, 4}, {3, 1}, {6, 1}, {8, 1}};
        AssertDeltaItemsEqual(actual, expected);
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
        UNIT_ASSERT_VALUES_EQUAL(error, "indexed columns [ column1 column2 ] should be the suffix of index columns [ column2 ]");

        UNIT_ASSERT(!ValidateColumnsMatches(TVector<TString>{"column2", "column1"}, settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "indexed columns [ column1 column2 ] should be the suffix of index columns [ column2 column1 ]");

        UNIT_ASSERT(ValidateColumnsMatches(TVector<TString>{"column1", "column2"}, settings, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // prefix columns are allowed before the indexed (text) suffix
        Ydb::Table::FulltextIndexSettings single;
        single.add_columns()->set_column("text");
        UNIT_ASSERT(ValidateColumnsMatches(TVector<TString>{"user_id", "text"}, single, error));
        UNIT_ASSERT_VALUES_EQUAL(error, "");
        UNIT_ASSERT(ValidateColumnsMatches(TVector<TString>{"a", "b", "text"}, single, error));
        UNIT_ASSERT(!ValidateColumnsMatches(TVector<TString>{"user_id", "other"}, single, error));
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

    Y_UNIT_TEST(ValidateSuperLemmerSettings) {
        const auto makeSettings = [] {
            Ydb::Table::FulltextIndexSettings settings;
            auto* column = settings.add_columns();
            column->set_column("text");
            auto* analyzers = column->mutable_analyzers();
            analyzers->set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);
            analyzers->set_use_filter_superlemmer(true);
            return settings;
        };

        TString error;

        {
            auto settings = makeSettings();
            UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
            UNIT_ASSERT_VALUES_EQUAL(error, "language required when use_filter_superlemmer is set");
        }

        {
            auto settings = makeSettings();
            settings.mutable_columns()->at(0).mutable_analyzers()->set_language("klingon");
            UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
            UNIT_ASSERT_VALUES_EQUAL(error, "language is not supported by superlemmer");
        }

        {
            auto settings = makeSettings();
            auto* analyzers = settings.mutable_columns()->at(0).mutable_analyzers();
            analyzers->set_language("russian");
            analyzers->set_use_filter_snowball(true);
            UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
            UNIT_ASSERT_VALUES_EQUAL(error, "cannot set use_filter_snowball and use_filter_superlemmer at the same time");
        }

        for (bool edge : {false, true}) {
            auto settings = makeSettings();
            auto* analyzers = settings.mutable_columns()->at(0).mutable_analyzers();
            analyzers->set_language("russian");
            if (edge) {
                analyzers->set_use_filter_edge_ngram(true);
            } else {
                analyzers->set_use_filter_ngram(true);
            }
            UNIT_ASSERT_C(!ValidateSettings(settings, error), error);
            UNIT_ASSERT_VALUES_EQUAL(error, "cannot set use_filter_superlemmer with use_filter_ngram or use_filter_edge_ngram at the same time");
        }

        {
            auto settings = makeSettings();
            settings.mutable_columns()->at(0).mutable_analyzers()->set_language("russian");
            UNIT_ASSERT_C(ValidateSettings(settings, error), error);
            UNIT_ASSERT_VALUES_EQUAL(error, "");
        }
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

    Y_UNIT_TEST(FillAnalyzer) {
        TString error;
        Ydb::Table::FulltextIndexSettings settings;
        settings.add_columns()->set_column("text");

        UNIT_ASSERT_C(FillSetting(settings, "analyzer", "standard", error), error);
        auto* analyzers = settings.mutable_columns(0)->mutable_analyzers();
        UNIT_ASSERT_EQUAL(analyzers->tokenizer(), Ydb::Table::FulltextIndexSettings::STANDARD);
        UNIT_ASSERT(analyzers->use_filter_lowercase());
        UNIT_ASSERT(analyzers->use_filter_stopwords());
        UNIT_ASSERT_C(ValidateSettings(settings, error), error);

        analyzers->Clear();
        UNIT_ASSERT_C(FillSetting(settings, "analyzer", "snowball", error), error);
        UNIT_ASSERT_C(FillSetting(settings, "language", "russian", error), error);
        UNIT_ASSERT_EQUAL(analyzers->tokenizer(), Ydb::Table::FulltextIndexSettings::STANDARD);
        UNIT_ASSERT(analyzers->use_filter_lowercase());
        UNIT_ASSERT(analyzers->use_filter_stopwords());
        UNIT_ASSERT(analyzers->use_filter_snowball());
        UNIT_ASSERT_C(ValidateSettings(settings, error), error);

        analyzers->Clear();
        UNIT_ASSERT_C(FillSetting(settings, "analyzer", "keyword", error), error);
        UNIT_ASSERT_EQUAL(analyzers->tokenizer(), Ydb::Table::FulltextIndexSettings::KEYWORD);
        UNIT_ASSERT_C(ValidateSettings(settings, error), error);

        UNIT_ASSERT(!FillSetting(settings, "analyzer", "unknown", error));
        UNIT_ASSERT_VALUES_EQUAL(error, "Invalid analyzer: unknown");
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
        UNIT_ASSERT_VALUES_EQUAL(Analyze(text, analyzers), (TVector<TString>{"apple", "WaLLet", "spaced", "dog_cat", "0123,456"}));

        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::ALPHANUMERIC);
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

    Y_UNIT_TEST(AnalyzeFilterStopwords) {
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);
        analyzers.set_use_filter_lowercase(true);
        analyzers.set_use_filter_stopwords(true);

        UNIT_ASSERT_VALUES_EQUAL(
            Analyze("The quick brown fox is in the garden", analyzers),
            (TVector<TString>{"quick", "brown", "fox", "garden"}));

        analyzers.set_language("russian");
        UNIT_ASSERT_VALUES_EQUAL(
            Analyze("Это быстрый лис и он в саду", analyzers),
            (TVector<TString>{"быстрый", "лис", "саду"}));
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

    Y_UNIT_TEST(BuildSearchTermsStructured) {
        using T = TSearchTerm;
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);

        // No `+`: every term optional, tokenized like BuildSearchTerms.
        UNIT_ASSERT_VALUES_EQUAL(BuildSearchTermsStructured("apple banana", analyzers),
            (TVector<T>{{"apple", false}, {"banana", false}}));

        // Leading `+` marks a required term; bare terms stay optional.
        UNIT_ASSERT_VALUES_EQUAL(BuildSearchTermsStructured("+apple banana", analyzers),
            (TVector<T>{{"apple", true}, {"banana", false}}));

        // Multiple required terms mixed with optional ones.
        UNIT_ASSERT_VALUES_EQUAL(BuildSearchTermsStructured("+apple +banana cherry", analyzers),
            (TVector<T>{{"apple", true}, {"banana", true}, {"cherry", false}}));

        // A `+` term that analyzes into several tokens marks all of them required.
        UNIT_ASSERT_VALUES_EQUAL(BuildSearchTermsStructured("+spaced-dog cat", analyzers),
            (TVector<T>{{"spaced", true}, {"dog", true}, {"cat", false}}));

        // Bare `+` (no body) is ignored.
        UNIT_ASSERT_VALUES_EQUAL(BuildSearchTermsStructured("+ apple", analyzers),
            (TVector<T>{{"apple", false}}));

        // A `+` inside a term (not at term start) is not an operator.
        UNIT_ASSERT_VALUES_EQUAL(BuildSearchTermsStructured("c++ test", analyzers),
            (TVector<T>{{"c", false}, {"test", false}}));

        // Extra whitespace between terms is collapsed.
        UNIT_ASSERT_VALUES_EQUAL(BuildSearchTermsStructured("+apple   banana", analyzers),
            (TVector<T>{{"apple", true}, {"banana", false}}));

        // Analyzer filters apply to the term body after stripping `+`.
        analyzers.set_use_filter_lowercase(true);
        UNIT_ASSERT_VALUES_EQUAL(BuildSearchTermsStructured("+Apple banana", analyzers),
            (TVector<T>{{"apple", true}, {"banana", false}}));
        analyzers.set_use_filter_lowercase(false);

        // Keyword tokenizer without `+` keeps the whole query as a single token.
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::KEYWORD);
        UNIT_ASSERT_VALUES_EQUAL(BuildSearchTermsStructured("foo bar", analyzers),
            (TVector<T>{{"foo bar", false}}));
    }

    Y_UNIT_TEST(MidNumberChainedStandard) {
        // Regression test: after consuming a MidNumber separator + digit, prev was
        // incorrectly set to LETTER instead of DIGIT, so a second separator would not
        // satisfy the (prev == DIGIT || prev == MID_DIGIT) guard and the token was cut short.
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);

        // Two separators: "1,2,3" must be one token.
        UNIT_ASSERT_VALUES_EQUAL(Analyze("1,2,3", analyzers), (TVector<TString>{"1,2,3"}));

        // Mix of MidNumber chars (comma and period).
        UNIT_ASSERT_VALUES_EQUAL(Analyze("1,2.3", analyzers), (TVector<TString>{"1,2.3"}));

        // Trailing separator does not join — "1," ends at safeEnd before the comma.
        UNIT_ASSERT_VALUES_EQUAL(Analyze("1,2,", analyzers), (TVector<TString>{"1,2"}));

        // Longer chain.
        UNIT_ASSERT_VALUES_EQUAL(Analyze("1,2,3,4,5", analyzers), (TVector<TString>{"1,2,3,4,5"}));

        // Multiple tokens separated by whitespace — each chained number is its own token.
        UNIT_ASSERT_VALUES_EQUAL(Analyze("1,2,3 4,5,6", analyzers), (TVector<TString>{"1,2,3", "4,5,6"}));

        // Mixed: a word token followed by a chained number token.
        UNIT_ASSERT_VALUES_EQUAL(Analyze("price 1,234,567", analyzers), (TVector<TString>{"price", "1,234,567"}));

        // Chained number token followed by a letter token.
        UNIT_ASSERT_VALUES_EQUAL(Analyze("1,2,3 end", analyzers), (TVector<TString>{"1,2,3", "end"}));

        // Punctuation breaks tokens: "1,2.foo" — the period before a letter is MidLetter territory,
        // but here it follows digits so the chain stops at "1,2" and "foo" is separate.
        UNIT_ASSERT_VALUES_EQUAL(Analyze("1,2.foo", analyzers), (TVector<TString>{"1,2", "foo"}));
    }

    Y_UNIT_TEST(WordBreakTest) {
        // Generated from
        // http://www.unicode.org/Public/12.1.0/ucd/auxiliary/WordBreakTest.txt
        // and
        // http://www.unicode.org/Public/12.1.0/ucd/auxiliary/WordBreakProperty.txt
        TString tests = NResource::Find("word_break_test.json");
        NJson::TJsonValue out;
        ReadJsonTree(tests, &out, true);
        Ydb::Table::FulltextIndexSettings::Analyzers analyzers;
        analyzers.set_tokenizer(Ydb::Table::FulltextIndexSettings::STANDARD);
        int ok = 0, failed = 0;
        for (auto& test: out.GetArray()) {
            TString in = test["input"].GetString();
            TVector<TString> expected;
            for (auto& token: test["tokens"].GetArray()) {
                expected.push_back(token.GetString());
            }
            TVector<TString> out = Analyze(in, analyzers);
            if (out != expected) {
                Cerr << "Input: " << in << "\nTokens:";
                for (auto& token: out) {
                    Cerr << " " << token;
                }
                Cerr << "\nExpected:";
                for (auto& token: expected) {
                    Cerr << " " << token;
                }
                Cerr << "\n\n";
                failed++;
            } else {
                ok++;
            }
        }
        if (failed > 0) {
            Cerr << "Ok " << ok << "/" << (ok+failed) << " tests\n";
        }
        UNIT_ASSERT(!failed);
    }
}

}
