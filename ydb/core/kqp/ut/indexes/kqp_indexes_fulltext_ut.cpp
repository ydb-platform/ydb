#include <ydb/core/kqp/ut/common/kqp_ut_common.h>


namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpFulltextIndexes) {

TKikimrRunner Kikimr() {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    return TKikimrRunner(settings);
}

void CreateTexts(NQuery::TQueryClient& db) {
    TString query = R"sql(
        CREATE TABLE `/Root/Texts` (
            Key Uint64,
            Text String,
            Data String,
            PRIMARY KEY (Key)
        );
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void UpsertSomeTexts(NQuery::TQueryClient& db) {
    TString query = R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, "Cats love cats.", "cats data"),
            (200, "Dogs love foxes.", "cats data")
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void UpsertTexts(NQuery::TQueryClient& db) {
    TString query = R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, "Cats chase small animals.", "cats data"),
            (200, "Dogs chase small cats.", "dogs data"),
            (300, "Cats love cats.", "cats cats data"),
            (400, "Foxes love dogs.", "foxes data")
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void AddIndex(NQuery::TQueryClient& db) {
    TString query = R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
            GLOBAL USING fulltext
            ON (Text)
            WITH (layout=flat, tokenizer=standard, use_filter_lowercase=true)
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void AddIndexNGram(NQuery::TQueryClient& db, const size_t nGramMinLength = 3, const size_t nGramMaxLength = 3, const bool edgeNGram = false) {
    const TString query = Sprintf(R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
            GLOBAL USING fulltext
            ON (Text)
            WITH (
                layout=flat,
                tokenizer=standard,
                use_filter_lowercase=true,
                use_filter_ngram=%d,
                use_filter_edge_ngram=%d,
                filter_ngram_min_length=%d,
                filter_ngram_max_length=%d
            );
    )sql", !edgeNGram, edgeNGram, nGramMinLength, nGramMaxLength);
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void AddIndexCovered(NQuery::TQueryClient& db) {
    TString query = R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
            GLOBAL USING fulltext
            ON (Text) COVER (Data)
            WITH (layout=flat, tokenizer=standard, use_filter_lowercase=true)
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void AddIndexSnowball(NQuery::TQueryClient& db, const TString& language) {
    TString query = Sprintf(R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
            GLOBAL USING fulltext
            ON (Text)
            WITH (
                layout=flat,
                tokenizer=standard,
                use_filter_lowercase=true,
                use_filter_snowball=true,
                language=%s
            )
    )sql", language.c_str());
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

TResultSet ReadIndex(NQuery::TQueryClient& db) {
    TString query = R"sql(
        SELECT * FROM `/Root/Texts/fulltext_idx/indexImplTable`;
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return result.GetResultSet(0);
}

Y_UNIT_TEST(AddIndex) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[200u];"cats"];
        [[300u];"cats"];
        [[100u];"chase"];
        [[200u];"chase"];
        [[200u];"dogs"];
        [[400u];"dogs"];
        [[400u];"foxes"];
        [[300u];"love"];
        [[400u];"love"];
        [[100u];"small"];
        [[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(AddIndexCovered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndexCovered(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"animals"];
        [["cats data"];[100u];"cats"];
        [["dogs data"];[200u];"cats"];
        [["cats cats data"];[300u];"cats"];
        [["cats data"];[100u];"chase"];
        [["dogs data"];[200u];"chase"];
        [["dogs data"];[200u];"dogs"];
        [["foxes data"];[400u];"dogs"];
        [["foxes data"];[400u];"foxes"];
        [["cats cats data"];[300u];"love"];
        [["foxes data"];[400u];"love"];
        [["cats data"];[100u];"small"];
        [["dogs data"];[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(AddIndexNGram) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndexNGram(db);

    const auto index = ReadIndex(db);

    CompareYson(R"([
        [[100u];"all"];
        [[200u];"all"];
        [[100u];"als"];
        [[100u];"ani"];
        [[100u];"ase"];
        [[200u];"ase"];
        [[100u];"ats"];
        [[200u];"ats"];
        [[300u];"ats"];
        [[100u];"cat"];
        [[200u];"cat"];
        [[300u];"cat"];
        [[100u];"cha"];
        [[200u];"cha"];
        [[200u];"dog"];
        [[400u];"dog"];
        [[400u];"fox"];
        [[100u];"has"];
        [[200u];"has"];
        [[100u];"ima"];
        [[300u];"lov"];
        [[400u];"lov"];
        [[100u];"mal"];
        [[200u];"mal"];
        [[100u];"nim"];
        [[200u];"ogs"];
        [[400u];"ogs"];
        [[300u];"ove"];
        [[400u];"ove"];
        [[400u];"oxe"];
        [[100u];"sma"];
        [[200u];"sma"];
        [[400u];"xes"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(AddIndexEdgeNGram) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndexNGram(db, 3, 3, true);

    const auto index = ReadIndex(db);
    Cerr << NYdb::FormatResultSetYson(index) << Endl;
    CompareYson(R"([
        [[100u];"ani"];
        [[100u];"cat"];
        [[200u];"cat"];
        [[300u];"cat"];
        [[100u];"cha"];
        [[200u];"cha"];
        [[200u];"dog"];
        [[400u];"dog"];
        [[400u];"fox"];
        [[300u];"lov"];
        [[400u];"lov"];
        [[100u];"sma"];
        [[200u];"sma"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(AddIndexSnowball) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);
    AddIndexSnowball(db, "english");
    const auto index = ReadIndex(db);
    Cerr << index.RowsCount() << Endl;
    CompareYson(R"([
        [[100u];"anim"];
        [[100u];"cat"];
        [[200u];"cat"];
        [[300u];"cat"];
        [[100u];"chase"];
        [[200u];"chase"];
        [[200u];"dog"];
        [[400u];"dog"];
        [[400u];"fox"];
        [[300u];"love"];
        [[400u];"love"];
        [[100u];"small"];
        [[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(AddIndexSnowballWithWrongLanguage) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);

    UNIT_ASSERT_TEST_FAILS(AddIndexSnowball(db, "klingon"));
}

Y_UNIT_TEST(InsertRow) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[200u];"dogs"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // InsertRow
        TString query = R"sql(
            INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[150u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(InsertRowMultipleTimes) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[200u];"dogs"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // InsertRow
        TString query = R"sql(
            INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data"),
                (151, "Wolfs love foxes.", "cows data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    { // InsertRow
        TString query = R"sql(
            INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (152, "Rabbits love foxes.", "rabbit data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[150u];"foxes"];
        [[151u];"foxes"];
        [[152u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[151u];"love"];
        [[152u];"love"];
        [[200u];"love"];
        [[152u];"rabbits"];
        [[151u];"wolfs"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(InsertRowReturning) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[200u];"dogs"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // InsertRow
        TString query = R"sql(
            INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["foxes data"];[150u];["Foxes love cats."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[150u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(InsertRowCovered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndexCovered(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // InsertRow
        TString query = R"sql(
            INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["foxes data"];[150u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["foxes data"];[150u];"foxes"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(InsertRowCoveredReturning) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndexCovered(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // InsertRow
        TString query = R"sql(
            INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["foxes data"];[150u];["Foxes love cats."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["foxes data"];[150u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["foxes data"];[150u];"foxes"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(UpsertRow) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[200u];"dogs"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // UpsertRow - insert new row
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[150u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // UpsertRow - modify existing row
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (100, "Birds love foxes.", "birds data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"birds"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[100u];"foxes"];
        [[150u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(UpsertRowMultipleTimes) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[200u];"dogs"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // UpsertRow - insert new rows
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data"),
                (151, "Wolfs love foxes.", "cows data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    { // UpsertRow - insert new row
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (152, "Rabbits love foxes.", "rabbits data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[150u];"foxes"];
        [[151u];"foxes"];
        [[152u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[151u];"love"];
        [[152u];"love"];
        [[200u];"love"];
        [[152u];"rabbits"];
        [[151u];"wolfs"]
    ])", NYdb::FormatResultSetYson(index));

    { // UpsertRow - modify existing row
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (100, "Birds love rabbits.", "birds data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"birds"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[150u];"foxes"];
        [[151u];"foxes"];
        [[152u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[151u];"love"];
        [[152u];"love"];
        [[200u];"love"];
        [[100u];"rabbits"];
        [[152u];"rabbits"];
        [[151u];"wolfs"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(UpsertRowReturning) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[200u];"dogs"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // UpsertRow - insert new row
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["foxes data"];[150u];["Foxes love cats."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[150u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // UpsertRow - modify existing row
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (200, "Birds love rabbits.", "birds data")
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["birds data"];[200u];["Birds love rabbits."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[200u];"birds"];
        [[100u];"cats"];
        [[150u];"cats"];
        [[150u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[200u];"love"];
        [[200u];"rabbits"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(UpsertRowCovered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndexCovered(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // UpsertRow - insert new row
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["foxes data"];[150u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["foxes data"];[150u];"foxes"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // UpsertRow - modify existing row
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (100, "Birds love foxes.", "birds data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["birds data"];[100u];"birds"];
        [["foxes data"];[150u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["birds data"];[100u];"foxes"];
        [["foxes data"];[150u];"foxes"];
        [["cats data"];[200u];"foxes"];
        [["birds data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(UpsertRowCoveredReturning) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndexCovered(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // UpsertRow - insert new row
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["foxes data"];[150u];["Foxes love cats."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["foxes data"];[150u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["foxes data"];[150u];"foxes"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // UpsertRow - modify existing row
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (200, "Birds love rabbits.", "birds data")
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["birds data"];[200u];["Birds love rabbits."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["birds data"];[200u];"birds"];
        [["cats data"];[100u];"cats"];
        [["foxes data"];[150u];"cats"];
        [["foxes data"];[150u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["birds data"];[200u];"love"];
        [["birds data"];[200u];"rabbits"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(ReplaceRow) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[200u];"dogs"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // ReplaceRow - insert new row
        TString query = R"sql(
            REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[150u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // ReplaceRow - replace existing row
        TString query = R"sql(
            REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
                (100, "Birds love foxes.", "birds data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"birds"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[100u];"foxes"];
        [[150u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(ReplaceRowMultipleTimes) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[200u];"dogs"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // ReplaceRow - insert new rows
        TString query = R"sql(
            REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data"),
                (151, "Wolfs love foxes.", "cows data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    { // ReplaceRow - insert new row
        TString query = R"sql(
            REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
                (152, "Rabbits love foxes.", "rabbit data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[150u];"foxes"];
        [[151u];"foxes"];
        [[152u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[151u];"love"];
        [[152u];"love"];
        [[200u];"love"];
        [[152u];"rabbits"];
        [[151u];"wolfs"]
    ])", NYdb::FormatResultSetYson(index));

    { // ReplaceRow - replace existing row
        TString query = R"sql(
            REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
                (100, "Birds love rabbits.", "birds data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"birds"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[150u];"foxes"];
        [[151u];"foxes"];
        [[152u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[151u];"love"];
        [[152u];"love"];
        [[200u];"love"];
        [[100u];"rabbits"];
        [[152u];"rabbits"];
        [[151u];"wolfs"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(ReplaceRowReturning) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[200u];"dogs"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // ReplaceRow - insert new row
        TString query = R"sql(
            REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["foxes data"];[150u];["Foxes love cats."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[150u];"cats"];
        [[200u];"dogs"];
        [[150u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // ReplaceRow - replace existing row
        TString query = R"sql(
            REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
                (200, "Birds love rabbits.", "birds data")
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["birds data"];[200u];["Birds love rabbits."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[200u];"birds"];
        [[100u];"cats"];
        [[150u];"cats"];
        [[150u];"foxes"];
        [[100u];"love"];
        [[150u];"love"];
        [[200u];"love"];
        [[200u];"rabbits"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(ReplaceRowCovered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndexCovered(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // ReplaceRow - insert new row
        TString query = R"sql(
            REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["foxes data"];[150u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["foxes data"];[150u];"foxes"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // ReplaceRow - replace existing row
        TString query = R"sql(
            REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
                (100, "Birds love foxes.", "birds data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["birds data"];[100u];"birds"];
        [["foxes data"];[150u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["birds data"];[100u];"foxes"];
        [["foxes data"];[150u];"foxes"];
        [["cats data"];[200u];"foxes"];
        [["birds data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(ReplaceRowCoveredReturning) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndexCovered(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // ReplaceRow - insert new row
        TString query = R"sql(
            REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["foxes data"];[150u];["Foxes love cats."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["foxes data"];[150u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["foxes data"];[150u];"foxes"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // ReplaceRow - replace existing row
        TString query = R"sql(
            REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
                (200, "Birds love rabbits.", "birds data")
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["birds data"];[200u];["Birds love rabbits."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["birds data"];[200u];"birds"];
        [["cats data"];[100u];"cats"];
        [["foxes data"];[150u];"cats"];
        [["foxes data"];[150u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["birds data"];[200u];"love"];
        [["birds data"];[200u];"rabbits"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(DeleteRow) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[200u];"cats"];
        [[300u];"cats"];
        [[100u];"chase"];
        [[200u];"chase"];
        [[200u];"dogs"];
        [[400u];"dogs"];
        [[400u];"foxes"];
        [[300u];"love"];
        [[400u];"love"];
        [[100u];"small"];
        [[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by PK
        TString query = R"sql(
            DELETE FROM `/Root/Texts` WHERE Key = 200;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[300u];"cats"];
        [[100u];"chase"];
        [[400u];"dogs"];
        [[400u];"foxes"];
        [[300u];"love"];
        [[400u];"love"];
        [[100u];"small"]
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by filter
        TString query = R"sql(
            DELETE FROM `/Root/Texts` WHERE Data = "foxes data";
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[300u];"cats"];
        [[100u];"chase"];
        [[300u];"love"];
        [[100u];"small"]
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by ON
        TString query = R"sql(
            DELETE FROM `/Root/Texts` ON SELECT 300 AS `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[100u];"chase"];
        [[100u];"small"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(DeleteRowMultipleTimes) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[200u];"cats"];
        [[300u];"cats"];
        [[100u];"chase"];
        [[200u];"chase"];
        [[200u];"dogs"];
        [[400u];"dogs"];
        [[400u];"foxes"];
        [[300u];"love"];
        [[400u];"love"];
        [[100u];"small"];
        [[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by multiple PK
        TString query = R"sql(
            DELETE FROM `/Root/Texts` WHERE Key = 200 OR Key = 400;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[300u];"cats"];
        [[100u];"chase"];
        [[300u];"love"];
        [[100u];"small"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(DeleteRowReturning) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[200u];"cats"];
        [[300u];"cats"];
        [[100u];"chase"];
        [[200u];"chase"];
        [[200u];"dogs"];
        [[400u];"dogs"];
        [[400u];"foxes"];
        [[300u];"love"];
        [[400u];"love"];
        [[100u];"small"];
        [[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by PK
        TString query = R"sql(
            DELETE FROM `/Root/Texts` WHERE Key = 200
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["dogs data"];[200u];["Dogs chase small cats."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[300u];"cats"];
        [[100u];"chase"];
        [[400u];"dogs"];
        [[400u];"foxes"];
        [[300u];"love"];
        [[400u];"love"];
        [[100u];"small"]
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by filter
        TString query = R"sql(
            DELETE FROM `/Root/Texts` WHERE Data = "foxes data"
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["foxes data"];[400u];["Foxes love dogs."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[300u];"cats"];
        [[100u];"chase"];
        [[300u];"love"];
        [[100u];"small"]
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by ON
        TString query = R"sql(
            DELETE FROM `/Root/Texts` ON SELECT 300 AS `Key`
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["cats cats data"];[300u];["Cats love cats."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[100u];"chase"];
        [[100u];"small"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(DeleteRowCovered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndexCovered(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"animals"];
        [["cats data"];[100u];"cats"];
        [["dogs data"];[200u];"cats"];
        [["cats cats data"];[300u];"cats"];
        [["cats data"];[100u];"chase"];
        [["dogs data"];[200u];"chase"];
        [["dogs data"];[200u];"dogs"];
        [["foxes data"];[400u];"dogs"];
        [["foxes data"];[400u];"foxes"];
        [["cats cats data"];[300u];"love"];
        [["foxes data"];[400u];"love"];
        [["cats data"];[100u];"small"];
        [["dogs data"];[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by PK
        TString query = R"sql(
            DELETE FROM `/Root/Texts` WHERE Key = 200;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"animals"];
        [["cats data"];[100u];"cats"];
        [["cats cats data"];[300u];"cats"];
        [["cats data"];[100u];"chase"];
        [["foxes data"];[400u];"dogs"];
        [["foxes data"];[400u];"foxes"];
        [["cats cats data"];[300u];"love"];
        [["foxes data"];[400u];"love"];
        [["cats data"];[100u];"small"];
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by filter
        TString query = R"sql(
            DELETE FROM `/Root/Texts` WHERE Data = "foxes data";
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"animals"];
        [["cats data"];[100u];"cats"];
        [["cats cats data"];[300u];"cats"];
        [["cats data"];[100u];"chase"];
        [["cats cats data"];[300u];"love"];
        [["cats data"];[100u];"small"];
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by ON
        TString query = R"sql(
            DELETE FROM `/Root/Texts` ON SELECT 300 AS `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"animals"];
        [["cats data"];[100u];"cats"];
        [["cats data"];[100u];"chase"];
        [["cats data"];[100u];"small"];
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(DeleteRowCoveredReturning) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndexCovered(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"animals"];
        [["cats data"];[100u];"cats"];
        [["dogs data"];[200u];"cats"];
        [["cats cats data"];[300u];"cats"];
        [["cats data"];[100u];"chase"];
        [["dogs data"];[200u];"chase"];
        [["dogs data"];[200u];"dogs"];
        [["foxes data"];[400u];"dogs"];
        [["foxes data"];[400u];"foxes"];
        [["cats cats data"];[300u];"love"];
        [["foxes data"];[400u];"love"];
        [["cats data"];[100u];"small"];
        [["dogs data"];[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by PK
        TString query = R"sql(
            DELETE FROM `/Root/Texts` WHERE Key = 200
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["dogs data"];[200u];["Dogs chase small cats."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"animals"];
        [["cats data"];[100u];"cats"];
        [["cats cats data"];[300u];"cats"];
        [["cats data"];[100u];"chase"];
        [["foxes data"];[400u];"dogs"];
        [["foxes data"];[400u];"foxes"];
        [["cats cats data"];[300u];"love"];
        [["foxes data"];[400u];"love"];
        [["cats data"];[100u];"small"];
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by filter
        TString query = R"sql(
            DELETE FROM `/Root/Texts` WHERE Data = "foxes data"
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["foxes data"];[400u];["Foxes love dogs."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"animals"];
        [["cats data"];[100u];"cats"];
        [["cats cats data"];[300u];"cats"];
        [["cats data"];[100u];"chase"];
        [["cats cats data"];[300u];"love"];
        [["cats data"];[100u];"small"];
    ])", NYdb::FormatResultSetYson(index));

    { // DeleteRow by ON
        TString query = R"sql(
            DELETE FROM `/Root/Texts` ON SELECT 300 AS `Key`
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["cats cats data"];[300u];["Cats love cats."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"animals"];
        [["cats data"];[100u];"cats"];
        [["cats data"];[100u];"chase"];
        [["cats data"];[100u];"small"];
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(UpdateRow) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[200u];"dogs"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update Text - index key column updated
        TString query = R"sql(
            UPDATE `/Root/Texts` SET Text = "Birds love foxes." WHERE Key = 100;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"birds"];
        [[200u];"dogs"];
        [[100u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update Data - non-indexed column updated
        TString query = R"sql(
            UPDATE `/Root/Texts` SET Data = "birds data" WHERE Key = 100;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"birds"];
        [[200u];"dogs"];
        [[100u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update by ON
        TString query = R"sql(
            UPDATE `/Root/Texts` ON SELECT 200 AS `Key`, "Rabbits love birds." AS `Text`, "rabbits data" AS `Data`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"birds"];
        [[200u];"birds"];
        [[100u];"foxes"];
        [[100u];"love"];
        [[200u];"love"];
        [[200u];"rabbits"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(UpdateRowCovered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndexCovered(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update Text - index key column updated
        TString query = R"sql(
            UPDATE `/Root/Texts` SET Text = "Birds love foxes." WHERE Key = 100;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"birds"];
        [["cats data"];[200u];"dogs"];
        [["cats data"];[100u];"foxes"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update Data - covered column updated
        TString query = R"sql(
            UPDATE `/Root/Texts` SET Data = "birds data" WHERE Key = 100;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["birds data"];[100u];"birds"];
        [["cats data"];[200u];"dogs"];
        [["birds data"];[100u];"foxes"];
        [["cats data"];[200u];"foxes"];
        [["birds data"];[100u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update by ON
        TString query = R"sql(
            UPDATE `/Root/Texts` ON SELECT 200 AS `Key`, "Rabbits love birds." AS `Text`, "rabbits data" AS `Data`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["birds data"];[100u];"birds"];
        [["rabbits data"];[200u];"birds"];
        [["birds data"];[100u];"foxes"];
        [["birds data"];[100u];"love"];
        [["rabbits data"];[200u];"love"];
        [["rabbits data"];[200u];"rabbits"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(UpdateRowMultipleTimes) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[200u];"dogs"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update multiple rows
        TString query = R"sql(
            UPDATE `/Root/Texts` SET Text = "Birds love foxes." WHERE Key = 100 OR Key = 200;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"birds"];
        [[200u];"birds"];
        [[100u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(UpdateRowReturning) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"cats"];
        [[200u];"dogs"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update Text - index key column updated
        TString query = R"sql(
            UPDATE `/Root/Texts` SET Text = "Birds love foxes." WHERE Key = 100
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["cats data"];[100u];["Birds love foxes."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"birds"];
        [[200u];"dogs"];
        [[100u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update Data - non-indexed column updated
        TString query = R"sql(
            UPDATE `/Root/Texts` SET Data = "birds data" WHERE Key = 100
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["birds data"];[100u];["Birds love foxes."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"birds"];
        [[200u];"dogs"];
        [[100u];"foxes"];
        [[200u];"foxes"];
        [[100u];"love"];
        [[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update by ON
        TString query = R"sql(
            UPDATE `/Root/Texts` ON SELECT 200 AS `Key`, "Rabbits love birds." AS `Text`, "rabbits data" AS `Data`
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["rabbits data"];[200u];["Rabbits love birds."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"birds"];
        [[200u];"birds"];
        [[100u];"foxes"];
        [[100u];"love"];
        [[200u];"love"];
        [[200u];"rabbits"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(UpdateRowCoveredReturning) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndexCovered(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"cats"];
        [["cats data"];[200u];"dogs"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update Text - index key column updated
        TString query = R"sql(
            UPDATE `/Root/Texts` SET Text = "Birds love foxes." WHERE Key = 100
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["cats data"];[100u];["Birds love foxes."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"birds"];
        [["cats data"];[200u];"dogs"];
        [["cats data"];[100u];"foxes"];
        [["cats data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update Data - covered column updated
        TString query = R"sql(
            UPDATE `/Root/Texts` SET Data = "birds data" WHERE Key = 100
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["birds data"];[100u];["Birds love foxes."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["birds data"];[100u];"birds"];
        [["cats data"];[200u];"dogs"];
        [["birds data"];[100u];"foxes"];
        [["cats data"];[200u];"foxes"];
        [["birds data"];[100u];"love"];
        [["cats data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));

    { // Update by ON
        TString query = R"sql(
            UPDATE `/Root/Texts` ON SELECT 200 AS `Key`, "Rabbits love birds." AS `Text`, "rabbits data" AS `Data`
            RETURNING *
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["rabbits data"];[200u];["Rabbits love birds."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [["birds data"];[100u];"birds"];
        [["rabbits data"];[200u];"birds"];
        [["birds data"];[100u];"foxes"];
        [["birds data"];[100u];"love"];
        [["rabbits data"];[200u];"love"];
        [["rabbits data"];[200u];"rabbits"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(Select) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndex(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[200u];"cats"];
        [[300u];"cats"];
        [[100u];"chase"];
        [[200u];"chase"];
        [[200u];"dogs"];
        [[400u];"dogs"];
        [[400u];"foxes"];
        [[300u];"love"];
        [[400u];"love"];
        [[100u];"small"];
        [[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));

    { // Select by one token
        TString query = R"sql(
            SELECT Key FROM `/Root/Texts/fulltext_idx/indexImplTable`
            WHERE __ydb_token = "dogs"
            ORDER BY Key
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[200u]];
            [[400u]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

    { // Select by two tokens using OR
        TString query = R"sql(
            SELECT DISTINCT Key FROM `/Root/Texts/fulltext_idx/indexImplTable`
            WHERE __ydb_token IN ("foxes", "animals")
            ORDER BY Key
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[100u]];
            [[400u]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    
    { // Select by two tokens using AND
        TString query = R"sql(
            SELECT Key FROM `/Root/Texts/fulltext_idx/indexImplTable`
            WHERE __ydb_token = "dogs"
            INTERSECT
            SELECT Key FROM `/Root/Texts/fulltext_idx/indexImplTable`
            WHERE __ydb_token = "chase"
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[200u]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
}

Y_UNIT_TEST(SelectCovered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndexCovered(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"animals"];
        [["cats data"];[100u];"cats"];
        [["dogs data"];[200u];"cats"];
        [["cats cats data"];[300u];"cats"];
        [["cats data"];[100u];"chase"];
        [["dogs data"];[200u];"chase"];
        [["dogs data"];[200u];"dogs"];
        [["foxes data"];[400u];"dogs"];
        [["foxes data"];[400u];"foxes"];
        [["cats cats data"];[300u];"love"];
        [["foxes data"];[400u];"love"];
        [["cats data"];[100u];"small"];
        [["dogs data"];[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));

    { // Select by one token
        TString query = R"sql(
            SELECT Key, Data FROM `/Root/Texts/fulltext_idx/indexImplTable`
            WHERE __ydb_token = "dogs"
            ORDER BY Key
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[200u];["dogs data"]];
            [[400u];["foxes data"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

    { // Select by two tokens using OR
        TString query = R"sql(
            SELECT DISTINCT Key, Data FROM `/Root/Texts/fulltext_idx/indexImplTable`
            WHERE __ydb_token IN ("foxes", "animals")
            ORDER BY Key
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[100u];["cats data"]];
            [[400u];["foxes data"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    
    { // Select by two tokens using AND
        TString query = R"sql(
            SELECT Key, Data FROM `/Root/Texts/fulltext_idx/indexImplTable`
            WHERE __ydb_token = "dogs"
            INTERSECT
            SELECT Key, Data FROM `/Root/Texts/fulltext_idx/indexImplTable`
            WHERE __ydb_token = "chase"
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[200u];["dogs data"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
}

Y_UNIT_TEST(CreateTable) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    { // CreateTexts
        TString query = R"sql(
            CREATE TABLE `/Root/Texts` (
                Key Uint64,
                Text String,
                Data String,
                PRIMARY KEY (Key),
                INDEX fulltext_idx
                    GLOBAL USING fulltext
                    ON (Text)
                    WITH (layout=flat, tokenizer=standard, use_filter_lowercase=true)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    UpsertTexts(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];"animals"];
        [[100u];"cats"];
        [[200u];"cats"];
        [[300u];"cats"];
        [[100u];"chase"];
        [[200u];"chase"];
        [[200u];"dogs"];
        [[400u];"dogs"];
        [[400u];"foxes"];
        [[300u];"love"];
        [[400u];"love"];
        [[100u];"small"];
        [[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(CreateTableCovered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    { // CreateTexts
        TString query = R"sql(
            CREATE TABLE `/Root/Texts` (
                Key Uint64,
                Text String,
                Data String,
                PRIMARY KEY (Key),
                INDEX fulltext_idx
                    GLOBAL USING fulltext
                    ON (Text) COVER (Data)
                    WITH (layout=flat, tokenizer=standard, use_filter_lowercase=true)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    UpsertTexts(db);
    auto index = ReadIndex(db);
    CompareYson(R"([
        [["cats data"];[100u];"animals"];
        [["cats data"];[100u];"cats"];
        [["dogs data"];[200u];"cats"];
        [["cats cats data"];[300u];"cats"];
        [["cats data"];[100u];"chase"];
        [["dogs data"];[200u];"chase"];
        [["dogs data"];[200u];"dogs"];
        [["foxes data"];[400u];"dogs"];
        [["foxes data"];[400u];"foxes"];
        [["cats cats data"];[300u];"love"];
        [["foxes data"];[400u];"love"];
        [["cats data"];[100u];"small"];
        [["dogs data"];[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(NoBulkUpsert) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    AddIndex(db);

    { // BulkUpsert
        NYdb::TValueBuilder rows;
        rows.BeginList();
        rows.AddListItem()
            .BeginStruct()
            .AddMember("Key").Uint64(900)
            .EndStruct();
        rows.EndList();
        auto result = kikimr.GetTableClient().BulkUpsert("/Root/Texts", rows.Build()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Only async-indexed tables are supported by BulkUpsert");
    }
    auto index = ReadIndex(db);
    CompareYson(R"([])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(NoIndexImplTableUpdates) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    AddIndex(db);

    { // BulkUpsert
        TString query = R"sql(
            UPSERT INTO `/Root/Texts/fulltext_idx/indexImplTable` (__ydb_token, Key) VALUES
                ("dogs", 901)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Writing to index implementation tables is not allowed");
    }
    auto index = ReadIndex(db);
    CompareYson(R"([])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(Utf8) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    { // CreateTexts
        TString query = R"sql(
            CREATE TABLE `/Root/Texts` (
                Key Uint64,
                Text Utf8,
                PRIMARY KEY (Key)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    { // UpsertTexts
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text) VALUES
                (100, "Мышь спит"),
                (200, "Собака ест")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    AddIndex(db);
    { // UpsertRow
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text) VALUES
                (150, "Кошка ест мышь")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    auto index = ReadIndex(db);
    CompareYson(R"([
        [[150u];"ест"];
        [[200u];"ест"];
        [[150u];"кошка"];
        [[100u];"мышь"];
        [[150u];"мышь"];
        [[200u];"собака"];
        [[100u];"спит"]
    ])", NYdb::FormatResultSetYson(index));

    {
        TString query = R"sql(
            SELECT Key FROM `/Root/Texts/fulltext_idx/indexImplTable`
            WHERE __ydb_token = "ест"
            ORDER BY Key
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[150u]];
            [[200u]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
}
    
}

}
