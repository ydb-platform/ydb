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
                (152, "Rabbit love foxes.", "rabbit data")
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
        [[152u];"rabbit"];
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
    // TODO: upserts are not implemented
}

Y_UNIT_TEST(UpsertRowCovered) {
    // TODO: upserts are not implemented
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
    // TODO: test update of key, text, data
}

Y_UNIT_TEST(UpdateRowCovered) {
    // TODO: test update of key, text, data
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
    return; // TODO: upserts are not implemented
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
    return; // TODO: upserts are not implemented
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
    
}

}
