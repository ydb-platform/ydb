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

void UpsertTexts(NQuery::TQueryClient& db) {
    TString query = R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, "Cats chase small animals.", "cats data"),
            (200, "Dogs chase small cats.", "dogs data"),
            (300, "Cats love cats.", "cats cats data"),
            (400, "Foxes love dogs.", "fox data")
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void UpsertRow(NQuery::TQueryClient& db) {
    TString query = R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (250, "Dogs are big animals.", "new dogs data")
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
        [["fox data"];[400u];"dogs"];
        [["fox data"];[400u];"foxes"];
        [["cats cats data"];[300u];"love"];
        [["fox data"];[400u];"love"];
        [["cats data"];[100u];"small"];
        [["dogs data"];[200u];"small"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(UpsertRow) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndex(db);
    return; // TODO: upserts are not implemented
    UpsertRow(db);

    auto index = ReadIndex(db);
    CompareYson(R"([
        
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(UpsertRowCovered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    
    CreateTexts(db);
    UpsertTexts(db);
    AddIndexCovered(db);
    return; // TODO: upserts are not implemented
    UpsertRow(db);

    auto index = ReadIndex(db);
    CompareYson(R"([
        
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(InsertRow) {
    // TODO: inserts are not implemented
}

Y_UNIT_TEST(InsertRowCovered) {
    // TODO: inserts are not implemented
}

Y_UNIT_TEST(DeleteRow) {
    // TODO: deletes are not implemented

    // TODO: test delete by key and filter
}

Y_UNIT_TEST(DeleteRowCovered) {
    // TODO: deletes are not implemented

    // TODO: test delete by key and filter
}

Y_UNIT_TEST(UpdateRow) {
    // TODO: deletes are not implemented

    // TODO: test update of key, text, data
}

Y_UNIT_TEST(UpdateRowCovered) {
    // TODO: deletes are not implemented

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
        [["fox data"];[400u];"dogs"];
        [["fox data"];[400u];"foxes"];
        [["cats cats data"];[300u];"love"];
        [["fox data"];[400u];"love"];
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

    { // UpsertRow
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
