#include <ydb/core/kqp/ut/indexes/fulltext/kqp_fulltext_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;

TKikimrRunner Kikimr(NKikimrConfig::TFeatureFlags&& featureFlags) {
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    return TKikimrRunner(settings);
}

TKikimrRunner Kikimr() {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    return TKikimrRunner(settings);
}

void CreateTexts(NQuery::TQueryClient& db, const bool utf8) {
    TString query = std::format(R"sql(
        CREATE TABLE `/Root/Texts` (
            Key Uint64,
            Text {0},
            Data {0},
            PRIMARY KEY (Key)
        );
    )sql", utf8 ? "Utf8" : "String");
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void UpsertSomeTexts(NQuery::TQueryClient& db) {
    TString query = R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, "Cats love cats.", "cats data"),
            (200, "Dogs love foxes.", "dogs data")
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

void AddIndex(NQuery::TQueryClient& db, const TString& indexName) {
    TString query = Sprintf(R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
            GLOBAL USING %s
            ON (Text)
            WITH (tokenizer=standard, use_filter_lowercase=true)
    )sql", indexName.c_str());
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void AddIndexNGram(NQuery::TQueryClient& db, const size_t nGramMinLength, const size_t nGramMaxLength,
    const bool relevance, const bool edgeNGram, const bool covered) {
    const TString query = Sprintf(R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
            GLOBAL USING %s
            ON (Text) %s
            WITH (
                tokenizer=standard,
                use_filter_lowercase=true,
                use_filter_ngram=%d,
                use_filter_edge_ngram=%d,
                filter_ngram_min_length=%d,
                filter_ngram_max_length=%d
            );
        )sql",
        relevance ? "fulltext_relevance" : "fulltext_plain",
        covered ? "COVER (Text, Data)" : "",
        !edgeNGram, edgeNGram, nGramMinLength, nGramMaxLength
    );
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void AddIndexCovered(NQuery::TQueryClient& db, const TString& indexName) {
    TString query = Sprintf(R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
            GLOBAL USING %s
            ON (Text) COVER (Data)
            WITH (tokenizer=standard, use_filter_lowercase=true)
    )sql", indexName.c_str());
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void AddIndexSnowball(NQuery::TQueryClient& db, const TString& language) {
    TString query = Sprintf(R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
            GLOBAL USING fulltext_plain
            ON (Text)
            WITH (
                tokenizer=standard,
                use_filter_lowercase=true,
                use_filter_snowball=true,
                language=%s
            )
    )sql", language.c_str());
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

} // namespace NKikimr::NKqp
