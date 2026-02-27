#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpFulltextIndexes) {

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

void CreateTexts(NQuery::TQueryClient& db, const bool utf8 = false) {
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

void AddIndex(NQuery::TQueryClient& db, const TString& indexName = "fulltext_plain") {
    TString query = Sprintf(R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
            GLOBAL USING %s
            ON (Text)
            WITH (tokenizer=standard, use_filter_lowercase=true)
    )sql", indexName.c_str());
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void DoValidateWildcardMatchQuery(NQuery::TQueryClient& db, const TString& query, const TString& expectedResult,
     const TString& likeResult = "", const TString& returnColumns = "`Key`, `Text`")
{
    {
        NYdb::NQuery::TExecuteQuerySettings querySettings;
        querySettings.ClientTimeout(TDuration::Minutes(1));
        TString sql = Sprintf(R"sql(
            SELECT %s FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%s", "Wildcard" as Mode)
            ORDER BY `Key`;
        )sql", returnColumns.c_str(), query.c_str());
        auto result = db.ExecuteQuery(sql, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(expectedResult, NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

    for(bool caseSensitive : {false, true})
    {
        NYdb::NQuery::TExecuteQuerySettings querySettings;
        querySettings.ClientTimeout(TDuration::Minutes(1));
        TString sql = Sprintf(R"sql(
            SELECT %s FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE `Text` %s "%s"
            ORDER BY `Key`;
        )sql", returnColumns.c_str(), caseSensitive ? "LIKE" : "ILIKE", query.c_str());
        auto result = db.ExecuteQuery(sql, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TString expected = (!caseSensitive || likeResult.empty()) ? expectedResult : likeResult;
        CompareYson(expected, NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
}

TString CondenseToYsonString(Ydb::TableStats::QueryStats& stats) {
    std::map<TString, std::pair<ui64, ui64>> statsMap;

    for(const auto& qp: stats.query_phases()) {
        for(const auto& ta : qp.table_access()) {
            std::pair<ui64, ui64>& readStat = statsMap[ta.name()];
            readStat.first += ta.reads().rows();
            readStat.second += ta.reads().bytes();
        }
    }

    TStringStream out;
    NYson::TYsonWriter writer(&out);

    writer.OnBeginList();

    for(const auto& [name, stat]: statsMap) {
        writer.OnListItem();

        writer.OnBeginList();

        writer.OnListItem();
        writer.OnStringScalar(name);

        writer.OnListItem();
        writer.OnUint64Scalar(stat.first);

        writer.OnListItem();
        writer.OnUint64Scalar(stat.second);

        writer.OnEndList();
    }

    writer.OnEndList();

    return out.Str();
}

Ydb::TableStats::QueryStats DoValidateRelevanceSingleQuery(TKikimrRunner& kikimr, NQuery::TQueryClient& db, const TString& relevanceQuery, std::vector<std::pair<ui64, double>> expectedResults) {
    // Get the actual relevance score
    auto settings = NYdb::NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Basic);
    auto result = kikimr.RunCall([&] { return db.ExecuteQuery(
        relevanceQuery, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync(); });

    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSets().size(), 1, "Expected 1 result set");
    auto resultSet = result.GetResultSet(0);
    UNIT_ASSERT_C(resultSet.RowsCount() == expectedResults.size(),
        "Expected " + std::to_string(expectedResults.size()) + " results for query: "
        + "Actual " + std::to_string(resultSet.RowsCount()) + "\n"
        + relevanceQuery);
    NYdb::TResultSetParser parser(resultSet);
    size_t idx = 0;
    while (parser.TryNextRow()) {
        ui64 key = *parser.ColumnParser("Key").GetOptionalUint64();
        double relevance = parser.ColumnParser("Relevance").GetDouble();

        UNIT_ASSERT_C(idx < expectedResults.size(),
            "More results than expected for query: \n" + relevanceQuery);

        auto expectedKey = expectedResults[idx].first;
        auto expectedRelevance = expectedResults[idx].second;

        UNIT_ASSERT_VALUES_EQUAL_C(key, expectedKey,
            "Key mismatch for query '" + relevanceQuery + "' at position " + std::to_string(idx) +
            ": expected " + std::to_string(expectedKey) + ", got " + std::to_string(key));

            // Allow small floating-point differences (similar to Lucene's 0.0001f tolerance)
        UNIT_ASSERT_C(std::abs(relevance - expectedRelevance) < 1e-4,
            "Relevance score mismatch for query '" + relevanceQuery + "' key " + std::to_string(key) +
            ": expected " + std::to_string(expectedRelevance) + ", got " + std::to_string(relevance));

        ++idx;
    }

    UNIT_ASSERT(result.GetStats());
    return NYdb::TProtoAccessor::GetProto(*result.GetStats());
}

void DoValidateRelevanceQuery(NQuery::TQueryClient& db, const TString& relevanceQuery, std::vector<std::pair<std::string, std::vector<std::pair<ui64, double>>>> cases, NYdb::TParamsBuilder params = {}) {
    for (const auto& [query, expectedResults] : cases) {
        // Get the actual relevance score
        auto result = db.ExecuteQuery(
            Sprintf(relevanceQuery.c_str(), query.c_str(), query.c_str()), NYdb::NQuery::TTxControl::NoTx(), params.Build()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSets().size(), 1, "Expected 1 result set");
        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_C(resultSet.RowsCount() == expectedResults.size(),
            "Expected " + std::to_string(expectedResults.size()) + " results for query: " + query);

        NYdb::TResultSetParser parser(resultSet);
        size_t idx = 0;
        while (parser.TryNextRow()) {
            ui64 key = *parser.ColumnParser("Key").GetOptionalUint64();
            double relevance = parser.ColumnParser("Relevance").GetDouble();

            UNIT_ASSERT_C(idx < expectedResults.size(),
                "More results than expected for query: " + query);

            auto expectedKey = expectedResults[idx].first;
            auto expectedRelevance = expectedResults[idx].second;

            UNIT_ASSERT_VALUES_EQUAL_C(key, expectedKey,
                "Key mismatch for query '" + query + "' at position " + std::to_string(idx) +
                ": expected " + std::to_string(expectedKey) + ", got " + std::to_string(key));

            // Allow small floating-point differences (similar to Lucene's 0.0001f tolerance)
            UNIT_ASSERT_C(std::abs(relevance - expectedRelevance) < 1e-4,
                "Relevance score mismatch for query '" + query + "' key " + std::to_string(key) +
                ": expected " + std::to_string(expectedRelevance) + ", got " + std::to_string(relevance));

            ++idx;
        }
    }
};


void AddIndexNGram(NQuery::TQueryClient& db, const size_t nGramMinLength = 3, const size_t nGramMaxLength = 3,
    const bool relevance = false, const bool edgeNGram = false, const bool covered = false) {
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

void AddIndexCovered(NQuery::TQueryClient& db, const TString& indexName = "fulltext_plain") {
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

void DropIndex(NQuery::TQueryClient& db) {
    TString query = R"sql(
        ALTER TABLE `/Root/Texts` DROP INDEX `fulltext_idx`;
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

TResultSet ReadIndex(NQuery::TQueryClient& db, const char* table = "indexImplTable") {
    TString query = Sprintf(R"sql(
        SELECT * FROM `/Root/Texts/fulltext_idx/%s`;
    )sql", table);
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return result.GetResultSet(0);
}

void TruncateTable(NQuery::TQueryClient& db) {
    TString query = R"sql(
        TRUNCATE TABLE `/Root/Texts`;
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

Y_UNIT_TEST(AddIndex) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
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
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
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
    AddIndexNGram(db, 3, 3, false, true);

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

Y_UNIT_TEST_TWIN(AddIndexWithRelevance, Covered) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);
    if (Covered)
        AddIndexCovered(db, "fulltext_relevance");
    else
        AddIndex(db, "fulltext_relevance");
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];1u;"animals"];
        [[100u];1u;"cats"];
        [[200u];1u;"cats"];
        [[300u];2u;"cats"];
        [[100u];1u;"chase"];
        [[200u];1u;"chase"];
        [[200u];1u;"dogs"];
        [[400u];1u;"dogs"];
        [[400u];1u;"foxes"];
        [[300u];1u;"love"];
        [[400u];1u;"love"];
        [[100u];1u;"small"];
        [[200u];1u;"small"]
    ])", NYdb::FormatResultSetYson(index));

    index = ReadIndex(db, NTableIndex::NFulltext::DocsTable);
    if (Covered) {
        CompareYson(R"([
            [["cats data"];[100u];4u];
            [["dogs data"];[200u];4u];
            [["cats cats data"];[300u];3u];
            [["foxes data"];[400u];3u]
        ])", NYdb::FormatResultSetYson(index));
    } else {
        CompareYson(R"([
            [[100u];4u];
            [[200u];4u];
            [[300u];3u];
            [[400u];3u]
        ])", NYdb::FormatResultSetYson(index));
    }

    index = ReadIndex(db, NTableIndex::NFulltext::DictTable);
    CompareYson(R"([
        [1u;"animals"];
        [3u;"cats"];
        [2u;"chase"];
        [2u;"dogs"];
        [1u;"foxes"];
        [2u;"love"];
        [2u;"small"]
    ])", NYdb::FormatResultSetYson(index));

    index = ReadIndex(db, NTableIndex::NFulltext::StatsTable);
    CompareYson(R"([
        [4u;0u;14u]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST(AddIndexWithRelevanceSettings) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);

    auto tableClient = kikimr.GetTableClient();
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    {
        Ydb::Table::FulltextIndexSettings fulltextSettings;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
            columns {
                column: "Text"
                analyzers {
                    tokenizer: STANDARD
                    use_filter_lowercase: true
                }
            }
        )", &fulltextSettings));
        Ydb::Table::GlobalIndexSettings wordSettings;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(R"(
            partition_at_keys {
                split_points {
                    type { tuple_type {
                        elements { optional_type { item { type_id: STRING } } }
                    } }
                    value {
                        items { bytes_value: "love" }
                    }
                }
            }
        )", &wordSettings));
        Ydb::Table::GlobalIndexSettings emptySettings;
        TVector<NYdb::NTable::TGlobalIndexSettings> partitionSettings;
        partitionSettings.emplace_back(NYdb::NTable::TGlobalIndexSettings::FromProto(wordSettings));
        partitionSettings.emplace_back(NYdb::NTable::TGlobalIndexSettings::FromProto(emptySettings));
        partitionSettings.emplace_back(NYdb::NTable::TGlobalIndexSettings::FromProto(emptySettings));
        partitionSettings.emplace_back(NYdb::NTable::TGlobalIndexSettings::FromProto(wordSettings));

        auto addIndex = TAlterTableSettings()
            .AppendAddIndexes(NYdb::NTable::TIndexDescription(
                "fulltext_idx",
                EIndexType::GlobalFulltextRelevance,
                {"Text"},
                {},
                partitionSettings,
                NYdb::NTable::TFulltextIndexSettings::FromProto(fulltextSettings)
            ));
        auto result = session.AlterTable("/Root/Texts", addIndex).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
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
                (151, "Wolves love foxes.", "cows data")
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
        [[151u];"wolves"]
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
        [["dogs data"];[200u];"dogs"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["foxes data"];[150u];"foxes"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["foxes data"];[150u];"foxes"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["dogs data"];[200u];"love"]
    ])", NYdb::FormatResultSetYson(index));
}

// Checks INSERT both with and without RETURNING
Y_UNIT_TEST_QUAD(InsertRowsWithRelevance, Covered, UseUpsert) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_ACTOR, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertSomeTexts(db);
    if (Covered)
        AddIndexCovered(db, "fulltext_relevance");
    else
        AddIndex(db, "fulltext_relevance");
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];2u;"cats"];
        [[200u];1u;"dogs"];
        [[200u];1u;"foxes"];
        [[100u];1u;"love"];
        [[200u];1u;"love"]
    ])", NYdb::FormatResultSetYson(index));
    auto dict = ReadIndex(db, NTableIndex::NFulltext::DictTable);
    CompareYson(R"([
        [1u;"cats"];
        [1u;"dogs"];
        [1u;"foxes"];
        [2u;"love"]
    ])", NYdb::FormatResultSetYson(dict));
    auto docs = ReadIndex(db, NTableIndex::NFulltext::DocsTable);
    if (Covered) {
        CompareYson(R"([
            [["cats data"];[100u];3u];
            [["dogs data"];[200u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    } else {
        CompareYson(R"([
            [[100u];3u];
            [[200u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    }
    auto stats = ReadIndex(db, NTableIndex::NFulltext::StatsTable);
    CompareYson(R"([[2u;0u;6u]])", NYdb::FormatResultSetYson(stats));

    { // Insert/upsert a new row
        TString query = Sprintf(R"sql(
            %s INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
            RETURNING *
        )sql", UseUpsert ? "UPSERT" : "INSERT");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["foxes data"];[150u];["Foxes love cats."]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];2u;"cats"];
        [[150u];1u;"cats"];
        [[200u];1u;"dogs"];
        [[150u];1u;"foxes"];
        [[200u];1u;"foxes"];
        [[100u];1u;"love"];
        [[150u];1u;"love"];
        [[200u];1u;"love"]
    ])", NYdb::FormatResultSetYson(index));
    dict = ReadIndex(db, NTableIndex::NFulltext::DictTable);
    CompareYson(R"([
        [2u;"cats"];
        [1u;"dogs"];
        [2u;"foxes"];
        [3u;"love"]
    ])", NYdb::FormatResultSetYson(dict));
    docs = ReadIndex(db, NTableIndex::NFulltext::DocsTable);
    if (Covered) {
        CompareYson(R"([
            [["cats data"];[100u];3u];
            [["foxes data"];[150u];3u];
            [["dogs data"];[200u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    } else {
        CompareYson(R"([
            [[100u];3u];
            [[150u];3u];
            [[200u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    }
    stats = ReadIndex(db, NTableIndex::NFulltext::StatsTable);
    CompareYson(R"([[3u;0u;9u]])", NYdb::FormatResultSetYson(stats));

    { // Insert/upsert more rows - now without RETURNING
        TString query = Sprintf(R"sql(
            %s INTO `/Root/Texts` (Key, Text, Data) VALUES
                (151, "Wolves love foxes.", "cows data"),
                (152, "Rabbits love foxes.", "rabbit data")
        )sql", UseUpsert ? "UPSERT" : "INSERT");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];2u;"cats"];
        [[150u];1u;"cats"];
        [[200u];1u;"dogs"];
        [[150u];1u;"foxes"];
        [[151u];1u;"foxes"];
        [[152u];1u;"foxes"];
        [[200u];1u;"foxes"];
        [[100u];1u;"love"];
        [[150u];1u;"love"];
        [[151u];1u;"love"];
        [[152u];1u;"love"];
        [[200u];1u;"love"];
        [[152u];1u;"rabbits"];
        [[151u];1u;"wolves"]
    ])", NYdb::FormatResultSetYson(index));
    dict = ReadIndex(db, NTableIndex::NFulltext::DictTable);
    CompareYson(R"([
        [2u;"cats"];
        [1u;"dogs"];
        [4u;"foxes"];
        [5u;"love"];
        [1u;"rabbits"];
        [1u;"wolves"]
    ])", NYdb::FormatResultSetYson(dict));
    docs = ReadIndex(db, NTableIndex::NFulltext::DocsTable);
    if (Covered) {
        CompareYson(R"([
            [["cats data"];[100u];3u];
            [["foxes data"];[150u];3u];
            [["cows data"];[151u];3u];
            [["rabbit data"];[152u];3u];
            [["dogs data"];[200u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    } else {
        CompareYson(R"([
            [[100u];3u];
            [[150u];3u];
            [[151u];3u];
            [[152u];3u];
            [[200u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    }
    stats = ReadIndex(db, NTableIndex::NFulltext::StatsTable);
    CompareYson(R"([[5u;0u;15u]])", NYdb::FormatResultSetYson(stats));
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
                (151, "Wolves love foxes.", "cows data")
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
        [[151u];"wolves"]
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
        [[151u];"wolves"]
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
        [["dogs data"];[200u];"dogs"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["foxes data"];[150u];"foxes"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["birds data"];[100u];"foxes"];
        [["foxes data"];[150u];"foxes"];
        [["dogs data"];[200u];"foxes"];
        [["birds data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["foxes data"];[150u];"foxes"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["dogs data"];[200u];"love"]
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

Y_UNIT_TEST_TWIN(UpsertWithRelevance, Covered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertSomeTexts(db);
    if (Covered)
        AddIndexCovered(db, "fulltext_relevance");
    else
        AddIndex(db, "fulltext_relevance");
    auto dict = ReadIndex(db, NTableIndex::NFulltext::DictTable);
    CompareYson(R"([
        [1u;"cats"];
        [1u;"dogs"];
        [1u;"foxes"];
        [2u;"love"]
    ])", NYdb::FormatResultSetYson(dict));
    // Dataset is the same as in InsertWithRelevance - don't check index table contents

    // Pure upsert of new rows is already checked in InsertWithRelevance

    { // Upsert a mix of new and updated rows
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (100, "Birds love rabbits.", "birds data"),
                (150, "Foxes love cats.", "foxes data"),
                (151, "Wolves love foxes.", "cows data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];1u;"birds"];
        [[150u];1u;"cats"];
        [[200u];1u;"dogs"];
        [[150u];1u;"foxes"];
        [[151u];1u;"foxes"];
        [[200u];1u;"foxes"];
        [[100u];1u;"love"];
        [[150u];1u;"love"];
        [[151u];1u;"love"];
        [[200u];1u;"love"];
        [[100u];1u;"rabbits"];
        [[151u];1u;"wolves"]
    ])", NYdb::FormatResultSetYson(index));
    dict = ReadIndex(db, NTableIndex::NFulltext::DictTable);
    CompareYson(R"([
        [1u;"birds"];
        [1u;"cats"];
        [1u;"dogs"];
        [3u;"foxes"];
        [4u;"love"];
        [1u;"rabbits"];
        [1u;"wolves"]
    ])", NYdb::FormatResultSetYson(dict));
    auto docs = ReadIndex(db, NTableIndex::NFulltext::DocsTable);
    if (Covered) {
        CompareYson(R"([
            [["birds data"];[100u];3u];
            [["foxes data"];[150u];3u];
            [["cows data"];[151u];3u];
            [["dogs data"];[200u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    } else {
        CompareYson(R"([
            [[100u];3u];
            [[150u];3u];
            [[151u];3u];
            [[200u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    }
    auto stats = ReadIndex(db, NTableIndex::NFulltext::StatsTable);
    CompareYson(R"([[4u;0u;12u]])", NYdb::FormatResultSetYson(stats));
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
                (151, "Wolves love foxes.", "cows data")
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
        [[151u];"wolves"]
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
        [[151u];"wolves"]
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
        [["dogs data"];[200u];"dogs"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["foxes data"];[150u];"foxes"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["birds data"];[100u];"foxes"];
        [["foxes data"];[150u];"foxes"];
        [["dogs data"];[200u];"foxes"];
        [["birds data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["foxes data"];[150u];"foxes"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["foxes data"];[150u];"love"];
        [["dogs data"];[200u];"love"]
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

Y_UNIT_TEST_TWIN(DeleteRowWithRelevance, Covered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);
    if (Covered)
        AddIndexCovered(db, "fulltext_relevance");
    else
        AddIndex(db, "fulltext_relevance");
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];1u;"animals"];
        [[100u];1u;"cats"];
        [[200u];1u;"cats"];
        [[300u];2u;"cats"];
        [[100u];1u;"chase"];
        [[200u];1u;"chase"];
        [[200u];1u;"dogs"];
        [[400u];1u;"dogs"];
        [[400u];1u;"foxes"];
        [[300u];1u;"love"];
        [[400u];1u;"love"];
        [[100u];1u;"small"];
        [[200u];1u;"small"]
    ])", NYdb::FormatResultSetYson(index));
    auto dict = ReadIndex(db, NTableIndex::NFulltext::DictTable);
    CompareYson(R"([
        [1u;"animals"];
        [3u;"cats"];
        [2u;"chase"];
        [2u;"dogs"];
        [1u;"foxes"];
        [2u;"love"];
        [2u;"small"]
    ])", NYdb::FormatResultSetYson(dict));
    auto docs = ReadIndex(db, NTableIndex::NFulltext::DocsTable);
    if (Covered) {
        CompareYson(R"([
            [["cats data"];[100u];4u];
            [["dogs data"];[200u];4u];
            [["cats cats data"];[300u];3u];
            [["foxes data"];[400u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    } else {
        CompareYson(R"([
            [[100u];4u];
            [[200u];4u];
            [[300u];3u];
            [[400u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    }
    auto stats = ReadIndex(db, NTableIndex::NFulltext::StatsTable);
    CompareYson(R"([[4u;0u;14u]])", NYdb::FormatResultSetYson(stats));

    { // DeleteRow by PK
        TString query = R"sql(
            DELETE FROM `/Root/Texts` WHERE Key = 200;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];1u;"animals"];
        [[100u];1u;"cats"];
        [[300u];2u;"cats"];
        [[100u];1u;"chase"];
        [[400u];1u;"dogs"];
        [[400u];1u;"foxes"];
        [[300u];1u;"love"];
        [[400u];1u;"love"];
        [[100u];1u;"small"]
    ])", NYdb::FormatResultSetYson(index));
    dict = ReadIndex(db, NTableIndex::NFulltext::DictTable);
    CompareYson(R"([
        [1u;"animals"];
        [2u;"cats"];
        [1u;"chase"];
        [1u;"dogs"];
        [1u;"foxes"];
        [2u;"love"];
        [1u;"small"]
    ])", NYdb::FormatResultSetYson(dict));
    docs = ReadIndex(db, NTableIndex::NFulltext::DocsTable);
    if (Covered) {
        CompareYson(R"([
            [["cats data"];[100u];4u];
            [["cats cats data"];[300u];3u];
            [["foxes data"];[400u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    } else {
        CompareYson(R"([
            [[100u];4u];
            [[300u];3u];
            [[400u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    }
    stats = ReadIndex(db, NTableIndex::NFulltext::StatsTable);
    CompareYson(R"([[3u;0u;10u]])", NYdb::FormatResultSetYson(stats));

    { // DeleteRow by filter
        TString query = R"sql(
            DELETE FROM `/Root/Texts` WHERE Data = "foxes data";
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];1u;"animals"];
        [[100u];1u;"cats"];
        [[300u];2u;"cats"];
        [[100u];1u;"chase"];
        [[300u];1u;"love"];
        [[100u];1u;"small"]
    ])", NYdb::FormatResultSetYson(index));
    dict = ReadIndex(db, NTableIndex::NFulltext::DictTable);
    CompareYson(R"([
        [1u;"animals"];
        [2u;"cats"];
        [1u;"chase"];
        [0u;"dogs"];
        [0u;"foxes"];
        [1u;"love"];
        [1u;"small"]
    ])", NYdb::FormatResultSetYson(dict));
    docs = ReadIndex(db, NTableIndex::NFulltext::DocsTable);
    if (Covered) {
        CompareYson(R"([
            [["cats data"];[100u];4u];
            [["cats cats data"];[300u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    } else {
        CompareYson(R"([
            [[100u];4u];
            [[300u];3u]
        ])", NYdb::FormatResultSetYson(docs));
    }
    stats = ReadIndex(db, NTableIndex::NFulltext::StatsTable);
    CompareYson(R"([[2u;0u;7u]])", NYdb::FormatResultSetYson(stats));

    { // DeleteRow by ON
        TString query = R"sql(
            DELETE FROM `/Root/Texts` ON SELECT 300 AS `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    index = ReadIndex(db);
    CompareYson(R"([
        [[100u];1u;"animals"];
        [[100u];1u;"cats"];
        [[100u];1u;"chase"];
        [[100u];1u;"small"]
    ])", NYdb::FormatResultSetYson(index));
    dict = ReadIndex(db, NTableIndex::NFulltext::DictTable);
    CompareYson(R"([
        [1u;"animals"];
        [1u;"cats"];
        [1u;"chase"];
        [0u;"dogs"];
        [0u;"foxes"];
        [0u;"love"];
        [1u;"small"]
    ])", NYdb::FormatResultSetYson(dict));
    docs = ReadIndex(db, NTableIndex::NFulltext::DocsTable);
    if (Covered) {
        CompareYson(R"([
            [["cats data"];[100u];4u]
        ])", NYdb::FormatResultSetYson(docs));
    } else {
        CompareYson(R"([
            [[100u];4u]
        ])", NYdb::FormatResultSetYson(docs));
    }
    stats = ReadIndex(db, NTableIndex::NFulltext::StatsTable);
    CompareYson(R"([[1u;0u;4u]])", NYdb::FormatResultSetYson(stats));
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
        [["dogs data"];[200u];"dogs"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["cats data"];[100u];"foxes"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["birds data"];[100u];"foxes"];
        [["dogs data"];[200u];"foxes"];
        [["birds data"];[100u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["cats data"];[100u];"foxes"];
        [["dogs data"];[200u];"foxes"];
        [["cats data"];[100u];"love"];
        [["dogs data"];[200u];"love"]
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
        [["dogs data"];[200u];"dogs"];
        [["birds data"];[100u];"foxes"];
        [["dogs data"];[200u];"foxes"];
        [["birds data"];[100u];"love"];
        [["dogs data"];[200u];"love"]
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
                    GLOBAL USING fulltext_plain
                    ON (Text)
                    WITH (tokenizer=standard, use_filter_lowercase=true)
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
                    GLOBAL USING fulltext_plain
                    ON (Text) COVER (Data)
                    WITH (tokenizer=standard, use_filter_lowercase=true)
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

Y_UNIT_TEST_TWIN(SelectWithFulltextMatch, UTF8) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    { // Create table with fulltext index
        TString query = Sprintf(R"sql(
            CREATE TABLE `/Root/Texts` (
                Key Uint64,
                Text %s,
                PRIMARY KEY (Key),
                INDEX fulltext_idx
                    GLOBAL USING fulltext_plain
                    ON (Text)
                    WITH (tokenizer=standard, use_filter_lowercase=true)
            );
        )sql", UTF8 ? "Utf8" : "String");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    { // Insert data with Russian text about machine learning
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text) VALUES
                (1, "Машинное обучение - это важная область искусственного интеллекта"),
                (2, "Глубокое обучение является подмножеством машинного обучения"),
                (3, "Нейронные сети используются в машинном обучении"),
                (4, "Машинное обучение помогает решать сложные задачи"),
                (5, "Алгоритмы машинного обучения обрабатывают большие данные"),
                (6, "Обучение моделей требует много вычислительных ресурсов"),
                (7, "Машинное обучение применяется в различных областях"),
                (8, "Современные методы машинного обучения очень эффективны"),
                (9, "Исследования в области машинного обучения продолжаются"),
                (10, "Практическое применение машинного обучения растет"),
                (11, "Коты любят играть"),
                (12, "Собаки любят бегать")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    std::vector<std::pair<std::string, std::vector<ui64>>> searchingTerms = {
        {"машинное обучение", {1, 4, 7}},
        {"моделей", {6}},
        {"собаки любят ", {12}},
        {"коты любят", {11}},
        {"тигры", {}},
    };

    for(const auto& [term, expectedKeys] : searchingTerms) { // Query with WHERE clause using FulltextMatch UDF
        TString query = Sprintf(R"sql(
            SELECT Key, Text FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(Text, "%s")
            ORDER BY Key
        )sql", term.c_str());
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_C(resultSet.RowsCount() == expectedKeys.size(), "Expected " + std::to_string(expectedKeys.size()) + " rows with " + term + " content");

        // Verify that all returned rows actually contain the search term
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto bodyValue = UTF8 ? parser.ColumnParser("Text").GetOptionalUtf8() : parser.ColumnParser("Text").GetOptionalString();
            ui64 key = *parser.ColumnParser("Key").GetOptionalUint64();
            UNIT_ASSERT_C(bodyValue, "Text should not be null");
            Cerr << "Key: " << key << Endl;
            UNIT_ASSERT_C(
                IsIn(expectedKeys, key),
                "All returned rows should contain search term related text"
            );
        }
    }

    for(const auto& [term, expectedKeys] : searchingTerms) { // Query with WHERE clause using FulltextMatch UDF
        TString query = Sprintf(R"sql(
            SELECT Key FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(Text, "%s")
            ORDER BY Key
        )sql", term.c_str());
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_C(resultSet.RowsCount() == expectedKeys.size(), "Expected " + std::to_string(expectedKeys.size()) + " rows with " + term + " content");

        // Verify that all returned rows actually contain the search term
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            ui64 key = *parser.ColumnParser("Key").GetOptionalUint64();
            Cerr << "Key: " << key << Endl;
            UNIT_ASSERT_C(
                IsIn(expectedKeys, key),
                "All returned rows should contain machine learning related text"
            );
        }
    }

    {
        TString query = Sprintf(R"sql(
            SELECT Key, Text FROM `/Root/Texts`
            WHERE FulltextMatch(Text, "машинное обучение")
        )sql");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        Cerr << "Result: " << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST(SelectWithFulltextMatchEmpty) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    AddIndex(db);

    const auto querySettings = NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(10));

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "404 not found")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

    UpsertSomeTexts(db);

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "404 not found")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
}

Y_UNIT_TEST(SelectWithFulltextMatchUnsupportedQueries) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    AddIndex(db, "fulltext_relevance");

    const auto querySettings = NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(10));

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "404 not found") OR Text = "404 not found"
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "FulltextMatch/FulltextScore node is not reachable by conjunctions.");
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "404 not found") AND Text = "404 not found"
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE NOT FulltextMatch(`Text`, "404 not found")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "FulltextMatch/FulltextScore node is not reachable by conjunctions.");
    }

    // temporary forbidden to use multiple fulltext match expressions in the same query.
    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "404 not found") AND FulltextScore(Text, "205 not found") > 0
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Multiple fulltext predicates in a single read are not supported.");
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "404 not found") AND FulltextMatch(Text, "205 not found")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(`Text`, "404 not found") > 0 AND FulltextScore(Text, "205 not found") > 0
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Multiple fulltext score predicates in a single read are not supported.");
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE Text = "404 not found"
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts`
            WHERE FulltextMatch(`Text`, "404 not found")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Fulltext index is not specified or unsupported predicate is used to access index");
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`, FulltextScore(`Text`, "404 not found") as Score
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(`Text`, "404 not found") > 0
            ORDER BY `Score` DESC;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`, FulltextScore(`Text`, "404 not found") as Score
            FROM `/Root/Texts` VIEW `fulltext_idx`
            ORDER BY `Score` DESC;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Score restriction is not found in the predicate. It's required to put FulltextScore() > 0 constraint in the where clause.");
    }
}


Y_UNIT_TEST(SelectWithFulltextMatchWithoutTextField) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);

    TString query = R"sql(
        SELECT `Key`
        FROM `/Root/Texts` VIEW `fulltext_idx`
        WHERE FulltextMatch(`Text`, "dogs")
        ORDER BY `Key`;
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

Y_UNIT_TEST(SelectWithFulltextRelevanceB1FactorAndK1Factor) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    { // Create table with fulltext index
        TString query = Sprintf(R"sql(
            CREATE TABLE `/Root/Texts` (
                Key Uint64,
                Text String,
                PRIMARY KEY (Key)
            );
        )sql");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    { // Insert data with Russian text about machine learning
        TString query = Sprintf(R"sql(
            UPSERT INTO `/Root/Texts` (Key, Text) VALUES
                (1, "Машинное обучение - это важная область искусственного интеллекта"),
                (2, "Глубокое обучение является подмножеством машинного обучения"),
                (3, "Нейронные сети используются в машинном обучении"),
                (4, "Машинное обучение помогает решать сложные задачи"),
                (5, "Алгоритмы машинного обучения обрабатывают большие данные"),
                (6, "Обучение моделей требует много вычислительных ресурсов"),
                (7, "Машинное обучение применяется в различных областях"),
                (8, "Современные методы машинного обучения очень эффективны"),
                (9, "Исследования в области машинного обучения продолжаются"),
                (10, "Практическое применение машинного обучения растет"),
                (11, "Коты любят играть"),
                (12, "Собаки любят бегать")
        )sql");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = Sprintf(R"sql(
            ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_relevance
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        )sql");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    DoValidateRelevanceQuery(db,
        R"sql(
            SELECT Key, Text, FulltextScore(Text, "%s", 1.2 as B) as Relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "%s", 1.2 as B) > 0
            ORDER BY Relevance DESC
            LIMIT 10
        )sql", { {"собаки любят ", { {12, 2.464092448}, } } });

    DoValidateRelevanceQuery(db,
        R"sql(
            SELECT Key, Text, FulltextScore(Text, "%s", 1.0 as B) as Relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "%s", 1.0 as B) > 0
            ORDER BY Relevance DESC
            LIMIT 10
        )sql", { {"собаки любят ", { {12, 2.301624815}, } } });

    DoValidateRelevanceQuery(db,
        R"sql(
            SELECT Key, Text, FulltextScore(Text, "%s", 0.8 as B) as Relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "%s", 0.8 as B) > 0
            ORDER BY Relevance DESC
            LIMIT 10
        )sql", { {"собаки любят ", { {12, 2.159256269}, } } });

    DoValidateRelevanceQuery(db,
        R"sql(
            SELECT Key, Text, FulltextScore(Text, "%s", 0.75 as K1, 1.2 as B) as Relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "%s", 0.75 as K1, 1.2 as B) > 0
            ORDER BY Relevance DESC
            LIMIT 10
        )sql", { {"собаки любят ", { {12, 2.839970958}, } } });

    DoValidateRelevanceQuery(db,
        R"sql(
            SELECT Key, Text, FulltextScore(Text, "%s", 0.8 as K1, 1.0 as B) as Relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "%s", 0.8 as K1, 1.0 as B) > 0
            ORDER BY Relevance DESC
            LIMIT 10
        )sql", { {"собаки любят ", { {12, 2.65123871}, } } });

    DoValidateRelevanceQuery(db,
        R"sql(
            SELECT Key, Text, FulltextScore(Text, "%s", 0.9 as K1, 0.8 as B) as Relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "%s", 0.9 as K1, 0.8 as B) > 0
            ORDER BY Relevance DESC
            LIMIT 10
        )sql", { {"собаки любят ", { {12, 2.421362522}, } } });

    DoValidateRelevanceQuery(db,
        R"sql(
            DECLARE $bfactor as Double;
            DECLARE $k1factor as Double;
            SELECT Key, Text, FulltextScore(Text, "собаки любят", $bfactor as B, $k1factor as K1) as Relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "собаки любят", $bfactor as B, $k1factor as K1) > 0
            ORDER BY Relevance DESC
            LIMIT 10
        )sql", { {"собаки любят ", { {12, 2.839970958}, } } },
        std::move(NYdb::TParamsBuilder().AddParam("$bfactor").Double(1.2).Build().AddParam("$k1factor").Double(0.75).Build()));

}

Y_UNIT_TEST_TWIN(SelectWithFulltextRelevance, UTF8) {
    // If UTF8 is true, the column order produced by full text source
    // is the "Text", "_yql_fulltext_relevance"
    // If UTF8 is false, the column order produced by full text source
    // is the "_yql_fulltext_relevance", "text"
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    { // Create table with fulltext index
        TString query = Sprintf(R"sql(
            CREATE TABLE `/Root/Texts` (
                Key Uint64,
                %s %s,
                PRIMARY KEY (Key)
            );
        )sql", UTF8 ? "text" : "Text", UTF8 ? "Utf8" : "String");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    { // Insert data with Russian text about machine learning
        TString query = Sprintf(R"sql(
            UPSERT INTO `/Root/Texts` (Key, %s) VALUES
                (1, "Машинное обучение - это важная область искусственного интеллекта"),
                (2, "Глубокое обучение является подмножеством машинного обучения"),
                (3, "Нейронные сети используются в машинном обучении"),
                (4, "Машинное обучение помогает решать сложные задачи"),
                (5, "Алгоритмы машинного обучения обрабатывают большие данные"),
                (6, "Обучение моделей требует много вычислительных ресурсов"),
                (7, "Машинное обучение применяется в различных областях"),
                (8, "Современные методы машинного обучения очень эффективны"),
                (9, "Исследования в области машинного обучения продолжаются"),
                (10, "Практическое применение машинного обучения растет"),
                (11, "Коты любят играть"),
                (12, "Собаки любят бегать")
        )sql", UTF8 ? "text" : "Text");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    std::vector<std::pair<std::string, std::vector<ui64>>> searchingTerms = {
        {"машинное обучение", {1, 4, 7}},
        {"моделей", {6}},
        {"собаки любят ", {12}},
        {"коты любят", {11}}
    };

    {
        TString query = Sprintf(R"sql(
            ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_relevance
                ON (%s)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        )sql", UTF8 ? "text" : "Text");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    for(const auto& [term, expectedKeys] : searchingTerms) { // Query with WHERE clause using FulltextMatch UDF
        TString query = Sprintf(R"sql(
            SELECT Key, %s, FulltextScore(%s, "%s") as Relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(%s, "%s") > 0
            ORDER BY Relevance DESC
            LIMIT 10
        )sql", UTF8 ? "text" : "Text", UTF8 ? "text" : "Text", term.c_str(), UTF8 ? "text" : "Text", term.c_str());

        Cerr << "Query: " << query << Endl;

        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_C(resultSet.RowsCount() == expectedKeys.size(), "Expected " + std::to_string(expectedKeys.size()) + " rows with " + term + " content" + " but got " + std::to_string(resultSet.RowsCount()));

        // Verify that all returned rows actually contain the search term
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto bodyValue = UTF8 ? parser.ColumnParser("text").GetOptionalUtf8() : parser.ColumnParser("Text").GetOptionalString();
            ui64 key = *parser.ColumnParser("Key").GetOptionalUint64();
            UNIT_ASSERT_C(bodyValue, "Body should not be null");
            Cerr << "Key: " << key << Endl;
            UNIT_ASSERT_C(
                IsIn(expectedKeys, key),
                "All returned rows should contain search term related text"
            );
        }
    }

    for(const auto& [term, expectedKeys] : searchingTerms) { // Query with WHERE clause using FulltextMatch UDF
        TString query = Sprintf(R"sql(
            SELECT Key, %s, FulltextScore(%s, "%s") as Relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(%s, "%s") > 0
            ORDER BY Relevance DESC
            LIMIT 10
        )sql", UTF8 ? "text" : "Text", UTF8 ? "text" : "Text", term.c_str(), UTF8 ? "text" : "Text", term.c_str());

        Cerr << "Query: " << query << Endl;

        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_C(resultSet.RowsCount() == expectedKeys.size(), "Expected " + std::to_string(expectedKeys.size()) + " rows with " + term + " content");

        // Verify that all returned rows actually contain the search term
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto bodyValue = UTF8 ? parser.ColumnParser("text").GetOptionalUtf8() : parser.ColumnParser("Text").GetOptionalString();
            ui64 key = *parser.ColumnParser("Key").GetOptionalUint64();
            double relevance = parser.ColumnParser("Relevance").GetDouble();
            UNIT_ASSERT_C(bodyValue, "Body should not be null");
            Cerr << "Key: " << key << ", Relevance: " << relevance << Endl;
            UNIT_ASSERT_C(
                IsIn(expectedKeys, key),
                "All returned rows should contain search term related text"
            );
        }
    }

    {
        TString query = Sprintf(R"sql(
            DECLARE $query as String;
            SELECT Key, %s, FulltextScore(%s, $query) as relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(%s, $query) > 0
            ORDER BY relevance DESC
        )sql", UTF8 ? "text" : "Text", UTF8 ? "text" : "Text", UTF8 ? "text" : "Text", UTF8 ? "text" : "Text");

        auto params = NYdb::TParamsBuilder();
        params
            .AddParam("$query")
                .String("машинное обучение")
                .Build();
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params.Build()).ExtractValueSync();
        Cerr << "Result: " << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = Sprintf(R"sql(
            SELECT Key, %s FROM `/Root/Texts`
            WHERE FulltextScore(%s, "машинное обучение") > 0
            ORDER BY FulltextScore(%s, "машинное обучение") DESC
        )sql", UTF8 ? "text" : "Text", UTF8 ? "text" : "Text", UTF8 ? "text" : "Text");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        Cerr << "Result: " << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Fulltext index is not specified or unsupported predicate is used to access index");
    }
}

Y_UNIT_TEST(LuceneRelevanceComparison) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    // Create table with fulltext index using relevance layout
    TString createQuery = R"sql(
        CREATE TABLE `/Root/Texts` (
            Key Uint64,
            Text String,
            PRIMARY KEY (Key)
        );
    )sql";
    auto result = db.ExecuteQuery(createQuery, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    // Insert exact documents from Lucene test
    TString insertQuery = R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text) VALUES
            (0, "the quick brown fox jumps over the lazy dog"),
            (1, "quick quick fox"),
            (2, "lazy dog sleeps"),
            (3, "brown bear eats honey"),
            (4, "xylophone music is rare")
    )sql";
    result = db.ExecuteQuery(insertQuery, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    // Add fulltext index with relevance
    TString indexQuery = R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
            GLOBAL USING fulltext_relevance
            ON (Text)
            WITH (tokenizer=standard, use_filter_lowercase=true)
    )sql";
    result = db.ExecuteQuery(indexQuery, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    // Test queries with exact relevance scores from Lucene BM25
    std::vector<std::pair<std::string, std::vector<std::pair<ui64, double>>>> testCases = {
        {"quick fox", {
            {1, 1.0704575},  // doc1: "quick quick fox"
            {0, 0.5720391}   // doc0: "the quick brown fox jumps over the lazy dog"
        }},
        {"lazy dog", {
            {2, 0.92791617}, // doc2: "lazy dog sleeps"
            {0, 0.5720391}   // doc0: "the quick brown fox jumps over the lazy dog"
        }},
        {"brown fox", {
            {0, 0.5720391}, // doc0: "the quick brown fox jumps over the lazy dog"
            {1, 0.46395808}, // doc1: "quick quick fox"
            {3, 0.42037117}  // doc3: "brown bear eats honey"
        }},
        {"honey", {
            {3, 0.66565275}  // doc3: "brown bear eats honey"
        }},
        {"xylophone rare", {
            {4, 1.3313055}   // doc4: "xylophone music is rare"
        }}
    };

    DoValidateRelevanceQuery(db,
        R"sql(
            SELECT Key, FulltextScore(Text, "%s", "or" as DefaultOperator, "1" as MinimumShouldMatch) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "%s", "or" as DefaultOperator, "1" as MinimumShouldMatch) > 0
            ORDER BY Relevance DESC
        )sql",
        testCases);

    DoValidateRelevanceQuery(db,
        R"sql(
            SELECT Key, FulltextScore(Text, "%s", "or" as DefaultOperator, "50%" as MinimumShouldMatch) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "%s", "or" as DefaultOperator, "50%" as MinimumShouldMatch) > 0
            ORDER BY Relevance DESC
        )sql",
        testCases);

    DoValidateRelevanceQuery(db,
        R"sql(
            SELECT Key, FulltextScore(Text, "%s", "or" as DefaultOperator, "-1" as MinimumShouldMatch) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "%s", "or" as DefaultOperator, "-1" as MinimumShouldMatch) > 0
            ORDER BY Relevance DESC
        )sql",
        testCases);

    DoValidateRelevanceQuery(db,
        R"sql(
            SELECT Key, FulltextScore(Text, "%s", "or" as DefaultOperator, "-100" as MinimumShouldMatch) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "%s", "or" as DefaultOperator, "-100" as MinimumShouldMatch) > 0
            ORDER BY Relevance DESC
        )sql",
        testCases);

    std::vector<std::pair<std::string, std::vector<std::pair<ui64, double>>>> andTestCases = {
        {"quick fox", {
            {1, 1.0704575},  // doc1: "quick quick fox"
            {0, 0.5720391}   // doc0: "the quick brown fox jumps over the lazy dog"
        }},
        {"lazy dog", {
            {2, 0.92791617}, // doc2: "lazy dog sleeps"
            {0, 0.5720391}   // doc0: "the quick brown fox jumps over the lazy dog"
        }},
        {"brown fox", {
            {0, 0.5720391}  // doc3: "brown bear eats honey"
        }},
        {"honey", {
            {3, 0.66565275}  // doc3: "brown bear eats honey"
        }},
        {"xylophone rare", {
            {4, 1.3313055}   // doc4: "xylophone music is rare"
        }}
    };

    DoValidateRelevanceQuery(db,
        R"sql(
            SELECT Key, FulltextScore(Text, "%s", "and" as DefaultOperator) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "%s", "and" as DefaultOperator) > 0
            ORDER BY Relevance DESC
        )sql",
        andTestCases);

    DoValidateRelevanceQuery(db,
        R"sql(
            SELECT Key, FulltextScore(Text, "%s", "or" as DefaultOperator, "100" as MinimumShouldMatch) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "%s", "or" as DefaultOperator, "100" as MinimumShouldMatch) > 0
            ORDER BY Relevance DESC
        )sql",
        andTestCases);

    {
        TString query = Sprintf(R"sql(
            SELECT Key, FulltextScore(Text, "quick fox", "and" as DefaultOperator, "1" as MinimumShouldMatch) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "quick fox", "and" as DefaultOperator, "1" as MinimumShouldMatch) > 0
            ORDER BY Relevance DESC
        )sql");

        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        Cerr << "Result: " << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "MinimumShouldMatch is not supported for AND default operator");
    }

    {
        TString query = Sprintf(R"sql(
            SELECT Key, FulltextScore(Text, "quick fox", "some" as DefaultOperator, "1" as MinimumShouldMatch) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "quick fox", "some" as DefaultOperator, "1" as MinimumShouldMatch) > 0
            ORDER BY Relevance DESC
        )sql");

        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        Cerr << "Result: " << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unsupported default operator: `some`. Should be `and` or `or`");
    }

    {
        TString query = Sprintf(R"sql(
            SELECT Key, FulltextScore(Text, "quick fox", "or" as DefaultOperator, "101%" as MinimumShouldMatch) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "quick fox", "or" as DefaultOperator, "101%" as MinimumShouldMatch) > 0
            ORDER BY Relevance DESC
        )sql");

        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        Cerr << "Result: " << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "MinimumShouldMatch is incorrect. Invalid percentage: `101%`. Should be less than or equal to 100");
    }

    {
        TString query = Sprintf(R"sql(
            SELECT Key, FulltextScore(Text, "quick fox", "or" as DefaultOperator, "-1%" as MinimumShouldMatch) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "quick fox", "or" as DefaultOperator, "-1%" as MinimumShouldMatch) > 0
            ORDER BY Relevance DESC
        )sql");

        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        Cerr << "Result: " << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "MinimumShouldMatch is incorrect. Invalid percentage: `-1%`. Should be positive");
    }

    {
        TString query = Sprintf(R"sql(
            SELECT Key, FulltextScore(Text, "quick fox", "or" as DefaultOperator, "0%" as MinimumShouldMatch) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "quick fox", "or" as DefaultOperator, "0%" as MinimumShouldMatch) > 0
            ORDER BY Relevance DESC
        )sql");

        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        Cerr << "Result: " << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "MinimumShouldMatch is incorrect. Invalid percentage: `0%`. Should be positive");
    }

    {
        TString query = Sprintf(R"sql(
            SELECT Key, FulltextScore(Text, "quick fox", "or" as DefaultOperator, "non_numeric%" as MinimumShouldMatch) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "quick fox", "or" as DefaultOperator, "non_numeric%" as MinimumShouldMatch) > 0
            ORDER BY Relevance DESC
        )sql");

        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        Cerr << "Result: " << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "MinimumShouldMatch is incorrect. Invalid percentage: `non_numeric%`. Should be a number");
    }

    {
        TString query = Sprintf(R"sql(
            SELECT Key, FulltextScore(Text, "quick fox", "or" as DefaultOperator, "non_numeric" as MinimumShouldMatch) as Relevance
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "quick fox", "or" as DefaultOperator, "non_numeric" as MinimumShouldMatch) > 0
            ORDER BY Relevance DESC
        )sql");

        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        Cerr << "Result: " << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "MinimumShouldMatch is incorrect. Invalid value: `non_numeric`. Should be a number");
    }
}

Y_UNIT_TEST(SelectWithFulltextMatchAndSnowball) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);

    {
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (`Key`, `Text`) VALUES
                (1, "LLMs often hallucinate"),
                (2, "code with erasure"),
                (3, "hallucinated once upon a time"),
                (4, "you float like a feather"),
                (5, "quantization of floating point number")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    AddIndexSnowball(db, "english");

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "hallucination")
            ORDER BY `Key`;

            SELECT `Key`, `Text`
            FROM `/Root/Texts`
            WHERE String::Contains(`Text`, "hallucination")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["LLMs often hallucinate"]];
            [[3u];["hallucinated once upon a time"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([])", NYdb::FormatResultSetYson(result.GetResultSet(1)));
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "erasure coding")
            ORDER BY `Key`;

            SELECT `Key`, `Text`
            FROM `/Root/Texts`
            WHERE String::Contains(`Text`, "erasure coding")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[2u];["code with erasure"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([])", NYdb::FormatResultSetYson(result.GetResultSet(1)));
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "float")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[4u];["you float like a feather"]];
            [[5u];["quantization of floating point number"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

    DropIndex(db);
    AddIndexSnowball(db, "russian");

    {
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (`Key`, `Text`) VALUES
                (1, "С учетом поляризации")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "поляризация")
            ORDER BY `Key`;

            SELECT `Key`, `Text`
            FROM `/Root/Texts`
            WHERE String::Contains(`Text`, "поляризация")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["С учетом поляризации"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([])", NYdb::FormatResultSetYson(result.GetResultSet(1)));
    }
}

Y_UNIT_TEST(SelectWithFulltextMatchAndNgramWildcardSingleStar) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    NYdb::NQuery::TExecuteQuerySettings querySettings;
    querySettings.ClientTimeout(TDuration::Minutes(1));

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndexNGram(db, 4, 6);

    {
        TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().ToString().contains("No search terms were extracted from the query"));
    }
}

Y_UNIT_TEST_QUAD(SelectWithFulltextMatchAndNgram, Edge, Covered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    NYdb::NQuery::TExecuteQuerySettings querySettings;
    querySettings.ClientTimeout(TDuration::Minutes(1));

    CreateTexts(db);

    {
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (`Key`, `Text`) VALUES
                (0, "Arena Allocation"),
                (1, "Area Renaissance"),
                (2, "Werner Heisenberg"),
                (3, "Bern city"),
                (4, "lusedaedae"),
                (5, "lusedaeda"),
                (100, ""),
                (101, NULL)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    AddIndexNGram(db, 3, 3, false, Edge, Covered);

    DoValidateWildcardMatchQuery(db, "% renaissance%", R"([
        [[1u];["Area Renaissance"]]
    ])", "[]");

    DoValidateWildcardMatchQuery(db, "Werner Heisenberg", R"([
        [[2u];["Werner Heisenberg"]]
    ])");

    DoValidateWildcardMatchQuery(db, "% renaissance%", R"([
        [[1u]]
    ])", "[]", "`Key`");

    DoValidateWildcardMatchQuery(db, "Werner Heisenberg", R"([
        [[2u]]
    ])", "", "`Key`");

    DoValidateWildcardMatchQuery(db, "% renaissance%", R"([
        [[1u]]
    ])", "[]", "`Key`");

    // {are, ren, ena} can be found separately in "Area Renaissance" but it's not correct result
    DoValidateWildcardMatchQuery(db, "arena %", R"([
        [[0u];["Arena Allocation"]]
    ])", "[]");

    // {ber, ern} can be found separately in "Werner Heisenberg" but it's not correct result
    DoValidateWildcardMatchQuery(db, "bern %", R"([
        [[3u];["Bern city"]]
    ])", "[]");

    // N-gram sets are the same: {lus, use, sed, eda, dae, aed}. Wont work without postfilter
    DoValidateWildcardMatchQuery(db, "lusedaedae", R"([
        [[4u];["lusedaedae"]]
    ])");

    DoValidateWildcardMatchQuery(db, "lusedaeda", R"([
        [[5u];["lusedaeda"]]
    ])");
}

void DoSelectWithFulltextMatchAndNgramWildcard(bool relevance, bool edge, bool covered) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    NYdb::NQuery::TExecuteQuerySettings querySettings;
    querySettings.ClientTimeout(TDuration::Minutes(1));

    CreateTexts(db);

    {
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (`Key`, `Text`) VALUES
                (0, "Arena Allocation"),
                (1, "Area Renaissance"),
                (2, "Werner Heisenberg"),
                (3, "Bern city"),
                (4, "aedaedalus"),
                (5, "edaedalus"),
                (6, "машинное обучение"),
                (7, "машинное масло"),
                (8, "котомор"),
                (9, "морекот"),
                (100, ""),
                (101, NULL)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    AddIndexNGram(db, 3, 5, relevance, edge, covered);

    DoValidateWildcardMatchQuery(db, "aren%", R"([
        [[0u];["Arena Allocation"]]
    ])", "[]");

    DoValidateWildcardMatchQuery(db, "wer%ner %berg", R"([
        [[2u];["Werner Heisenberg"]]
    ])", "[]");

    DoValidateWildcardMatchQuery(db, "edaeda%", R"([
        [[5u];["edaedalus"]]
    ])");

    DoValidateWildcardMatchQuery(db, "маш% обу%ние", R"([
        [[6u];["машинное обучение"]]
    ])");

    DoValidateWildcardMatchQuery(db, "мор%кот", R"([
        [[9u];["морекот"]]
    ])");

    if (!edge)
    {
        const TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%lloc%")
            ORDER BY `Key`;
        )sql";
        const auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        if (edge) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToString().contains("No search terms were extracted from the query"));
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
                [[0u];["Arena Allocation"]]
            ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }
    }
}

Y_UNIT_TEST_QUAD(SelectWithFulltextMatchAndNgramWildcard, Edge, Covered) {
    DoSelectWithFulltextMatchAndNgramWildcard(false, Edge, Covered);
}

Y_UNIT_TEST_QUAD(SelectWithRelevanceContainsAndNgramWildcard, Edge, Covered) {
    DoSelectWithFulltextMatchAndNgramWildcard(true, Edge, Covered);
}

Y_UNIT_TEST(SelectWithFulltextMatchAndNgramWildcardSpecialCharacters) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    NYdb::NQuery::TExecuteQuerySettings querySettings;
    querySettings.ClientTimeout(TDuration::Minutes(1));

    CreateTexts(db);

    {
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (`Key`, `Text`) VALUES
                (0, "simple li[a-zne"),
                (1, "l[i]ne"),
                (2, "{}n$321 ^...&-"),
                (3, "[a-z]+ f[i]ne (foo)?ba[rz] ([^2-5]+|.{3})$")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        const TString query = R"sql(
            ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (
                    tokenizer=whitespace,
                    use_filter_lowercase=true,
                    use_filter_ngram=true,
                    filter_ngram_min_length=3,
                    filter_ngram_max_length=3
                );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%[a-%")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%[^2-5]%})$")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%[i]%")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "{}n$3%")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "([^2-5]+|.{3})$")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "^...%")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[0u];["simple li[a-zne"]];
            [[3u];["[a-z]+ f[i]ne (foo)?ba[rz] ([^2-5]+|.{3})$"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([
            [[3u];["[a-z]+ f[i]ne (foo)?ba[rz] ([^2-5]+|.{3})$"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([
            [[1u];["l[i]ne"]];
            [[3u];["[a-z]+ f[i]ne (foo)?ba[rz] ([^2-5]+|.{3})$"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(2)));
        CompareYson(R"([
            [[2u];["{}n$321 ^...&-"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(3)));
        CompareYson(R"([
            [[3u];["[a-z]+ f[i]ne (foo)?ba[rz] ([^2-5]+|.{3})$"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(4)));
        CompareYson(R"([
            [[2u];["{}n$321 ^...&-"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(5)));
    }
}

Y_UNIT_TEST(ImbalanceNgrams) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);

    NYdb::NQuery::TExecuteQuerySettings querySettings;
    querySettings.ClientTimeout(TDuration::Minutes(1));

    CreateTexts(db);

    {
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (`Key`, `Text`) VALUES
                (1, "Users/Ab"),
                (2, "Users/Ann"),
                (3, "Users/Britt"),
                (4, "Users/Jack"),
                (5, "Users/Joe"),
                (6, "Users/Henry"),
                (7, "Users/Pete"),
                (8, "Users/Zoe"),
                (9, "Users/John"),
                (10, "Users/James"),
                (11, "Users/Lewis")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        const TString query = R"sql(
            ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_relevance
                ON (Text)
                WITH (
                    tokenizer=whitespace,
                    use_filter_lowercase=true,
                    use_filter_ngram=true,
                    filter_ngram_min_length=3,
                    filter_ngram_max_length=3
                );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        // Check that imbalanced ngram filtering doesn't break the result
        TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%Users/Br%")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[3u];["Users/Britt"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
}

Y_UNIT_TEST(SelectWithFulltextMatchAndNgramWildcardBoundaries) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    NYdb::NQuery::TExecuteQuerySettings querySettings;
    querySettings.ClientTimeout(TDuration::Minutes(1));

    CreateTexts(db);

    {
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (`Key`, `Text`) VALUES
                (0, "Arena"),
                (1, "Area"),
                (2, "Werner"),
                (3, "Bern"),
                (4, "rea"),
                (5, "b")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    AddIndexNGram(db, 4, 6);

    {
        TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "aren%")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "werner%")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[0u];["Arena"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([
            [[2u];["Werner"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(1)));
    }

    for (const TString& q : {"are\%", "\%rea", "b"}) {
        const TString query = std::format(R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "{}")
            ORDER BY `Key`;
        )sql", q.c_str());
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().ToString().contains("No search terms were extracted from the query"));
    }
}

Y_UNIT_TEST(SelectWithFulltextMatchAndNgramWildcardUtf8Size) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    NYdb::NQuery::TExecuteQuerySettings querySettings;
    querySettings.ClientTimeout(TDuration::Minutes(1));

    {
        const TString query = R"sql(
            CREATE TABLE `/Root/Texts` (
                Key Uint64,
                Text Utf8,
                PRIMARY KEY (Key)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (`Key`, `Text`) VALUES
                (0, "👾🧪 experiment 4️⃣②፳"),
                (1, "🙈 🎶 4️⃣"),
                (2, "سنة جديدة سعيدة!"),
                (3, "Gleðilegt Nýtt Ár!"),
                (4, "新年快乐！"),
                (5, "꫞"),
                (6, "﷽ that's one character")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        const TString query = R"sql(
            ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (
                    tokenizer=whitespace,
                    use_filter_lowercase=true,
                    use_filter_ngram=true,
                    filter_ngram_min_length=2,  -- to check that single emoji or multi-byte character is ignored
                    filter_ngram_max_length=5
                );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    for (const TString& q : {"🙈", "🎶", "4", "꫞", "﷽"}) {
        const TString query = std::format(R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "{}")
            ORDER BY `Key`;
        )sql", q.c_str());
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().ToString().contains("No search terms were extracted from the query"));
    }

    DoValidateWildcardMatchQuery(db, "%4️⃣", R"([
        [[1u];["🙈 🎶 4️⃣"]]
    ])");

    DropIndex(db);

    {
        const TString query = R"sql(
            ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (
                    tokenizer=whitespace,
                    use_filter_lowercase=true,
                    use_filter_ngram=true,
                    filter_ngram_min_length=1,
                    filter_ngram_max_length=5
                );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        const TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%🧪% %②፳")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "🙈 🎶 4️⃣")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%4%") -- 4️⃣ is grapheme cluster with first rune "4"
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%Ár!")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%年%！")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "꫞")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "﷽%")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[0u];["👾🧪 experiment 4️⃣②፳"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([
            [[1u];["🙈 🎶 4️⃣"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([
            [[0u];["👾🧪 experiment 4️⃣②፳"]];
            [[1u];["🙈 🎶 4️⃣"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(2)));
        CompareYson(R"([
            [[3u];["Gleðilegt Nýtt Ár!"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(3)));
        CompareYson(R"([
            [[4u];["新年快乐！"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(4)));
        CompareYson(R"([
            [[5u];["꫞"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(5)));
        CompareYson(R"([
            [[6u];["﷽ that's one character"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(6)));
    }
}

Y_UNIT_TEST_QUAD(SelectWithFulltextMatchAndNgramWildcardUnicode, RELEVANCE, UTF8) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    NYdb::NQuery::TExecuteQuerySettings querySettings;
    querySettings.ClientTimeout(TDuration::Minutes(1));

    CreateTexts(db, UTF8);

    {
        TString query = R"sql(
            UPSERT INTO `/Root/Texts` (`Key`, `Text`) VALUES
                (0, "abc023"),
                (1, "◌̧◌̇◌̣"),
                (2, "﷽‎؈ۻ"),
                (3, "异体字異體字"),
                (4, "ä̸̱b̴̪͛"),
                (5, "😢🐶🐕🐈"),
                (6, "4️⃣🐕‍🦺🐈‍⬛"),
                (7, "👨‍👩‍👧‍👦🇦🇨")

        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        const TString query = std::format(R"sql(
            ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
                GLOBAL USING {0}
                ON (Text)
                WITH (
                    tokenizer=whitespace,
                    use_filter_lowercase=true,
                    use_filter_ngram=true,
                    filter_ngram_min_length=2,
                    filter_ngram_max_length=2
                );
        )sql", RELEVANCE ? "fulltext_relevance" : "fulltext_plain");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        const TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%bc0%")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%◌̇%")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%؈ۻ")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%字異體%")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "ä̸̱%")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%🐶🐕%")
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%🐕‍🦺%")  -- despite it looks like single character it is not: it's 🐕 with modifiers
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "👨\u200D👩%")  -- 👨‍👩‍👧‍👦 is combination of 👨, 👩, 👧 and 👦
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[0u];["abc023"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([
            [[1u];["◌̧◌̇◌̣"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([
            [[2u];["﷽‎؈ۻ"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(2)));
        CompareYson(R"([
            [[3u];["异体字異體字"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(3)));
        CompareYson(R"([
            [[4u];["ä̸̱b̴̪͛"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(4)));
        CompareYson(R"([
            [[5u];["😢🐶🐕🐈"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(5)));
        CompareYson(R"([
            [[6u];["4️⃣🐕‍🦺🐈‍⬛"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(6)));
        CompareYson(R"([
            [[7u];["👨‍👩‍👧‍👦🇦🇨"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(7)));
    }

    {
        const TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%🐶%")  -- should fail with BAD_REQUEST: query shorter than ngrams, no search terms extracted
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().ToString().contains("No search terms were extracted from the query"));
    }
}

Y_UNIT_TEST(SelectWithFulltextMatchAndEdgeNgramWildcard) {
        auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    NYdb::NQuery::TExecuteQuerySettings querySettings;
    querySettings.ClientTimeout(TDuration::Minutes(1));

    CreateTexts(db);

    {
        const TString query = R"sql(
            UPSERT INTO `/Root/Texts` (`Key`, `Text`) VALUES
                (0, "aaaaabbcd efg"),
                (1, "123456+789=")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    AddIndexNGram(db, 1, 5, false, true);

    DoValidateWildcardMatchQuery(db, "aaaaabbcd efg", R"([
        [[0u];["aaaaabbcd efg"]]
    ])");
    DoValidateWildcardMatchQuery(db, "aaaaab%", R"([
        [[0u];["aaaaabbcd efg"]]
    ])");
    DoValidateWildcardMatchQuery(db, "aa%bc%", R"([
        [[0u];["aaaaabbcd efg"]]
    ])");

    DoValidateWildcardMatchQuery(db, "ef", R"([
    ])");

    DoValidateWildcardMatchQuery(db, "12%6+7%9=", R"([
        [[1u];["123456+789="]]
    ])");

    DoValidateWildcardMatchQuery(db, "123450+789=", R"([
    ])");

    DoValidateWildcardMatchQuery(db, "123%", R"([
        [[1u];["123456+789="]]
    ])");

    DoValidateWildcardMatchQuery(db, "1", R"([
    ])");


    {
        const TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%ef%")
            ORDER BY `Key`;
        )sql";
        const auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().ToString().contains("No search terms were extracted from the query"));
    }
}

Y_UNIT_TEST(SelectWithFulltextMatchAndNgramWildcardVariableSize) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    NYdb::NQuery::TExecuteQuerySettings querySettings;
    querySettings.ClientTimeout(TDuration::Minutes(1));

    CreateTexts(db);

    {
        const TString query = R"sql(
            UPSERT INTO `/Root/Texts` (`Key`, `Text`) VALUES
                (0, "Arena Allocation"),
                (1, "Area Renaissance"),
                (2, "Werner Heisenberg"),
                (3, "Bern city"),
                (4, "lusedaedae"),
                (5, "lusedaeda"),
                (100, ""),
                (101, NULL)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    AddIndexNGram(db, 2, 2);

    {
        const TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "are% %llo%", "Wildcard" as Mode)
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "ber%", "Wildcard" as Mode)
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "lu%aed%", "Wildcard" as Mode)
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[0u];["Arena Allocation"]];
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([
            [[3u];["Bern city"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([
            [[4u];["lusedaedae"]];
            [[5u];["lusedaeda"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(2)));
    }

    DropIndex(db);
    AddIndexNGram(db, 4, 4);

    auto singleRetryQuery = [&](auto& db, const TString& query, const EStatus expected = EStatus::SUCCESS) {
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        if (result.GetStatus() != expected) {
            result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expected, result.GetIssues().ToString());
            return result;
        }

        return result;
    };

    {
        const TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "area %rena%", "Wildcard" as Mode)
            ORDER BY `Key`;
        )sql";

        auto result = singleRetryQuery(db, query);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["Area Renaissance"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

    {
        const TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "ber%", "Wildcard" as Mode)
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "lu%aed%", "Wildcard" as Mode)
            ORDER BY `Key`;
        )sql";
        auto result = singleRetryQuery(db, query, EStatus::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().ToString().contains("No search terms were extracted from the query"));
    }

    DropIndex(db);
    AddIndexNGram(db, 2, 4);

    {
        const TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "are% %ena%", "Wildcard" as Mode)
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "ber%", "Wildcard" as Mode)
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "lu%aed%", "Wildcard" as Mode)
            ORDER BY `Key`;
        )sql";
        auto result = singleRetryQuery(db, query);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["Area Renaissance"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([
            [[3u];["Bern city"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([
            [[4u];["lusedaedae"]];
            [[5u];["lusedaeda"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(2)));
    }

    DropIndex(db);
    AddIndexNGram(db, 3, 5);

    {
        const TString query = R"sql(
            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "are% %ena%", "Wildcard" as Mode)
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "ber%", "Wildcard" as Mode)
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "lu% aed%", "Wildcard" as Mode)
            ORDER BY `Key`;

            SELECT `Key`, `Text` FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "lu%aed%", "Wildcard" as Mode)
            ORDER BY `Key`;
        )sql";
        auto result = singleRetryQuery(db, query);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1u];["Area Renaissance"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([
            [[3u];["Bern city"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([])", NYdb::FormatResultSetYson(result.GetResultSet(2)));
        CompareYson(R"([
            [[4u];["lusedaedae"]];
            [[5u];["lusedaeda"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(3)));
    }
}

Y_UNIT_TEST_QUAD(SelectWithFulltextMatchShorterThanMinNgram, RELEVANCE, UTF8) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    NYdb::NQuery::TExecuteQuerySettings querySettings;
    querySettings.ClientTimeout(TDuration::Minutes(1));

    CreateTexts(db, UTF8);
    UpsertTexts(db);

    {
        const TString query = std::format(R"sql(
            ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
                GLOBAL USING {0}
                ON (Text)
                WITH (
                    tokenizer=standard,
                    use_filter_lowercase=true,
                    use_filter_ngram=true,
                    filter_ngram_min_length=3,
                    filter_ngram_max_length=3
                );
        )sql", RELEVANCE ? "fulltext_relevance" : "fulltext_plain");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        const TString query = std::format(R"sql(
            SELECT *
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "at");

            SELECT *
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "%at%");
        )sql");
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().ToString().contains("No search terms were extracted from the query"));
    }

    for (const TString& q : {"at", "\%at\%"}) {
        const TString query = std::format(R"sql(
            SELECT *
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "{}")
            LIMIT 100;
        )sql", q.c_str());
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), querySettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().ToString().contains("No search terms were extracted from the query"));
    }
}

Y_UNIT_TEST(ExplainFulltextIndexContains) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);

    auto tableClient = kikimr.GetTableClient();
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    TString query = R"sql(
        SELECT Key, Text
        FROM `/Root/Texts` VIEW `fulltext_idx`
        WHERE FulltextMatch(Text, "cats")
    )sql";
    auto result = session.ExplainDataQuery(query).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    Cerr << result.GetPlan() << Endl;

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(result.GetPlan(), &plan, true);
    UNIT_ASSERT(ValidatePlanNodeIds(plan));

    // Verify ReadFullTextIndex operator is present
    auto readFullTextIndex = FindPlanNodeByKv(plan, "Name", "ReadFullTextIndex");
    UNIT_ASSERT(readFullTextIndex.IsDefined());

    // Verify operator properties
    const auto& opProps = readFullTextIndex.GetMapSafe();
    UNIT_ASSERT(opProps.contains("Table"));
    UNIT_ASSERT(opProps.contains("Index"));
    UNIT_ASSERT(opProps.contains("Columns"));
    UNIT_ASSERT_VALUES_EQUAL(opProps.at("Index").GetStringSafe(), "fulltext_idx");
}

Y_UNIT_TEST(ExplainFulltextIndexRelevance) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, "fulltext_relevance");

    auto tableClient = kikimr.GetTableClient();
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    TString query = R"sql(
        SELECT Key, Text, FulltextScore(Text, "cats") as Relevance
        FROM `/Root/Texts` VIEW `fulltext_idx`
        WHERE FulltextScore(Text, "cats") > 0
        ORDER BY Relevance DESC
    )sql";
    auto result = session.ExplainDataQuery(query).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    Cerr << result.GetPlan() << Endl;

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(result.GetPlan(), &plan, true);
    UNIT_ASSERT(ValidatePlanNodeIds(plan));

    // Verify ReadFullTextIndex operator is present
    auto readFullTextIndex = FindPlanNodeByKv(plan, "Name", "ReadFullTextIndex");
    UNIT_ASSERT(readFullTextIndex.IsDefined());

    // Verify operator properties
    const auto& opProps = readFullTextIndex.GetMapSafe();
    UNIT_ASSERT(opProps.contains("Table"));
    UNIT_ASSERT(opProps.contains("Index"));
    UNIT_ASSERT(opProps.contains("Columns"));
    UNIT_ASSERT_VALUES_EQUAL(opProps.at("Index").GetStringSafe(), "fulltext_idx");
}

Y_UNIT_TEST(ExplainFulltextIndexScanQuery) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();
    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db);

    auto tableClient = kikimr.GetTableClient();
    TStreamExecScanQuerySettings querySettings;
    querySettings.Explain(true);

    TString query = R"sql(
        SELECT Key, Text
        FROM `/Root/Texts` VIEW `fulltext_idx`
        WHERE FulltextMatch(Text, "cats")
    )sql";
    auto it = tableClient.StreamExecuteScanQuery(query, querySettings).GetValueSync();
    auto res = CollectStreamResult(it);
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    UNIT_ASSERT(res.PlanJson);

    Cerr << *res.PlanJson << Endl;

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(*res.PlanJson, &plan, true);
    UNIT_ASSERT(ValidatePlanNodeIds(plan));

    // Verify ReadFullTextIndex operator is present
    auto readFullTextIndex = FindPlanNodeByKv(plan, "Name", "ReadFullTextIndex");
    UNIT_ASSERT(readFullTextIndex.IsDefined());

    // Verify operator properties
    const auto& opProps = readFullTextIndex.GetMapSafe();
    UNIT_ASSERT(opProps.contains("Table"));
    UNIT_ASSERT(opProps.contains("Index"));
    UNIT_ASSERT(opProps.contains("Columns"));
    UNIT_ASSERT_VALUES_EQUAL(opProps.at("Index").GetStringSafe(), "fulltext_idx");
}

Y_UNIT_TEST(AddFullTextFlatIndexWithTruncateWithSelect) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    featureFlags.SetEnableTruncateTable(true);

    auto kikimr = Kikimr(std::move(featureFlags));

    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_ACTOR, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    AddIndex(db);

    auto select = [&](){
        TString query = R"sql(
            SELECT `Key`, `Text`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "404 not found")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 0);
    };

    auto ensureTableIsEmpty = [&](){
        auto result = db.ExecuteQuery("SELECT * FROM `/Root/Texts`;", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 0);
    };

    auto verifyIndexWorksCorrectly = [&](){
        select();
        UpsertSomeTexts(db);
        select();
    };

    verifyIndexWorksCorrectly();

    for (size_t tryIndex = 0; tryIndex < 5; ++tryIndex) {
        TruncateTable(db);
        ensureTableIsEmpty();
        verifyIndexWorksCorrectly();
    }
}

Y_UNIT_TEST(AddFullTextRelevanceIndexWithTruncate) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    featureFlags.SetEnableTruncateTable(true);

    auto kikimr = Kikimr(std::move(featureFlags));
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    AddIndex(db, "fulltext_relevance");

    auto verifyIndexWorksCorrectly = [&](){
        UpsertTexts(db);
        auto index = ReadIndex(db);
        CompareYson(R"([
            [[100u];1u;"animals"];
            [[100u];1u;"cats"];
            [[200u];1u;"cats"];
            [[300u];2u;"cats"];
            [[100u];1u;"chase"];
            [[200u];1u;"chase"];
            [[200u];1u;"dogs"];
            [[400u];1u;"dogs"];
            [[400u];1u;"foxes"];
            [[300u];1u;"love"];
            [[400u];1u;"love"];
            [[100u];1u;"small"];
            [[200u];1u;"small"]
        ])", NYdb::FormatResultSetYson(index));
    };

    verifyIndexWorksCorrectly();

    for (size_t tryIndex = 0; tryIndex < 5; ++tryIndex) {
        TruncateTable(db);
        verifyIndexWorksCorrectly();
    }

}

// Positive tests: CREATE TABLE with inline fulltext index

Y_UNIT_TEST(FulltextIndexCreateTableWithUint64Key) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    {
        TString query = R"sql(
            CREATE TABLE `/Root/TextsUint64Key` (
                Key Uint64,
                Text String,
                Data String,
                PRIMARY KEY (Key),
                INDEX fulltext_idx
                    GLOBAL USING fulltext_plain
                    ON (Text)
                    WITH (tokenizer=standard, use_filter_lowercase=true)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            UPSERT INTO `/Root/TextsUint64Key` (Key, Text, Data) VALUES
                (1, "Cats chase small animals.", "cats data"),
                (2, "Dogs chase small cats.", "dogs data"),
                (3, "Cats love cats.", "cats cats data"),
                (4, "Foxes love dogs.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            SELECT Key, Text FROM `/Root/TextsUint64Key` VIEW `fulltext_idx`
            WHERE FulltextMatch(Text, "cats")
            ORDER BY Key
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 3);
    }
}

Y_UNIT_TEST(FulltextIndexCreateTableWithInt64Key) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    TString query = R"sql(
        CREATE TABLE `/Root/TextsInt64Key` (
            Key Int64,
            Text String,
            Data String,
            PRIMARY KEY (Key),
            INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        );
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
        "primary key column 'Key' to be of type 'Uint64' but got Int64");
}

// Positive test: CREATE TABLE then ALTER TABLE ADD INDEX

Y_UNIT_TEST(FulltextIndexAlterTableWithUint64Key) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    {
        TString query = R"sql(
            CREATE TABLE `/Root/TextsUint64KeyAlter` (
                Key Uint64,
                Text String,
                Data String,
                PRIMARY KEY (Key)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            UPSERT INTO `/Root/TextsUint64KeyAlter` (Key, Text, Data) VALUES
                (1, "Cats chase small animals.", "cats data"),
                (2, "Dogs chase small cats.", "dogs data"),
                (3, "Cats love cats.", "cats cats data"),
                (4, "Foxes love dogs.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            ALTER TABLE `/Root/TextsUint64KeyAlter` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            SELECT Key, Text FROM `/Root/TextsUint64KeyAlter` VIEW `fulltext_idx`
            WHERE FulltextMatch(Text, "cats")
            ORDER BY Key
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 3);
    }
}

// Negative tests: unsupported PK types

Y_UNIT_TEST(FulltextIndexAlterTableWithInt64Key) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    {
        TString query = R"sql(
            CREATE TABLE `/Root/TextsInt64KeyAlter` (
                Key Int64,
                Text String,
                Data String,
                PRIMARY KEY (Key)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            ALTER TABLE `/Root/TextsInt64KeyAlter` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
            "primary key column 'Key' to be of type 'Uint64' but got Int64");
    }
}

Y_UNIT_TEST(FulltextIndexCreateTableWithUtf8Key) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    TString query = R"sql(
        CREATE TABLE `/Root/TextsUtf8Key` (
            Key Utf8,
            Text String,
            Data String,
            PRIMARY KEY (Key),
            INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        );
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
        "primary key column 'Key' to be of type 'Uint64' but got Utf8");
}

Y_UNIT_TEST(FulltextIndexCreateTableWithStringKey) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    TString query = R"sql(
        CREATE TABLE `/Root/TextsStringKey` (
            Key String NOT NULL,
            Text String,
            Data String,
            PRIMARY KEY (Key),
            INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        );
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
        "primary key column 'Key' to be of type 'Uint64' but got String");
}

Y_UNIT_TEST(FulltextIndexCreateTableWithUint32Key) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    TString query = R"sql(
        CREATE TABLE `/Root/TextsUint32Key` (
            Key Uint32,
            Text String,
            Data String,
            PRIMARY KEY (Key),
            INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        );
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
        "primary key column 'Key' to be of type 'Uint64' but got Uint32");
}

// Negative tests: CREATE TABLE with fulltext index and composite primary keys

Y_UNIT_TEST(FulltextIndexCreateTableWithCompositeKeyTwoColumns) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    TString query = R"sql(
        CREATE TABLE `/Root/TextsCompositeKey2` (
            Category Utf8,
            Id Int64,
            Text String,
            Data String,
            PRIMARY KEY (Category, Id),
            INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        );
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
        "exactly one primary key column");
}

Y_UNIT_TEST(FulltextIndexCreateTableWithCompositeKeyThreeColumns) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    TString query = R"sql(
        CREATE TABLE `/Root/TextsCompositeKey3` (
            Tenant String,
            Category Utf8,
            Id Int64,
            Text String,
            Data String,
            PRIMARY KEY (Tenant, Category, Id),
            INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        );
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
        "exactly one primary key column");
}

Y_UNIT_TEST(FulltextIndexCreateTableWithCompositeKeyMixedTypes) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    TString query = R"sql(
        CREATE TABLE `/Root/TextsCompositeKeyMixed` (
            Year Int64,
            Region String,
            Name Utf8,
            Description String,
            Tags String,
            PRIMARY KEY (Year, Region, Name),
            INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Description)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        );
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
        "exactly one primary key column");
}

Y_UNIT_TEST(FulltextRelevanceIndexCreateTableWithCompositeKey) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    TString query = R"sql(
        CREATE TABLE `/Root/TextsCompositeKeyRelevance` (
            Category Utf8,
            Id Int64,
            Text String,
            Data String,
            PRIMARY KEY (Category, Id),
            INDEX fulltext_idx
                GLOBAL USING fulltext_relevance
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        );
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
        "exactly one primary key column");
}

// Test that fulltext queries handle delivery problems gracefully
// This uses the observer pattern to inject delivery problems
Y_UNIT_TEST_TWIN(FullTextDeliveryProblem, LimitRowsPerRequest) {
    // Test that fulltext query succeeds even if delivery problem happens
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);

    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
    settings.SetUseRealThreads(false);
    if (LimitRowsPerRequest) {
        settings.AppConfig.MutableTableServiceConfig()->MutableIteratorReadQuotaSettings()->SetMaxRows(1);
        settings.AppConfig.MutableTableServiceConfig()->MutableIteratorReadQuotaSettings()->SetMaxBytes(1024);
    }

    Y_DEFER {
        SetDefaultIteratorQuotaSettings(32767, 5_MB);
    };

    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TKikimrRunner kikimr(settings);
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    auto db = kikimr.GetQueryClient();

    // Create table with fulltext index using RunCall to properly handle fake threads
    kikimr.RunCall([&]() { CreateTexts(db); return true; });
    kikimr.RunCall([&]() { UpsertTexts(db); return true; });
    kikimr.RunCall([&]() { AddIndex(db, "fulltext_relevance"); return true; });

    auto sender = runtime.AllocateEdgeActor();

    using namespace NTableIndex;
    using namespace NTableIndex::NFulltext;

    // Get shards for the index posting table
    auto docsShards = GetTableShards(&kikimr.GetTestServer(), sender, JoinSeq('/', TVector<TString>{"/Root/Texts/fulltext_idx", DocsTable}));
    auto implShards = GetTableShards(&kikimr.GetTestServer(), sender, JoinSeq('/', TVector<TString>{"/Root/Texts/fulltext_idx", ImplTable}));
    auto dictShards = GetTableShards(&kikimr.GetTestServer(), sender, JoinSeq('/', TVector<TString>{"/Root/Texts/fulltext_idx", DictTable}));
    auto statsShards = GetTableShards(&kikimr.GetTestServer(), sender, JoinSeq('/', TVector<TString>{"/Root/Texts/fulltext_idx", StatsTable}));
    auto mainShards = GetTableShards(&kikimr.GetTestServer(), sender, "/Root/Texts");


    THashMap<ui64, int> shardSet;
    UNIT_ASSERT(!docsShards.empty());
    UNIT_ASSERT(!implShards.empty());
    UNIT_ASSERT(!dictShards.empty());
    UNIT_ASSERT(!statsShards.empty());
    UNIT_ASSERT(!mainShards.empty());

    for (auto shard : implShards) {
        shardSet[shard] = 0;
    }
    for (auto shard : dictShards) {
        shardSet[shard] = 0;
    }
    for (auto shard : statsShards) {
        shardSet[shard] = 0;
    }
    for (auto shard : mainShards) {
        shardSet[shard] = 0;
    }

    for (auto shard : docsShards) {
        shardSet[shard] = 0;
    }

    int readCount = 0;
    int deliveryProblemSent = 0;

    // Set up observer to inject delivery problem on first TEvForward with TEvRead to our shards
    auto observer = [&](TAutoPtr<NActors::IEventHandle>& ev) -> TTestActorRuntimeBase::EEventAction {
        bool drop = false;
        if (ev->GetTypeRewrite() == NKikimr::TEvPipeCache::TEvForward::EventType) {
            auto* forward = ev->Get<NKikimr::TEvPipeCache::TEvForward>();
            // Check if this is a TEvRead going to one of our shards
            if (forward->Ev->Type() == NKikimr::TEvDataShard::TEvRead::EventType &&
                shardSet.contains(forward->TabletId)) {
                int& cnt = shardSet[forward->TabletId];

                Cerr << "Observed TEvRead #" << readCount << " to shard " << forward->TabletId
                     << ", sender: " << ev->Sender << Endl;

                readCount++;
                if ((cnt & 1) == 0) {
                    Cerr << "Injecting delivery problem for shard " << forward->TabletId
                         << " to actor " << ev->Sender << Endl;
                    auto undelivery = MakeHolder<NKikimr::TEvPipeCache::TEvDeliveryProblem>(forward->TabletId, true);
                    runtime.Send(new NActors::IEventHandle(ev->Sender, sender, undelivery.Release()));
                    deliveryProblemSent++;
                    drop = true;
                }
                cnt++;
            }
        }
        if (drop) {
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    runtime.SetObserverFunc(observer);

    // Execute fulltext query using RunCall pattern
    auto result = kikimr.RunCall([&]() {
        TString query = R"sql(
            SELECT Key, Text, FulltextScore(Text, "cats") as Relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "cats") > 0
            ORDER BY Relevance DESC
            LIMIT 10
        )sql";
        return db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    });

    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    // Verify we got results
    auto resultSet = result.GetResultSet(0);
    UNIT_ASSERT(resultSet.RowsCount() == 3);

    // Verify the delivery problem was actually sent
    UNIT_ASSERT(deliveryProblemSent > 0);
    Cerr << "Test completed successfully, total reads observed: " << readCount << Endl;
}

Y_UNIT_TEST(FulltextIndexCreateTableWithUtf8KeyAndNgram) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    TString query = R"sql(
        CREATE TABLE `/Root/Texts` (
            `Name` Utf8,
            `Text` Utf8,
            PRIMARY KEY (`Name`),
            INDEX `fulltext_idx`
                GLOBAL USING fulltext_plain
                ON (`Name`)
                WITH (
                    tokenizer=standard,
                    use_filter_lowercase=true,
                    use_filter_ngram=true,
                    filter_ngram_min_length=3,
                    filter_ngram_max_length=3
                )
        );
    )sql";
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
        "primary key column 'Name' to be of type 'Uint64' but got Utf8");
}

// Test L2 reads with imbalanced word frequencies in relevance index.
// When one word is much more frequent than another (>10x), the fulltext source
// uses a two-layer merge: L1 for the rare word, L2 for the common word.
Y_UNIT_TEST_QUAD(FulltextRelevanceL2Reads, LimitRowsPerRequest, InjectFail) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    if (InjectFail) {
        settings.SetUseRealThreads(false);
    }

    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    if (LimitRowsPerRequest) {
        settings.AppConfig.MutableTableServiceConfig()->MutableIteratorReadQuotaSettings()->SetMaxRows(1);
        settings.AppConfig.MutableTableServiceConfig()->MutableIteratorReadQuotaSettings()->SetMaxBytes(1024);
    }

    Y_DEFER {
        SetDefaultIteratorQuotaSettings(32767, 5_MB);
    };

    auto kikimr = TKikimrRunner(settings);
    auto db = kikimr.GetQueryClient();

    { // Create table
        TString query = R"sql(
            CREATE TABLE `/Root/Articles` (
                Key Uint64,
                Text String,
                PRIMARY KEY (Key)
            );
        )sql";
        auto result = kikimr.RunCall([&] { return db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync(); });
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    { // Insert data: "the" appears in many documents, "quantum" in very few
        // This creates a >10x imbalance in word frequencies
        TString query = R"sql(
            UPSERT INTO `/Root/Articles` (Key, Text) VALUES
                (1,  "the quick brown fox"),
                (2,  "the lazy dog sleeps"),
                (3,  "the cat sat on the mat"),
                (4,  "the rain in spain"),
                (5,  "the sun is shining"),
                (6,  "the moon is bright"),
                (7,  "the stars are beautiful"),
                (8,  "the world is vast"),
                (9,  "the ocean is deep"),
                (10, "the mountain is tall"),
                (11, "the river flows fast"),
                (12, "the forest is dense"),
                (13, "quantum computing is revolutionary and fast developing"),
                (14, "the quantum realm is fascinating"),
                (15, "the sky is blue")
        )sql";
        auto result = kikimr.RunCall([&]{ return db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync(); });
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    { // Add relevance index
        TString query = R"sql(
            ALTER TABLE `/Root/Articles` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_relevance
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        )sql";
        auto result = kikimr.RunCall([&] { return db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync(); });
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    using namespace NTableIndex;
    using namespace NTableIndex::NFulltext;

    auto sender = kikimr.GetTestServer().GetRuntime()->AllocateEdgeActor();

    // Get shards for the index posting table
    auto docsShards = GetTableShards(&kikimr.GetTestServer(), sender, JoinSeq('/', TVector<TString>{"/Root/Articles/fulltext_idx", DocsTable}));
    auto implShards = GetTableShards(&kikimr.GetTestServer(), sender, JoinSeq('/', TVector<TString>{"/Root/Articles/fulltext_idx", ImplTable}));
    auto dictShards = GetTableShards(&kikimr.GetTestServer(), sender, JoinSeq('/', TVector<TString>{"/Root/Articles/fulltext_idx", DictTable}));
    auto statsShards = GetTableShards(&kikimr.GetTestServer(), sender, JoinSeq('/', TVector<TString>{"/Root/Articles/fulltext_idx", StatsTable}));
    auto mainShards = GetTableShards(&kikimr.GetTestServer(), sender, "/Root/Articles");

    THashMap<ui64, int> shardSet;

    UNIT_ASSERT(!docsShards.empty());
    UNIT_ASSERT(!implShards.empty());
    UNIT_ASSERT(!dictShards.empty());
    UNIT_ASSERT(!statsShards.empty());
    UNIT_ASSERT(!mainShards.empty());

    for (auto shard : implShards) {
        shardSet[shard] = 0;
    }
    for (auto shard : dictShards) {
        shardSet[shard] = 0;
    }
    for (auto shard : statsShards) {
        shardSet[shard] = 0;
    }
    for (auto shard : mainShards) {
        shardSet[shard] = 0;
    }

    for (auto shard : docsShards) {
        shardSet[shard] = 0;
    }

    int readCount = 0;
    int resultsCount = 0;

    // Set up observer to inject delivery problem on first TEvForward with TEvRead to our shards
    auto observer = [&](TAutoPtr<NActors::IEventHandle>& ev) -> TTestActorRuntimeBase::EEventAction {
        bool drop = false;
        if (ev->GetTypeRewrite() == NKikimr::TEvPipeCache::TEvForward::EventType) {
            auto* forward = ev->Get<NKikimr::TEvPipeCache::TEvForward>();
            // Check if this is a TEvRead going to one of our shards
            if (forward->Ev->Type() == NKikimr::TEvDataShard::TEvRead::EventType &&
                shardSet.contains(forward->TabletId)) {
                int& cnt = shardSet[forward->TabletId];

                Cerr << "Observed TEvRead #" << readCount << " to shard " << forward->TabletId
                     << ", sender: " << ev->Sender << Endl;

                readCount++;
                if ((cnt & 1) == 0 && (resultsCount % 3) == 2) {
                    resultsCount = 0;
                    Cerr << "Injecting delivery problem for shard " << forward->TabletId
                         << " to actor " << ev->Sender << Endl;
                    auto undelivery = MakeHolder<NKikimr::TEvPipeCache::TEvDeliveryProblem>(forward->TabletId, true);
                    kikimr.GetTestServer().GetRuntime()->Send(new NActors::IEventHandle(ev->Sender, sender, undelivery.Release()));
                    drop = true;
                }
                cnt++;
            }
        }

        if (ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvReadResult::EventType) {
            auto* msg = ev->Get<NKikimr::TEvDataShard::TEvReadResult>();
            auto readId = msg->Record.GetReadId();
            Y_UNUSED(readId);
            resultsCount++;
        }

        if (drop) {
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };

    kikimr.GetTestServer().GetRuntime()->SetObserverFunc(observer);

    {
        auto stats = DoValidateRelevanceSingleQuery(kikimr, db, R"sql(
            SELECT Key, FulltextScore(Text, "the quantum") as Relevance
            FROM `/Root/Articles` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "the quantum") > 0
            ORDER BY Relevance DESC
            LIMIT 100
        )sql", { { 14, 0.841570 } });
        CompareYson(R"([
            ["/Root/Articles";0u;0u];
            ["/Root/Articles/fulltext_idx/indexImplDictTable";2u;18u];
            ["/Root/Articles/fulltext_idx/indexImplDocsTable";1u;12u];
            ["/Root/Articles/fulltext_idx/indexImplStatsTable";1u;16u];
            ["/Root/Articles/fulltext_idx/indexImplTable";16u;192u]
        ])", CondenseToYsonString(stats));
    }

    {
        auto stats = DoValidateRelevanceSingleQuery(kikimr, db, R"sql(
            SELECT Key, Text, FulltextScore(Text, "the quantum") as Relevance
            FROM `/Root/Articles` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "the quantum") > 0
            ORDER BY Relevance DESC
            LIMIT 100
        )sql", { { 14, 0.841570 } });
        CompareYson(R"([
            ["/Root/Articles";1u;40u];
            ["/Root/Articles/fulltext_idx/indexImplDictTable";2u;18u];
            ["/Root/Articles/fulltext_idx/indexImplDocsTable";1u;12u];
            ["/Root/Articles/fulltext_idx/indexImplStatsTable";1u;16u];
            ["/Root/Articles/fulltext_idx/indexImplTable";16u;192u]
        ])", CondenseToYsonString(stats));
    }

    {
        auto stats = DoValidateRelevanceSingleQuery(kikimr, db, R"sql(
            SELECT Key, FulltextScore(Text, "the computing") as Relevance
            FROM `/Root/Articles` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "the computing") > 0
            ORDER BY Relevance DESC
            LIMIT 100
        )sql", {  });
        CompareYson(R"([
            ["/Root/Articles";0u;0u];
            ["/Root/Articles/fulltext_idx/indexImplDictTable";2u;20u];
            ["/Root/Articles/fulltext_idx/indexImplDocsTable";0u;0u];
            ["/Root/Articles/fulltext_idx/indexImplStatsTable";1u;16u];
            ["/Root/Articles/fulltext_idx/indexImplTable";1u;12u]
        ])", CondenseToYsonString(stats));
    }

    {
        auto stats = DoValidateRelevanceSingleQuery(kikimr, db, R"sql(
            SELECT Key, FulltextScore(Text, "the fast river") as Relevance
            FROM `/Root/Articles` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "the fast river") > 0
            ORDER BY Relevance DESC
            LIMIT 100
        )sql", { {11, 2.040364 } });
        CompareYson(R"([
            ["/Root/Articles";0u;0u];
            ["/Root/Articles/fulltext_idx/indexImplDictTable";3u;24u];
            ["/Root/Articles/fulltext_idx/indexImplDocsTable";1u;12u];
            ["/Root/Articles/fulltext_idx/indexImplStatsTable";1u;16u];
            ["/Root/Articles/fulltext_idx/indexImplTable";4u;48u]
        ])", CondenseToYsonString(stats));
    }
}

Y_UNIT_TEST_TWIN(FullTextReadResultStatusRetry, LimitRowsPerRequest) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);

    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
    settings.SetUseRealThreads(false);
    if (LimitRowsPerRequest) {
        settings.AppConfig.MutableTableServiceConfig()->MutableIteratorReadQuotaSettings()->SetMaxRows(1);
        settings.AppConfig.MutableTableServiceConfig()->MutableIteratorReadQuotaSettings()->SetMaxBytes(1024);
    }

    Y_DEFER {
        SetDefaultIteratorQuotaSettings(32767, 5_MB);
    };

    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TKikimrRunner kikimr(settings);
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    auto db = kikimr.GetQueryClient();

    kikimr.RunCall([&]() { CreateTexts(db); return true; });
    kikimr.RunCall([&]() { UpsertTexts(db); return true; });
    kikimr.RunCall([&]() { AddIndex(db, "fulltext_relevance"); return true; });

    int errorsInjected = 0;

    auto observer = [&](TAutoPtr<NActors::IEventHandle>& ev) -> TTestActorRuntimeBase::EEventAction {
        if (ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvReadResult::EventType) {
            auto* msg = ev->Get<NKikimr::TEvDataShard::TEvReadResult>();
            if (msg->Record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
                Ydb::StatusIds::StatusCode errorCode;
                switch (errorsInjected % 3) {
                    case 0: errorCode = Ydb::StatusIds::OVERLOADED; break;
                    case 1: errorCode = Ydb::StatusIds::INTERNAL_ERROR; break;
                    default: errorCode = Ydb::StatusIds::NOT_FOUND; break;
                }
                if (errorsInjected < 6) {
                    Cerr << "Injecting " << Ydb::StatusIds::StatusCode_Name(errorCode)
                         << " for ReadId=" << msg->Record.GetReadId() << Endl;
                    msg->Record.MutableStatus()->SetCode(errorCode);
                    msg->Record.ClearArrowBatch();
                    msg->Record.ClearCellVec();
                    msg->Record.SetRowCount(0);
                    msg->Record.SetFinished(false);
                    errorsInjected++;
                }
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    runtime.SetObserverFunc(observer);

    auto result = kikimr.RunCall([&]() {
        TString query = R"sql(
            SELECT Key, Text, FulltextScore(Text, "cats") as Relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "cats") > 0
            ORDER BY Relevance DESC
            LIMIT 10
        )sql";
        return db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    });

    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    auto resultSet = result.GetResultSet(0);
    UNIT_ASSERT(resultSet.RowsCount() == 3);

    UNIT_ASSERT(errorsInjected > 0);
    Cerr << "Test completed successfully, errors injected: " << errorsInjected << Endl;
}

Y_UNIT_TEST(FullTextReadResultStatusAbort) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);

    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
    settings.SetUseRealThreads(false);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TKikimrRunner kikimr(settings);
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    auto db = kikimr.GetQueryClient();

    kikimr.RunCall([&]() { CreateTexts(db); return true; });
    kikimr.RunCall([&]() { UpsertTexts(db); return true; });
    kikimr.RunCall([&]() { AddIndex(db, "fulltext_relevance"); return true; });

    bool errorInjected = false;

    auto observer = [&](TAutoPtr<NActors::IEventHandle>& ev) -> TTestActorRuntimeBase::EEventAction {
        if (ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvReadResult::EventType) {
            auto* msg = ev->Get<NKikimr::TEvDataShard::TEvReadResult>();
            if (!errorInjected && msg->Record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
                Cerr << "Injecting BAD_SESSION for ReadId=" << msg->Record.GetReadId() << Endl;
                msg->Record.MutableStatus()->SetCode(Ydb::StatusIds::BAD_SESSION);
                msg->Record.ClearArrowBatch();
                msg->Record.ClearCellVec();
                msg->Record.SetRowCount(0);
                msg->Record.SetFinished(false);
                errorInjected = true;
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    runtime.SetObserverFunc(observer);

    auto result = kikimr.RunCall([&]() {
        TString query = R"sql(
            SELECT Key, Text, FulltextScore(Text, "cats") as Relevance FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "cats") > 0
            ORDER BY Relevance DESC
            LIMIT 10
        )sql";
        return db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    });

    UNIT_ASSERT(errorInjected);
    UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
    Cerr << "Non-retryable error correctly aborted the query, status: "
         << result.GetStatus() << Endl;
}

Y_UNIT_TEST_TWIN(FullTextReadResultStatusRetryL2, LimitRowsPerRequest) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);

    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
    settings.SetUseRealThreads(false);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    if (LimitRowsPerRequest) {
        settings.AppConfig.MutableTableServiceConfig()->MutableIteratorReadQuotaSettings()->SetMaxRows(1);
        settings.AppConfig.MutableTableServiceConfig()->MutableIteratorReadQuotaSettings()->SetMaxBytes(1024);
    }

    Y_DEFER {
        SetDefaultIteratorQuotaSettings(32767, 5_MB);
    };

    TKikimrRunner kikimr(settings);
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    auto db = kikimr.GetQueryClient();

    {
        TString query = R"sql(
            CREATE TABLE `/Root/Articles` (
                Key Uint64,
                Text String,
                PRIMARY KEY (Key)
            );
        )sql";
        auto result = kikimr.RunCall([&] { return db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync(); });
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            UPSERT INTO `/Root/Articles` (Key, Text) VALUES
                (1,  "the quick brown fox"),
                (2,  "the lazy dog sleeps"),
                (3,  "the cat sat on the mat"),
                (4,  "the rain in spain"),
                (5,  "the sun is shining"),
                (6,  "the moon is bright"),
                (7,  "the stars are beautiful"),
                (8,  "the world is vast"),
                (9,  "the ocean is deep"),
                (10, "the mountain is tall"),
                (11, "the river flows fast"),
                (12, "the forest is dense"),
                (13, "quantum computing is revolutionary and fast developing"),
                (14, "the quantum realm is fascinating"),
                (15, "the sky is blue")
        )sql";
        auto result = kikimr.RunCall([&]{ return db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync(); });
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            ALTER TABLE `/Root/Articles` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_relevance
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        )sql";
        auto result = kikimr.RunCall([&] { return db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync(); });
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    int errorsInjected = 0;

    auto observer = [&](TAutoPtr<NActors::IEventHandle>& ev) -> TTestActorRuntimeBase::EEventAction {
        if (ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvReadResult::EventType) {
            auto* msg = ev->Get<NKikimr::TEvDataShard::TEvReadResult>();
            if (msg->Record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
                Ydb::StatusIds::StatusCode errorCode;
                switch (errorsInjected % 3) {
                    case 0: errorCode = Ydb::StatusIds::OVERLOADED; break;
                    case 1: errorCode = Ydb::StatusIds::INTERNAL_ERROR; break;
                    default: errorCode = Ydb::StatusIds::NOT_FOUND; break;
                }
                if (errorsInjected < 6) {
                    Cerr << "Injecting " << Ydb::StatusIds::StatusCode_Name(errorCode)
                         << " for ReadId=" << msg->Record.GetReadId() << Endl;
                    msg->Record.MutableStatus()->SetCode(errorCode);
                    msg->Record.ClearArrowBatch();
                    msg->Record.ClearCellVec();
                    msg->Record.SetRowCount(0);
                    msg->Record.SetFinished(false);
                    errorsInjected++;
                }
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    runtime.SetObserverFunc(observer);

    {
        auto stats = DoValidateRelevanceSingleQuery(kikimr, db, R"sql(
            SELECT Key, FulltextScore(Text, "the quantum") as Relevance
            FROM `/Root/Articles` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "the quantum") > 0
            ORDER BY Relevance DESC
            LIMIT 100
        )sql", { { 14, 0.841570 } });
    }

    UNIT_ASSERT(errorsInjected > 0);
    Cerr << "L2 test completed successfully, errors injected: " << errorsInjected << Endl;

    errorsInjected = 0;

    {
        auto stats = DoValidateRelevanceSingleQuery(kikimr, db, R"sql(
            SELECT Key, FulltextScore(Text, "the fast river") as Relevance
            FROM `/Root/Articles` VIEW `fulltext_idx`
            WHERE FulltextScore(Text, "the fast river") > 0
            ORDER BY Relevance DESC
            LIMIT 100
        )sql", { {11, 2.040364 } });
    }

    UNIT_ASSERT(errorsInjected > 0);
    Cerr << "L2 three-word test completed, errors injected: " << errorsInjected << Endl;
}

}

} // namespace NKikimr::NKqp
