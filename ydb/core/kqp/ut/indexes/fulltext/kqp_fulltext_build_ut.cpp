#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/kqp/ut/indexes/fulltext/kqp_fulltext_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpFulltextBuild) {

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

        auto addIndex = TAlterTableSettings()
            .AppendAddIndexes(NYdb::NTable::TIndexDescription::CreateFulltextRelevanceIndex(
                "fulltext_idx", {"Text"},
                NYdb::NTable::TFulltextIndexSettings::FromProto(fulltextSettings),
                {},
                NYdb::NTable::TGlobalIndexSettings::FromProto(wordSettings),
                NYdb::NTable::TGlobalIndexSettings::FromProto(wordSettings)
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

Y_UNIT_TEST_TWIN(UpdateWithRelevance, Covered) {
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

    { // Update a row
        TString query = R"sql(
            UPDATE `/Root/Texts` SET Text="Birds love rabbits.", Data="birds data" WHERE Key=100
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    auto index = ReadIndex(db);
    CompareYson(R"([
        [[100u];1u;"birds"];
        [[200u];1u;"dogs"];
        [[200u];1u;"foxes"];
        [[100u];1u;"love"];
        [[200u];1u;"love"];
        [[100u];1u;"rabbits"];
    ])", NYdb::FormatResultSetYson(index));
    dict = ReadIndex(db, NTableIndex::NFulltext::DictTable);
    CompareYson(R"([
        [1u;"birds"];
        [0u;"cats"];
        [1u;"dogs"];
        [1u;"foxes"];
        [2u;"love"];
        [1u;"rabbits"]
    ])", NYdb::FormatResultSetYson(dict));
    auto docs = ReadIndex(db, NTableIndex::NFulltext::DocsTable);
    if (Covered) {
        CompareYson(R"([
            [["birds data"];[100u];3u];
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

Y_UNIT_TEST_TWIN(ReplaceRowReturning, EnableIndexStreamWrite) {
    auto kikimr = Kikimr(EnableIndexStreamWrite);
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

    {
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            UPSERT INTO `/Root/TextsInt64Key` (Key, Text, Data) VALUES
                (-2, "Cats chase small animals.", "cats data"),
                (-1, "Dogs chase small cats.", "dogs data"),
                (0, "Cats love cats.", "cats cats data"),
                (1, "Foxes love dogs.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            SELECT Key, Text FROM `/Root/TextsInt64Key` VIEW `fulltext_idx`
            WHERE FulltextMatch(Text, "cats")
            ORDER BY Key
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 3);
    }
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

// Positive test: ALTER TABLE ADD INDEX on Int64 key

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
            UPSERT INTO `/Root/TextsInt64KeyAlter` (Key, Text, Data) VALUES
                (-2, "Cats chase small animals.", "cats data"),
                (-1, "Dogs chase small cats.", "dogs data"),
                (0, "Cats love cats.", "cats cats data"),
                (1, "Foxes love dogs.", "foxes data")
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            SELECT Key, Text FROM `/Root/TextsInt64KeyAlter` VIEW `fulltext_idx`
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
        "primary key column 'Key' to be of type 'Uint64', 'Int64', 'Uint32' or 'Int32' but got Utf8");
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
        "primary key column 'Key' to be of type 'Uint64', 'Int64', 'Uint32' or 'Int32' but got String");
}

Y_UNIT_TEST(FulltextIndexCreateTableWithUint32Key) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    {
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            UPSERT INTO `/Root/TextsUint32Key` (Key, Text, Data) VALUES
                (1u, "Cats chase small animals.", "cats data"),
                (2u, "Dogs chase small cats.", "dogs data"),
                (3u, "Cats love cats.", "cats cats data"),
                (4u, "Foxes love dogs.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            SELECT Key, Text FROM `/Root/TextsUint32Key` VIEW `fulltext_idx`
            WHERE FulltextMatch(Text, "cats")
            ORDER BY Key
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 3);
    }
}

Y_UNIT_TEST(FulltextIndexCreateTableWithInt32Key) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    {
        TString query = R"sql(
            CREATE TABLE `/Root/TextsInt32Key` (
                Key Int32,
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
            UPSERT INTO `/Root/TextsInt32Key` (Key, Text, Data) VALUES
                (-2, "Cats chase small animals.", "cats data"),
                (-1, "Dogs chase small cats.", "dogs data"),
                (0, "Cats love cats.", "cats cats data"),
                (1, "Foxes love dogs.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            SELECT Key, Text FROM `/Root/TextsInt32Key` VIEW `fulltext_idx`
            WHERE FulltextMatch(Text, "cats")
            ORDER BY Key
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 3);
    }
}

// Positive tests: Serial PK types (auto-incrementing, backed by signed integers)

Y_UNIT_TEST(FulltextIndexCreateTableWithSerialKey) {
    // Serial / Serial4 -> Int32 backend
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    {
        TString query = R"sql(
            CREATE TABLE `/Root/TextsSerialKey` (
                Key Serial,
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
            INSERT INTO `/Root/TextsSerialKey` (Text, Data) VALUES
                ("Cats chase small animals.", "cats data"),
                ("Dogs chase small cats.", "dogs data"),
                ("Cats love cats.", "cats cats data"),
                ("Foxes love dogs.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            SELECT Key, Text FROM `/Root/TextsSerialKey` VIEW `fulltext_idx`
            WHERE FulltextMatch(Text, "cats")
            ORDER BY Key
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 3);
    }
}

Y_UNIT_TEST(FulltextIndexCreateTableWithBigSerialKey) {
    // Serial8 / BigSerial -> Int64 backend
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    {
        TString query = R"sql(
            CREATE TABLE `/Root/TextsBigSerialKey` (
                Key BigSerial,
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
            INSERT INTO `/Root/TextsBigSerialKey` (Text, Data) VALUES
                ("Cats chase small animals.", "cats data"),
                ("Dogs chase small cats.", "dogs data"),
                ("Cats love cats.", "cats cats data"),
                ("Foxes love dogs.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            SELECT Key, Text FROM `/Root/TextsBigSerialKey` VIEW `fulltext_idx`
            WHERE FulltextMatch(Text, "cats")
            ORDER BY Key
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 3);
    }
}

// Negative test: SmallSerial -> Int16 backend (unsupported by fulltext)

Y_UNIT_TEST(FulltextIndexCreateTableWithSmallSerialKey) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    TString query = R"sql(
        CREATE TABLE `/Root/TextsSmallSerialKey` (
            Key SmallSerial,
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
        "primary key column 'Key' to be of type 'Uint64', 'Int64', 'Uint32' or 'Int32' but got Int16");
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
        "primary key column 'Name' to be of type 'Uint64', 'Int64', 'Uint32' or 'Int32' but got Utf8");
}

Y_UNIT_TEST(FulltextIndexBuildCustomParallel) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    auto settings = TKikimrSettings()
        .SetFeatureFlags(featureFlags)
        .SetUseRealThreads(false);
    auto kikimr = TKikimrRunner(settings);
    auto runtime = kikimr.GetTestServer().GetRuntime();

    ui32 capturedParallel = 0;
    auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == NSchemeShard::TEvIndexBuilder::TEvCreateRequest::EventType) {
            capturedParallel = ev->Get<NSchemeShard::TEvIndexBuilder::TEvCreateRequest>()
                ->Record.GetSettings().max_shards_in_flight();
        }
        return false;
    };
    runtime->SetEventFilter(captureEvents);

    kikimr.RunCall([&] {
        auto db = kikimr.GetQueryClient();
        CreateTexts(db);
        UpsertTexts(db);
        TString query = R"sql(
            ALTER TABLE `/Root/Texts` ADD INDEX fulltext_idx
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true, parallel=2)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    });

    UNIT_ASSERT_VALUES_EQUAL(capturedParallel, 2);
}

}

} // namespace NKikimr::NKqp
