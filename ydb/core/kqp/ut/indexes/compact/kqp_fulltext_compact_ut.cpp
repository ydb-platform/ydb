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

namespace {

void ExecuteQuery(NQuery::TQueryClient& db, const TString& query) {
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

TString FulltextSearch(NQuery::TQueryClient& db, const TString& searchQuery) {
    TString query = Sprintf(R"sql(
        SELECT `Key`, `Text`, `Data`
        FROM `/Root/Texts` VIEW `fulltext_idx`
        WHERE FulltextMatch(`Text`, "%s")
        ORDER BY `Key`;
    )sql", searchQuery.c_str());
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return NYdb::FormatResultSetYson(result.GetResultSet(0));
}

TResultSet ReadIndex(NQuery::TQueryClient& db, const char* table = "indexImplTable") {
    TString query = Sprintf(R"sql(
        SELECT * FROM `/Root/Texts/fulltext_idx/%s`;
    )sql", table);
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return result.GetResultSet(0);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpFulltextCompact) {

Y_UNIT_TEST(AddIndexCompact) {
    auto kikimr = KikimrWithCompact();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);
    AddIndex(db, "fulltext_plain");

    auto index = ReadIndex(db);
    CompareYson(R"([
        [%true;4294967295u;100u;"d";"animals"];
        [%true;4294967295u;300u;"ddd";"cats"];
        [%true;4294967295u;200u;"dd";"chase"];
        [%true;4294967295u;400u;"\xC8\1\xC8\1";"dogs"];
        [%true;4294967295u;400u;"\x90\3";"foxes"];
        [%true;4294967295u;400u;"\xAC\2d";"love"];
        [%true;4294967295u;200u;"dd";"small"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST_TWIN(AddIndexCompactRelevance, Covered) {
    auto kikimr = KikimrWithCompact();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);
    if (Covered) {
        AddIndexCovered(db, "fulltext_relevance");
    } else {
        AddIndex(db, "fulltext_relevance");
    }

    auto index = ReadIndex(db);
    CompareYson(R"([
        [%true;4294967295u;100u;"\xA4\1";"animals"];
        [%true;4294967295u;300u;"\xA4\1\xA4\1\xE4\1\2";"cats"];
        [%true;4294967295u;200u;"\xA4\1\xA4\1";"chase"];
        [%true;4294967295u;400u;"\x88\3\x88\3";"dogs"];
        [%true;4294967295u;400u;"\x90\6";"foxes"];
        [%true;4294967295u;400u;"\xAC\4\xA4\1";"love"];
        [%true;4294967295u;200u;"\xA4\1\xA4\1";"small"]
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

Y_UNIT_TEST_TWIN(InsertRow, WithRelevance) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, WithRelevance ? "fulltext_relevance" : "fulltext_plain");
    auto index = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "indexImplTable: " << index << Endl;
    if (WithRelevance) {
        auto docs = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::DocsTable));
        auto dict = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::DictTable));
        auto stats = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::StatsTable));
        Cerr << "indexImplDocsTable: " << docs << Endl;
        Cerr << "indexImplDictTable: " << dict << Endl;
        Cerr << "indexImplStatsTable: " << stats << Endl;
        CompareYson(R"([
            [%true;4294967295u;100u;"\xE4\1\2";"cats"];
            [%true;4294967295u;200u;"\x88\3";"dogs"];
            [%true;4294967295u;200u;"\x88\3";"foxes"];
            [%true;4294967295u;200u;"\xA4\1\xA4\1";"love"]
        ])", index);
        CompareYson(R"([
            [[100u];3u];
            [[200u];3u]
        ])", docs);
        CompareYson(R"([
            [1u;"cats"];
            [1u;"dogs"];
            [1u;"foxes"];
            [2u;"love"]
        ])", dict);
        CompareYson(R"([
            [2u;0u;6u]
        ])", stats);
    } else {
        CompareYson(R"([
            [%true;4294967295u;100u;"d";"cats"];
            [%true;4294967295u;200u;"\xC8\1";"dogs"];
            [%true;4294967295u;200u;"\xC8\1";"foxes"];
            [%true;4294967295u;200u;"dd";"love"]
        ])", index);
    }

    { // InsertRow
        TString query = R"sql(
            INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    index = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "indexImplTable: " << index << Endl;
    if (WithRelevance) {
        auto docs = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::DocsTable));
        auto dict = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::DictTable));
        auto stats = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::StatsTable));
        Cerr << "indexImplDocsTable: " << docs << Endl;
        Cerr << "indexImplDictTable: " << dict << Endl;
        Cerr << "indexImplStatsTable: " << stats << Endl;
        CompareYson(R"([
            [%true;4294967295u;100u;"\xE4\1\2";"cats"];
            [%true;4294967295u;18446744073709551615u;"\x96\2";"cats"];
            [%true;4294967295u;200u;"\x88\3";"dogs"];
            [%true;4294967290u;200u;"\x96\2";"foxes"];
            [%true;4294967295u;200u;"\x88\3";"foxes"];
            [%true;4294967290u;200u;"\x96\2";"love"];
            [%true;4294967295u;200u;"\xA4\1\xA4\1";"love"]
        ])", index);
        CompareYson(R"([
            [[100u];3u];
            [[150u];3u];
            [[200u];3u]
        ])", docs);
        CompareYson(R"([
            [2u;"cats"];
            [1u;"dogs"];
            [2u;"foxes"];
            [3u;"love"]
        ])", dict);
        CompareYson(R"([
            [3u;0u;9u]
        ])", stats);
    } else {
        CompareYson(R"([
            [%true;4294967295u;100u;"d";"cats"];
            [%true;4294967295u;18446744073709551615u;"\x96\1";"cats"];
            [%true;4294967295u;200u;"\xC8\1";"dogs"];
            [%true;4294967290u;200u;"\x96\1";"foxes"];
            [%true;4294967295u;200u;"\xC8\1";"foxes"];
            [%true;4294967290u;200u;"\x96\1";"love"];
            [%true;4294967295u;200u;"dd";"love"]
        ])", index);
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`, `Data`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "foxes cats")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[150u];["Foxes love cats."];["foxes data"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

}

Y_UNIT_TEST_TWIN(InsertMultipleTimes, WithRelevance) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();
    const TString indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, indexType);

    // First batch insert
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (150, "Foxes love cats.", "foxes data"),
            (151, "Wolves love foxes.", "cows data")
    )sql");

    // Second insert
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (152, "Rabbits love foxes.", "rabbit data")
    )sql");

    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]]
    ])", FulltextSearch(db, "cats"));

    CompareYson(R"([
        [[150u];["Foxes love cats."];["foxes data"]];
        [[151u];["Wolves love foxes."];["cows data"]];
        [[152u];["Rabbits love foxes."];["rabbit data"]];
        [[200u];["Dogs love foxes."];["dogs data"]]
    ])", FulltextSearch(db, "foxes"));

    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]];
        [[151u];["Wolves love foxes."];["cows data"]];
        [[152u];["Rabbits love foxes."];["rabbit data"]];
        [[200u];["Dogs love foxes."];["dogs data"]]
    ])", FulltextSearch(db, "love"));

    if (WithRelevance) {
        auto stats = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::StatsTable));
        CompareYson(R"([[5u;0u;15u]])", stats);
    }
}

Y_UNIT_TEST(UpsertNewRow) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, "fulltext_plain");

    // Upsert a new row (no existing key conflict - same as INSERT path)
    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (150, "Foxes love cats.", "foxes data")
    )sql");

    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]]
    ])", FulltextSearch(db, "cats"));

    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]];
        [[200u];["Dogs love foxes."];["dogs data"]]
    ])", FulltextSearch(db, "love"));
}

Y_UNIT_TEST(UpsertNewRowRelevance) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, "fulltext_relevance");

    // Upsert a new row (no existing key conflict)
    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (150, "Foxes love cats.", "foxes data")
    )sql");

    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]]
    ])", FulltextSearch(db, "cats"));

    auto dict = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::DictTable));
    CompareYson(R"([
        [2u;"cats"];
        [1u;"dogs"];
        [2u;"foxes"];
        [3u;"love"]
    ])", dict);

    auto stats = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::StatsTable));
    CompareYson(R"([[3u;0u;9u]])", stats);
}

Y_UNIT_TEST_TWIN(UpsertModifyExisting, WithRelevance) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();
    const TString indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, indexType);

    // Upsert modify existing row - change text content
    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, "Birds love rabbits.", "birds data")
    )sql");

    // "cats" no longer matches key 100
    CompareYson(R"([])", FulltextSearch(db, "cats"));

    // "birds" now matches key 100
    CompareYson(R"([
        [[100u];["Birds love rabbits."];["birds data"]]
    ])", FulltextSearch(db, "birds"));

    // "love" matches both rows
    CompareYson(R"([
        [[100u];["Birds love rabbits."];["birds data"]];
        [[200u];["Dogs love foxes."];["dogs data"]]
    ])", FulltextSearch(db, "love"));
}

Y_UNIT_TEST_TWIN(UpsertMixNewAndExisting, WithRelevance) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();
    const TString indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, indexType);

    // Upsert: key 100 exists (modify), keys 150/151 are new (insert)
    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, "Birds love rabbits.", "birds data"),
            (150, "Foxes love cats.", "foxes data"),
            (151, "Wolves love foxes.", "cows data")
    )sql");

    // key 100 was modified, "cats" removed from it
    CompareYson(R"([
        [[150u];["Foxes love cats."];["foxes data"]]
    ])", FulltextSearch(db, "cats"));

    CompareYson(R"([
        [[100u];["Birds love rabbits."];["birds data"]]
    ])", FulltextSearch(db, "birds"));

    CompareYson(R"([
        [[151u];["Wolves love foxes."];["cows data"]]
    ])", FulltextSearch(db, "wolves"));
}

Y_UNIT_TEST_TWIN(DeleteRow, WithRelevance) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();
    const TString indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreateTexts(db);
    UpsertTexts(db);
    AddIndex(db, indexType);

    // Verify initial state via search
    CompareYson(R"([
        [[100u];["Cats chase small animals."];["cats data"]];
        [[200u];["Dogs chase small cats."];["dogs data"]]
    ])", FulltextSearch(db, "chase"));

    // Delete one row
    ExecuteQuery(db, R"sql(
        DELETE FROM `/Root/Texts` WHERE Key = 100
    )sql");

    // "chase" should now only match key 200
    CompareYson(R"([
        [[200u];["Dogs chase small cats."];["dogs data"]]
    ])", FulltextSearch(db, "chase"));

    // "animals" should return nothing
    CompareYson(R"([])", FulltextSearch(db, "animals"));

    // "cats" should still match keys 200 and 300
    CompareYson(R"([
        [[200u];["Dogs chase small cats."];["dogs data"]];
        [[300u];["Cats love cats."];["cats cats data"]]
    ])", FulltextSearch(db, "cats"));
}

Y_UNIT_TEST_TWIN(DeleteMultipleRows, WithRelevance) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();
    const TString indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreateTexts(db);
    UpsertTexts(db);
    AddIndex(db, indexType);

    // Delete multiple rows one by one
    ExecuteQuery(db, R"sql(
        DELETE FROM `/Root/Texts` WHERE Key = 100
    )sql");
    ExecuteQuery(db, R"sql(
        DELETE FROM `/Root/Texts` WHERE Key = 200
    )sql");

    // "chase" should be empty (both rows with "chase" deleted)
    CompareYson(R"([])", FulltextSearch(db, "chase"));

    // "small" should be empty
    CompareYson(R"([])", FulltextSearch(db, "small"));

    // "cats" should only match key 300
    CompareYson(R"([
        [[300u];["Cats love cats."];["cats cats data"]]
    ])", FulltextSearch(db, "cats"));

    // "love" should match keys 300 and 400
    CompareYson(R"([
        [[300u];["Cats love cats."];["cats cats data"]];
        [[400u];["Foxes love dogs."];["foxes data"]]
    ])", FulltextSearch(db, "love"));
}

Y_UNIT_TEST_TWIN(UpdateRow, WithRelevance) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();
    const TString indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, indexType);

    // Update row text
    ExecuteQuery(db, R"sql(
        UPDATE `/Root/Texts` SET Text = "Birds love rabbits.", Data = "birds data" WHERE Key = 100
    )sql");

    // "cats" should now return empty
    CompareYson(R"([])", FulltextSearch(db, "cats"));

    // "birds" should match key 100
    CompareYson(R"([
        [[100u];["Birds love rabbits."];["birds data"]]
    ])", FulltextSearch(db, "birds"));

    // "rabbits" should match key 100
    CompareYson(R"([
        [[100u];["Birds love rabbits."];["birds data"]]
    ])", FulltextSearch(db, "rabbits"));
}

Y_UNIT_TEST_TWIN(ReplaceRow, WithRelevance) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();
    const TString indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, indexType);

    // Replace with new row
    ExecuteQuery(db, R"sql(
        REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
            (150, "Wolves love foxes.", "wolves data")
    )sql");

    CompareYson(R"([
        [[150u];["Wolves love foxes."];["wolves data"]]
    ])", FulltextSearch(db, "wolves"));

    // Replace existing row
    ExecuteQuery(db, R"sql(
        REPLACE INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, "Birds love foxes.", "birds data")
    )sql");

    // "cats" should now be empty
    CompareYson(R"([])", FulltextSearch(db, "cats"));

    // "birds" should match
    CompareYson(R"([
        [[100u];["Birds love foxes."];["birds data"]]
    ])", FulltextSearch(db, "birds"));

    // "foxes" should match keys 100, 150, 200
    CompareYson(R"([
        [[100u];["Birds love foxes."];["birds data"]];
        [[150u];["Wolves love foxes."];["wolves data"]];
        [[200u];["Dogs love foxes."];["dogs data"]]
    ])", FulltextSearch(db, "foxes"));
}

Y_UNIT_TEST(AddIndexCoveredCompact) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);
    AddIndexCovered(db, "fulltext_plain");

    auto index = ReadIndex(db);
    Cerr << "covered compact index: " << NYdb::FormatResultSetYson(index) << Endl;

    // Verify search works with covered index
    CompareYson(R"([
        [[100u];["Cats chase small animals."];["cats data"]];
        [[200u];["Dogs chase small cats."];["dogs data"]]
    ])", FulltextSearch(db, "chase"));
}

Y_UNIT_TEST_TWIN(Compaction, WithRelevance) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();
    const TString indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, indexType);

    // Set low compaction thresholds to trigger compaction
    NDataShard::gFulltextMaxDelta = 2;
    NDataShard::gFulltextMaxSegment = 2;

    // Insert enough rows to trigger compaction
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (150, "Foxes love cats.", "foxes data")
    )sql");
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (151, "Wolves love foxes.", "wolves data")
    )sql");
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (152, "Rabbits love foxes.", "rabbit data")
    )sql");

    // Reset to defaults
    NDataShard::gFulltextMaxDelta = 10000;
    NDataShard::gFulltextMaxSegment = 10000;

    // Despite compaction, search should still work correctly
    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]]
    ])", FulltextSearch(db, "cats"));

    CompareYson(R"([
        [[150u];["Foxes love cats."];["foxes data"]];
        [[151u];["Wolves love foxes."];["wolves data"]];
        [[152u];["Rabbits love foxes."];["rabbit data"]];
        [[200u];["Dogs love foxes."];["dogs data"]]
    ])", FulltextSearch(db, "foxes"));

    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]];
        [[151u];["Wolves love foxes."];["wolves data"]];
        [[152u];["Rabbits love foxes."];["rabbit data"]];
        [[200u];["Dogs love foxes."];["dogs data"]]
    ])", FulltextSearch(db, "love"));

    // Verify index table has multiple segments (compaction splits)
    auto index = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "index after compaction: " << index << Endl;
}

Y_UNIT_TEST_TWIN(CompactionWithDelete, WithRelevance) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();
    const TString indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, indexType);

    NDataShard::gFulltextMaxDelta = 2;
    NDataShard::gFulltextMaxSegment = 2;

    // Insert, then delete
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (150, "Foxes love cats.", "foxes data")
    )sql");
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (151, "Birds love rabbits.", "birds data")
    )sql");
    ExecuteQuery(db, R"sql(
        DELETE FROM `/Root/Texts` WHERE Key = 200
    )sql");

    NDataShard::gFulltextMaxDelta = 10000;
    NDataShard::gFulltextMaxSegment = 10000;

    // Verify correctness after compaction with delete
    CompareYson(R"([])", FulltextSearch(db, "dogs"));

    CompareYson(R"([
        [[151u];["Birds love rabbits."];["birds data"]]
    ])", FulltextSearch(db, "birds"));

    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]]
    ])", FulltextSearch(db, "cats"));

    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]];
        [[151u];["Birds love rabbits."];["birds data"]]
    ])", FulltextSearch(db, "love"));
}

Y_UNIT_TEST_TWIN(LsmCompaction, WithRelevance) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();
    const TString indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, indexType);

    // Insert more data to create multiple SST files
    NDataShard::gFulltextMaxDelta = 10000;
    NDataShard::gFulltextMaxSegment = 10000;

    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (150, "Foxes love cats.", "foxes data")
    )sql");
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (151, "Wolves love foxes.", "wolves data")
    )sql");
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (152, "Rabbits love foxes.", "rabbit data")
    )sql");

    auto indexBefore = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "index before LSM compaction: " << indexBefore << Endl;

    // Force LSM compaction on the index impl table
    auto* server = &kikimr.GetTestServer();
    WaitForCompaction(server, "/Root/Texts/fulltext_idx/indexImplTable");

    auto indexAfter = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "index after LSM compaction: " << indexAfter << Endl;

    // Verify search still returns correct results after LSM compaction
    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]]
    ])", FulltextSearch(db, "cats"));

    CompareYson(R"([
        [[150u];["Foxes love cats."];["foxes data"]];
        [[151u];["Wolves love foxes."];["wolves data"]];
        [[152u];["Rabbits love foxes."];["rabbit data"]];
        [[200u];["Dogs love foxes."];["dogs data"]]
    ])", FulltextSearch(db, "foxes"));

    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]];
        [[151u];["Wolves love foxes."];["wolves data"]];
        [[152u];["Rabbits love foxes."];["rabbit data"]];
        [[200u];["Dogs love foxes."];["dogs data"]]
    ])", FulltextSearch(db, "love"));
}

Y_UNIT_TEST_TWIN(LsmCompactionWithConcurrentWrites, WithRelevance) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();
    const TString indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, indexType);

    // Insert rows one by one to create multiple SST files in the index table
    NDataShard::gFulltextMaxDelta = 10000;
    NDataShard::gFulltextMaxSegment = 10000;

    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (150, "Foxes love cats.", "foxes data")
    )sql");
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (151, "Wolves love foxes.", "wolves data")
    )sql");
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (152, "Rabbits love foxes.", "rabbit data")
    )sql");

    // Open a snapshot transaction on the main table to pin row versions
    // (prevents the tablet from advancing MinRowVersion past this point)
    auto session = db.GetSession().GetValueSync().GetSession();
    auto snapshotResult = session.ExecuteQuery(R"sql(
        SELECT `Key`, `Text`, `Data`
        FROM `/Root/Texts`
        ORDER BY `Key`;
    )sql", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SnapshotRO())).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(snapshotResult.GetStatus(), EStatus::SUCCESS, snapshotResult.GetIssues().ToString());

    auto tx = snapshotResult.GetTransaction();
    UNIT_ASSERT(tx);
    UNIT_ASSERT(tx->IsActive());
    Cerr << "snapshot pinned with " << NYdb::FormatResultSetYson(snapshotResult.GetResultSet(0)) << Endl;

    // Insert more data while snapshot is held — creates new SST files
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (300, "Bears love honey.", "bears data")
    )sql");
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (301, "Eagles love fish.", "eagles data")
    )sql");

    // Verify search results before compaction
    auto loveBeforeCompaction = FulltextSearch(db, "love");
    Cerr << "love before compaction: " << loveBeforeCompaction << Endl;

    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]];
        [[151u];["Wolves love foxes."];["wolves data"]];
        [[152u];["Rabbits love foxes."];["rabbit data"]];
        [[200u];["Dogs love foxes."];["dogs data"]];
        [[300u];["Bears love honey."];["bears data"]];
        [[301u];["Eagles love fish."];["eagles data"]]
    ])", loveBeforeCompaction);

    // Force LSM compaction while the snapshot is held
    // The snapshot pins MinRowVersion, so compaction must not merge away
    // row versions that the snapshot might need
    auto* server = &kikimr.GetTestServer();
    WaitForCompaction(server, "/Root/Texts/fulltext_idx/indexImplTable");

    // Verify search results are identical after compaction
    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]];
        [[151u];["Wolves love foxes."];["wolves data"]];
        [[152u];["Rabbits love foxes."];["rabbit data"]];
        [[200u];["Dogs love foxes."];["dogs data"]];
        [[300u];["Bears love honey."];["bears data"]];
        [[301u];["Eagles love fish."];["eagles data"]]
    ])", FulltextSearch(db, "love"));

    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]]
    ])", FulltextSearch(db, "cats"));

    CompareYson(R"([
        [[150u];["Foxes love cats."];["foxes data"]];
        [[151u];["Wolves love foxes."];["wolves data"]];
        [[152u];["Rabbits love foxes."];["rabbit data"]];
        [[200u];["Dogs love foxes."];["dogs data"]]
    ])", FulltextSearch(db, "foxes"));

    CompareYson(R"([
        [[300u];["Bears love honey."];["bears data"]]
    ])", FulltextSearch(db, "honey"));

    CompareYson(R"([
        [[301u];["Eagles love fish."];["eagles data"]]
    ])", FulltextSearch(db, "fish"));

    // Close the snapshot
    auto commitResult = tx->Commit().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());

    // Run compaction again now that the snapshot is released
    // This time MinRowVersion can advance and compaction can merge more aggressively
    WaitForCompaction(server, "/Root/Texts/fulltext_idx/indexImplTable");

    // Verify all data is still correct after second compaction
    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]];
        [[151u];["Wolves love foxes."];["wolves data"]];
        [[152u];["Rabbits love foxes."];["rabbit data"]];
        [[200u];["Dogs love foxes."];["dogs data"]];
        [[300u];["Bears love honey."];["bears data"]];
        [[301u];["Eagles love fish."];["eagles data"]]
    ])", FulltextSearch(db, "love"));

    CompareYson(R"([
        [[100u];["Cats love cats."];["cats data"]];
        [[150u];["Foxes love cats."];["foxes data"]]
    ])", FulltextSearch(db, "cats"));

    CompareYson(R"([
        [[300u];["Bears love honey."];["bears data"]]
    ])", FulltextSearch(db, "honey"));
}

} // Y_UNIT_TEST_SUITE(KqpFulltextCompact)

Y_UNIT_TEST_SUITE(KqpJsonCompact) {

TResultSet ReadIndex(NQuery::TQueryClient& db, const char* table = "indexImplTable") {
    TString query = Sprintf(R"sql(
        SELECT * FROM `/Root/Texts/json_idx/%s`;
    )sql", table);
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return result.GetResultSet(0);
}

Y_UNIT_TEST(AddJsonCompactIndex) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();

    ExecuteQuery(db, R"sql(
        CREATE TABLE `/Root/Texts` (
            Key Uint64,
            Text Json,
            Data String,
            PRIMARY KEY (Key)
        );
    )sql");

    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, '"hello world"', "data1"),
            (200, '42', "data2"),
            (300, 'true', "data3"),
            (400, '{"name":"test","value":123}', "data4")
    )sql");

    ExecuteQuery(db, R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX json_idx
            GLOBAL USING json ON (Text)
    )sql");

    auto index = ReadIndex(db);
    auto indexStr = NYdb::FormatResultSetYson(index);
    Cerr << "json index: " << indexStr << Endl;

    NYdb::TResultSetParser parser(index);
    UNIT_ASSERT(parser.RowsCount() > 0);
}

Y_UNIT_TEST(JsonCompactInsertRow) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();

    ExecuteQuery(db, R"sql(
        CREATE TABLE `/Root/Texts` (
            Key Uint64,
            Text Json,
            Data String,
            PRIMARY KEY (Key)
        );
    )sql");

    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, '"hello"', "data1"),
            (200, '"world"', "data2")
    )sql");

    ExecuteQuery(db, R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX json_idx
            GLOBAL USING json ON (Text)
    )sql");

    auto indexBefore = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "index before insert: " << indexBefore << Endl;

    // Insert new row
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (150, '{"nested":"value"}', "data3")
    )sql");

    auto indexAfter = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "index after insert: " << indexAfter << Endl;

    NYdb::TResultSetParser parser(ReadIndex(db));
    UNIT_ASSERT(parser.RowsCount() > 0);
}

Y_UNIT_TEST(JsonCompactUpsertModify) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();

    ExecuteQuery(db, R"sql(
        CREATE TABLE `/Root/Texts` (
            Key Uint64,
            Text Json,
            Data String,
            PRIMARY KEY (Key)
        );
    )sql");

    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, '"hello"', "data1"),
            (200, '"world"', "data2")
    )sql");

    ExecuteQuery(db, R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX json_idx
            GLOBAL USING json ON (Text)
    )sql");

    // Upsert: modify existing
    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, '{"changed":"yes"}', "data1_modified")
    )sql");

    auto index = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "index after upsert modify: " << index << Endl;

    NYdb::TResultSetParser parser(ReadIndex(db));
    UNIT_ASSERT(parser.RowsCount() > 0);
}

Y_UNIT_TEST(JsonCompactDeleteRow) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();

    ExecuteQuery(db, R"sql(
        CREATE TABLE `/Root/Texts` (
            Key Uint64,
            Text Json,
            Data String,
            PRIMARY KEY (Key)
        );
    )sql");

    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, '"hello"', "data1"),
            (200, '"world"', "data2"),
            (300, '42', "data3")
    )sql");

    ExecuteQuery(db, R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX json_idx
            GLOBAL USING json ON (Text)
    )sql");

    auto indexBefore = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "index before delete: " << indexBefore << Endl;

    // Delete row
    ExecuteQuery(db, R"sql(
        DELETE FROM `/Root/Texts` WHERE Key = 100
    )sql");

    auto indexAfter = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "index after delete: " << indexAfter << Endl;
}

Y_UNIT_TEST(JsonCompactCompaction) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();

    ExecuteQuery(db, R"sql(
        CREATE TABLE `/Root/Texts` (
            Key Uint64,
            Text Json,
            Data String,
            PRIMARY KEY (Key)
        );
    )sql");

    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, '"hello"', "data1"),
            (200, '"world"', "data2")
    )sql");

    ExecuteQuery(db, R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX json_idx
            GLOBAL USING json ON (Text)
    )sql");

    NDataShard::gFulltextMaxDelta = 2;
    NDataShard::gFulltextMaxSegment = 2;

    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (150, '{"a":"b"}', "data3")
    )sql");
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (151, '{"c":"d"}', "data4")
    )sql");
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (152, '"test"', "data5")
    )sql");

    NDataShard::gFulltextMaxDelta = 10000;
    NDataShard::gFulltextMaxSegment = 10000;

    auto index = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "json_compact index after compaction: " << index << Endl;

    NYdb::TResultSetParser parser(ReadIndex(db));
    UNIT_ASSERT(parser.RowsCount() > 0);
}

} // Y_UNIT_TEST_SUITE(KqpJsonCompact)

} // namespace NKikimr::NKqp
