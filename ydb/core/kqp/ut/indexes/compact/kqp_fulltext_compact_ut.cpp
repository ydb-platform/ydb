#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/simple/services.h>
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

TString FulltextSearch(NQuery::TQueryClient& db, const char* searchQuery, const char* field = "Text", const char* idxName = "fulltext_idx") {
    TString query = Sprintf(R"sql(
        SELECT `Key`, `Text`, `Data`
        FROM `/Root/Texts` VIEW `%s`
        WHERE FulltextMatch(`%s`, "%s")
        ORDER BY `Key`;
    )sql", idxName, field, searchQuery);
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

void RestartTableShards(TKikimrRunner& kikimr, const TString& path) {
    auto& server = kikimr.GetTestServer();
    auto& runtime = *server.GetRuntime();
    const auto sender = runtime.AllocateEdgeActor();
    const auto shards = GetTableShards(&server, sender, path);
    UNIT_ASSERT_C(!shards.empty(), "No shards found for " << path);
    for (const ui64 shard : shards) {
        runtime.Send(MakePipePerNodeCacheID(false), NActors::TActorId(),
            new TEvPipeCache::TEvForward(new TEvents::TEvPoisonPill(), shard, false));
    }
}

void RestartFulltextShards(TKikimrRunner& kikimr, NQuery::TQueryClient& db, bool withRelevance) {
    RestartTableShards(kikimr, "/Root/Texts");
    RestartTableShards(kikimr, "/Root/Texts/fulltext_idx/indexImplTable");
    if (withRelevance) {
        RestartTableShards(kikimr, "/Root/Texts/fulltext_idx/indexImplDocsTable");
        RestartTableShards(kikimr, "/Root/Texts/fulltext_idx/indexImplStatsTable");
    }

    // A successful index query is the recovery barrier. RetryQuery waits for all
    // poisoned tablets used by the plan to boot, without relying on a fixed delay.
    auto result = db.RetryQuery([](NQuery::TSession session) {
        return session.ExecuteQuery(R"sql(
            SELECT `Key`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "love")
            ORDER BY `Key`;
        )sql", NQuery::TTxControl::NoTx());
    }).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

void CreatePartitionedTexts(NQuery::TQueryClient& db) {
    ExecuteQuery(db, R"sql(
        CREATE TABLE `/Root/Texts` (
            Key Uint64,
            Text String,
            Data String,
            PRIMARY KEY (Key)
        ) WITH (
            PARTITION_AT_KEYS = (150, 300)
        );
    )sql");
}

void UpdateKqpCompactFlag(TKikimrRunner& kikimr, bool enabled) {
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    const auto edge = runtime.AllocateEdgeActor();
    auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
    auto* config = request->Record.MutableConfig();
    auto* flags = config->MutableFeatureFlags();
    flags->SetEnableFulltextIndex(true);
    flags->SetEnableCompactFulltextIndex(enabled);
    flags->SetEnableFulltextIndexRowId(true);
    flags->SetEnableJsonIndex(true);
    flags->SetEnableAddUniqueIndex(true);
    config->MutableTableServiceConfig()->SetBackportMode(
        NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    config->MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

    runtime.Send(MakeKqpProxyID(runtime.GetNodeId()), edge, request.Release());
    auto response = runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(
        edge, TDuration::Seconds(10));
    UNIT_ASSERT_C(response, "KQP proxy must acknowledge FeatureFlags update");
}

bool IsCompactImplementation(TKikimrRunner& kikimr, const TString& indexName) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    auto describe = session.DescribeTable(
        TStringBuilder() << "/Root/Texts/" << indexName << "/indexImplTable").ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, describe.GetIssues().ToString());
    for (const auto& column : describe.GetTableDescription().GetColumns()) {
        if (column.Name == NTableIndex::NFulltext::GenColumn) {
            return true;
        }
    }
    return false;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpFulltextCompact) {

Y_UNIT_TEST(AddIndexCompact) {
    auto kikimr = KikimrWithCompact(true);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);
    AddIndex(db, "fulltext_plain");

    auto index = ReadIndex(db);
    Cerr << "index: " << NYdb::FormatResultSetYson(index) << "\n";

    CompareYson(R"([
        [%true;18446744073709551615u;100u;"d";"animals"];
        [%true;18446744073709551615u;300u;"ddd";"cats"];
        [%true;18446744073709551615u;200u;"dd";"chase"];
        [%true;18446744073709551615u;400u;"\xC8\1\xC8\1";"dogs"];
        [%true;18446744073709551615u;400u;"\x90\3";"foxes"];
        [%true;18446744073709551615u;400u;"\xAC\2d";"love"];
        [%true;18446744073709551615u;200u;"dd";"small"]
    ])", NYdb::FormatResultSetYson(index));
}

// ALTER ADD INDEX uses the public asynchronous BuildIndex path. The request carries a logical fulltext
// kind; SchemeShard resolves it to the physical legacy/compact type using its own cached flag. Deliver an
// update only to KQP to pin that authority boundary and ensure the skew still produces one coherent layout.
// Existing indexes must keep accepting DML/read/drop across later KQP toggles.
Y_UNIT_TEST_TWIN(KqpCompactFlagSkewKeepsSqlIndexTypeConsistent, SchemeShardCompact) {
    auto kikimr = SchemeShardCompact ? KikimrWithCompact(true) : Kikimr();
    auto db = kikimr.GetQueryClient();
    CreateTexts(db);
    UpsertSomeTexts(db);

    ExecuteQuery(db, R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX compact_idx
            GLOBAL USING fulltext_plain ON (Text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql");
    UNIT_ASSERT_VALUES_EQUAL_C(IsCompactImplementation(kikimr, "compact_idx"), SchemeShardCompact,
        "initial physical schema must follow SchemeShard's compact flag");

    // Change only KQP to the opposite value. The public BuildIndex path is owned by SchemeShard, so the
    // accepted build must still follow SchemeShard's cached flag (compact or legacy, never a mixed layout).
    UpdateKqpCompactFlag(kikimr, /*enabled=*/!SchemeShardCompact);
    ExecuteQuery(db, R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX skew_idx
            GLOBAL USING fulltext_plain ON (Text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql");
    UNIT_ASSERT_VALUES_EQUAL_C(IsCompactImplementation(kikimr, "skew_idx"), SchemeShardCompact,
        "SchemeShard must remain authoritative for public BuildIndex");

    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (150, "Foxes love cats.", "foxes data");
        UPDATE `/Root/Texts` SET Text = "Dogs chase wolves." WHERE Key = 200;
    )sql");
    for (const TString& index : {TString("compact_idx"), TString("skew_idx")}) {
        auto result = db.ExecuteQuery(Sprintf(R"sql(
            SELECT Key FROM `/Root/Texts` VIEW `%s`
            WHERE FulltextMatch(Text, "cats") ORDER BY Key;
        )sql", index.c_str()), NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[[100u]];[[150u]]]");
    }

    // Drop one existing index while KQP has the opposite view, restore KQP's flag, then prove the other
    // existing physical layout is still readable and droppable.
    ExecuteQuery(db, "ALTER TABLE `/Root/Texts` DROP INDEX compact_idx;");
    UpdateKqpCompactFlag(kikimr, /*enabled=*/SchemeShardCompact);
    auto legacyRead = db.ExecuteQuery(R"sql(
        SELECT Key FROM `/Root/Texts` VIEW `skew_idx`
        WHERE FulltextMatch(Text, "cats") ORDER BY Key;
    )sql", NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(legacyRead.GetStatus(), EStatus::SUCCESS, legacyRead.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(legacyRead.GetResultSet(0)), "[[[100u]];[[150u]]]");
    ExecuteQuery(db, "ALTER TABLE `/Root/Texts` DROP INDEX skew_idx;");
}

Y_UNIT_TEST_TWIN(AddIndexCompactRelevance, Covered) {
    auto kikimr = KikimrWithCompact(true);
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
    Cerr << "index: " << NYdb::FormatResultSetYson(index) << "\n";
    CompareYson(R"([
        [%true;18446744073709551615u;100u;"\xA4\1";"animals"];
        [%true;18446744073709551615u;300u;"\xA4\1\xA4\1\xE4\1\2";"cats"];
        [%true;18446744073709551615u;200u;"\xA4\1\xA4\1";"chase"];
        [%true;18446744073709551615u;400u;"\x88\3\x88\3";"dogs"];
        [%true;18446744073709551615u;400u;"\x90\6";"foxes"];
        [%true;18446744073709551615u;400u;"\xAC\4\xA4\1";"love"];
        [%true;18446744073709551615u;200u;"\xA4\1\xA4\1";"small"]
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

    index = ReadIndex(db, NTableIndex::NFulltext::StatsTable);
    CompareYson(R"([
        [4u;0u;14u]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST_TWIN(FulltextCompactUpdateRequiresStreamWrite, WithRelevance) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableCompactFulltextIndex(true);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(false);
    auto kikimr = TKikimrRunner(settings);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, WithRelevance ? "fulltext_relevance" : "fulltext_plain");

    TVector<TString> queries = {
        "INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES (150, \"Foxes love cats.\", \"foxes data\")",
        "UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES (150, \"Foxes love cats.\", \"foxes data\")",
        "UPDATE `/Root/Texts` SET Text=\"Foxes love cats\" WHERE Key=100",
        "DELETE FROM `/Root/Texts` WHERE Key=100"
    };
    for (auto& query: queries) {
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::INTERNAL_ERROR, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST_TWIN(InsertRow, WithRelevance) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, WithRelevance ? "fulltext_relevance" : "fulltext_plain");
    auto index = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "indexImplTable: " << index << Endl;
    if (WithRelevance) {
        auto docs = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::DocsTable));
        auto stats = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::StatsTable));
        Cerr << "indexImplDocsTable: " << docs << Endl;
        Cerr << "indexImplStatsTable: " << stats << Endl;
        CompareYson(R"([
            [%true;18446744073709551615u;100u;"\xE4\1\2";"cats"];
            [%true;18446744073709551615u;200u;"\x88\3";"dogs"];
            [%true;18446744073709551615u;200u;"\x88\3";"foxes"];
            [%true;18446744073709551615u;200u;"\xA4\1\xA4\1";"love"]
        ])", index);
        CompareYson(R"([
            [[100u];3u];
            [[200u];3u]
        ])", docs);
        CompareYson(R"([
            [2u;0u;6u]
        ])", stats);
    } else {
        CompareYson(R"([
            [%true;18446744073709551615u;100u;"d";"cats"];
            [%true;18446744073709551615u;200u;"\xC8\1";"dogs"];
            [%true;18446744073709551615u;200u;"\xC8\1";"foxes"];
            [%true;18446744073709551615u;200u;"dd";"love"]
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
        auto stats = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::StatsTable));
        Cerr << "indexImplDocsTable: " << docs << Endl;
        Cerr << "indexImplStatsTable: " << stats << Endl;
        CompareYson(R"([
            [%true;18446744073709551614u;150u;"\x96\2";"cats"];
            [%true;18446744073709551615u;100u;"\xE4\1\2";"cats"];
            [%true;18446744073709551615u;200u;"\x88\3";"dogs"];
            [%true;18446744073709551614u;150u;"\x96\2";"foxes"];
            [%true;18446744073709551615u;200u;"\x88\3";"foxes"];
            [%true;18446744073709551614u;150u;"\x96\2";"love"];
            [%true;18446744073709551615u;200u;"\xA4\1\xA4\1";"love"]
        ])", index);
        CompareYson(R"([
            [[100u];3u];
            [[150u];3u];
            [[200u];3u]
        ])", docs);
        CompareYson(R"([
            [3u;0u;9u]
        ])", stats);
    } else {
        CompareYson(R"([
            [%true;18446744073709551614u;150u;"\x96\1";"cats"];
            [%true;18446744073709551615u;100u;"d";"cats"];
            [%true;18446744073709551615u;200u;"\xC8\1";"dogs"];
            [%true;18446744073709551614u;150u;"\x96\1";"foxes"];
            [%true;18446744073709551615u;200u;"\xC8\1";"foxes"];
            [%true;18446744073709551614u;150u;"\x96\1";"love"];
            [%true;18446744073709551615u;200u;"dd";"love"]
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
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

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
    auto kikimr = KikimrWithCompact(true);
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
    auto kikimr = KikimrWithCompact(true);
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

    auto stats = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::StatsTable));
    CompareYson(R"([[3u;0u;9u]])", stats);
}

Y_UNIT_TEST_TWIN(UpsertModifyExisting, WithRelevance) {
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

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
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

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
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

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
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

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
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

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
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

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
    auto kikimr = KikimrWithCompact(true);
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
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

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
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

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

TKikimrRunner KikimrWithZeroSnapshotTimeout() {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    featureFlags.SetEnableCompactFulltextIndex(true);
    featureFlags.SetEnableJsonIndex(true);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
    // Set KeepSnapshotTimeout to 0 and CleanupSnapshotPeriod to 100ms so that
    // MVCC watermark can advance quickly. This allows compaction to merge old
    // row versions without waiting for the default 5-minute / 15-second timeouts.
    settings.AppConfig.MutableDataShardConfig()->SetKeepSnapshotTimeout(0);
    settings.AppConfig.MutableDataShardConfig()->SetCleanupSnapshotPeriod(100);
    return TKikimrRunner(settings);
}

Y_UNIT_TEST_TWIN(LsmCompaction, WithRelevance) {
    auto kikimr = KikimrWithZeroSnapshotTimeout();
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

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

    // Wait for the MVCC cleanup timer to fire and advance the watermark,
    // so that RemovedRowVersions covers all written versions.
    // CleanupSnapshotPeriod is set to 100ms in KikimrWithCompact(true).
    Sleep(TDuration::Seconds(1));

    // Force LSM compaction on the index impl table
    auto* server = &kikimr.GetTestServer();
    WaitForCompaction(server, "/Root/Texts/fulltext_idx/indexImplTable");

    auto indexAfter = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "index after LSM compaction: " << indexAfter << Endl;

    // Verify that compaction actually merged segments (fewer rows in the index)
    UNIT_ASSERT_C(indexBefore != indexAfter,
        "Index content should change after LSM compaction (segments should merge)");

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
    auto kikimr = KikimrWithZeroSnapshotTimeout();
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

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

    // Wait for the MVCC cleanup timer to fire and advance the watermark,
    // so that RemovedRowVersions covers all written versions.
    // CleanupSnapshotPeriod is set to 100ms in KikimrWithCompact(true).
    Sleep(TDuration::Seconds(1));

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

Y_UNIT_TEST_TWIN(RecoveryBeforeAndAfterCompaction, WithRelevance) {
    auto kikimr = KikimrWithZeroSnapshotTimeout();
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, indexType);

    // Produce several durable posting generations with all supported mutation kinds.
    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (150, "Foxes love cats.", "foxes data")
    )sql");
    ExecuteQuery(db, R"sql(
        UPDATE `/Root/Texts`
        SET Text = "Birds love rabbits.", Data = "birds data"
        WHERE Key = 100
    )sql");
    ExecuteQuery(db, R"sql(
        DELETE FROM `/Root/Texts` WHERE Key = 200
    )sql");

    RestartFulltextShards(kikimr, db, WithRelevance);

    // Recovery must preserve both additions and tombstones from pre-restart generations.
    CompareYson(R"([])", FulltextSearch(db, "dogs"));
    CompareYson(R"([
        [[100u];["Birds love rabbits."];["birds data"]]
    ])", FulltextSearch(db, "birds"));
    CompareYson(R"([
        [[150u];["Foxes love cats."];["foxes data"]]
    ])", FulltextSearch(db, "cats"));

    // Force small logical segments, then force an LSM compaction. WaitForCompaction
    // synchronizes on tablet completion, so this test does not depend on a timer or Sleep.
    NDataShard::gFulltextMaxDelta = 2;
    NDataShard::gFulltextMaxSegment = 2;
    Y_DEFER {
        NDataShard::gFulltextMaxDelta = 10000;
        NDataShard::gFulltextMaxSegment = 10000;
    };

    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (151, "Wolves love foxes.", "wolves data")
    )sql");
    ExecuteQuery(db, R"sql(
        UPDATE `/Root/Texts`
        SET Text = "Otters chase mice.", Data = "otters data"
        WHERE Key = 150
    )sql");
    ExecuteQuery(db, R"sql(
        DELETE FROM `/Root/Texts` WHERE Key = 100
    )sql");

    WaitForCompaction(&kikimr.GetTestServer(), "/Root/Texts/fulltext_idx/indexImplTable");
    RestartFulltextShards(kikimr, db, WithRelevance);

    // Verify recovery of the compacted generations and continued handling of
    // post-restart writes. Old terms must not leak through compacted tombstones.
    CompareYson(R"([])", FulltextSearch(db, "birds"));
    CompareYson(R"([])", FulltextSearch(db, "cats"));
    CompareYson(R"([
        [[150u];["Otters chase mice."];["otters data"]]
    ])", FulltextSearch(db, "otters"));
    CompareYson(R"([
        [[151u];["Wolves love foxes."];["wolves data"]]
    ])", FulltextSearch(db, "foxes"));

    ExecuteQuery(db, R"sql(
        INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (152, "Badgers chase otters.", "badgers data")
    )sql");
    CompareYson(R"([
        [[150u];["Otters chase mice."];["otters data"]];
        [[152u];["Badgers chase otters."];["badgers data"]]
    ])", FulltextSearch(db, "otters"));
}

// Build compact postings from all three main-table partitions, mutate rows on both sides of each boundary,
// and reboot every main/implementation tablet. Index queries and GetTableShards are the recovery/topology
// barriers, so this test has no sleeps or timing assumptions. Plain and relevance layouts share the same
// posting lifecycle.
Y_UNIT_TEST_TWIN(MultiShardBuildDmlAndRecovery, WithRelevance) {
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();
    const char* indexType = WithRelevance ? "fulltext_relevance" : "fulltext_plain";

    CreatePartitionedTexts(db);
    UpsertTexts(db);

    auto& server = kikimr.GetTestServer();
    auto& runtime = *server.GetRuntime();
    const auto sender = runtime.AllocateEdgeActor();
    auto mainShards = GetTableShards(&server, sender, "/Root/Texts");
    UNIT_ASSERT_VALUES_EQUAL_C(mainShards.size(), 3u,
        "PARTITION_AT_KEYS must create a three-shard source for the compact index build");

    AddIndex(db, indexType);
    CompareYson(R"([
        [[300u];["Cats love cats."];["cats cats data"]];
        [[400u];["Foxes love dogs."];["foxes data"]]
    ])", FulltextSearch(db, "love"));

    // Create durable add/update/delete generations spanning all three source partitions.
    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (50, "Owls love mice.", "owls data"),
            (200, "Dogs chase wolves.", "dogs updated")
    )sql");
    ExecuteQuery(db, R"sql(
        DELETE FROM `/Root/Texts` WHERE Key = 300
    )sql");
    CompareYson(R"([
        [[50u];["Owls love mice."];["owls data"]];
        [[400u];["Foxes love dogs."];["foxes data"]]
    ])", FulltextSearch(db, "love"));
    CompareYson(R"([
        [[100u];["Cats chase small animals."];["cats data"]];
        [[200u];["Dogs chase wolves."];["dogs updated"]]
    ])", FulltextSearch(db, "chase"));

    // Continue with several writes in the final partition, while retaining updates in both lower ones.
    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, "Cats love otters.", "cats updated"),
            (325, "Badgers chase owls.", "badgers data"),
            (350, "Wolves love badgers.", "wolves data")
    )sql");
    CompareYson(R"([
        [[50u];["Owls love mice."];["owls data"]];
        [[100u];["Cats love otters."];["cats updated"]];
        [[350u];["Wolves love badgers."];["wolves data"]];
        [[400u];["Foxes love dogs."];["foxes data"]]
    ])", FulltextSearch(db, "love"));
    CompareYson(R"([
        [[200u];["Dogs chase wolves."];["dogs updated"]];
        [[325u];["Badgers chase owls."];["badgers data"]]
    ])", FulltextSearch(db, "chase"));

    // Reboot every main and implementation shard after the split. RestartFulltextShards uses a successful
    // compact index read as its boot barrier and also covers relevance-only docs/dict/stats tables.
    RestartFulltextShards(kikimr, db, WithRelevance);
    CompareYson(R"([
        [[50u];["Owls love mice."];["owls data"]];
        [[100u];["Cats love otters."];["cats updated"]];
        [[350u];["Wolves love badgers."];["wolves data"]];
        [[400u];["Foxes love dogs."];["foxes data"]]
    ])", FulltextSearch(db, "love"));
    CompareYson(R"([
        [[200u];["Dogs chase wolves."];["dogs updated"]];
        [[325u];["Badgers chase owls."];["badgers data"]]
    ])", FulltextSearch(db, "chase"));
}

// Regression for a late compact-generation sequence response. Previously SendGenSequenceRequests ran
// while a DELETE task was still buffering. If its lookup found no row, the task could finish and be erased
// before TEvNextValResult arrived, and HandleGenSequence aborted on a missing WriteTasks cookie. Put that
// no-op DELETE and a new-row UPSERT in one request, as Query Service workloads do. A custom Utf8 PK also
// exercises the auto-provisioned __ydb_row_id path used by fulltext indexes.
Y_UNIT_TEST(DeleteMissingThenUpsertNewWithRowId) {
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();

    ExecuteQuery(db, R"sql(
        CREATE TABLE `/Root/Texts` (
            Key Utf8,
            Text String,
            Data String,
            PRIMARY KEY (Key)
        );
    )sql");
    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            ("a", "Cats love cats.", "initial");
    )sql");
    AddIndex(db, "fulltext_relevance");

    const TString request = R"sql(
        DELETE FROM `/Root/Texts` WHERE Key = "missing";
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            ("b", "Dogs love cats.", "inserted");
    )sql";

    // Explicit autocommit is the production trigger. Repeating the same logical request covers a retry
    // after the new row already exists and must not leave another asynchronous generation response behind.
    for (ui32 attempt = 0; attempt < 2; ++attempt) {
        auto result = db.ExecuteQuery(
            request,
            NQuery::TTxControl::BeginTx(NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    // A rolled-back request may consume sequence values, but neither its main row nor postings may leak.
    auto session = db.GetSession().GetValueSync().GetSession();
    auto pending = session.ExecuteQuery(R"sql(
        DELETE FROM `/Root/Texts` WHERE Key = "also-missing";
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            ("rollback", "Rollback cats.", "rollback");
    )sql", NQuery::TTxControl::BeginTx(NQuery::TTxSettings::SerializableRW())).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(pending.GetStatus(), EStatus::SUCCESS, pending.GetIssues().ToString());
    UNIT_ASSERT(pending.GetTransaction());
    auto rollback = pending.GetTransaction()->Rollback().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(rollback.GetStatus(), EStatus::SUCCESS, rollback.GetIssues().ToString());

    auto rows = db.ExecuteQuery(R"sql(
        SELECT Key, __ydb_row_id FROM `/Root/Texts` ORDER BY Key;
    )sql", NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(rows.GetStatus(), EStatus::SUCCESS, rows.GetIssues().ToString());
    TResultSetParser rowParser(rows.GetResultSet(0));
    TSet<TString> keys;
    TSet<ui64> rowIds;
    while (rowParser.TryNextRow()) {
        keys.insert(TString(*rowParser.ColumnParser("Key").GetOptionalUtf8()));
        rowIds.insert(rowParser.ColumnParser("__ydb_row_id").GetUint64());
    }
    UNIT_ASSERT_VALUES_EQUAL((TSet<TString>{"a", "b"}), keys);
    UNIT_ASSERT_VALUES_EQUAL_C(rowIds.size(), 2u, "committed rows must retain distinct generated row ids");

    auto search = db.ExecuteQuery(R"sql(
        SELECT Key FROM `/Root/Texts` VIEW `fulltext_idx`
        WHERE FulltextMatch(Text, "cats") ORDER BY Key;
    )sql", NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(search.GetStatus(), EStatus::SUCCESS, search.GetIssues().ToString());
    TResultSetParser searchParser(search.GetResultSet(0));
    TSet<TString> matches;
    while (searchParser.TryNextRow()) {
        matches.insert(TString(*searchParser.ColumnParser("Key").GetOptionalUtf8()));
    }
    UNIT_ASSERT_VALUES_EQUAL((TSet<TString>{"a", "b"}), matches);
}

Y_UNIT_TEST(UpsertTwoIndexes) {
    auto kikimr = KikimrWithCompact(true);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertSomeTexts(db);

    {
        TString query = R"sql(
            ALTER TABLE `/Root/Texts` ADD INDEX idx_text
                GLOBAL USING fulltext_plain
                ON (Text)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            ALTER TABLE `/Root/Texts` ADD INDEX idx_data
                GLOBAL USING fulltext_plain
                ON (Data)
                WITH (tokenizer=standard, use_filter_lowercase=true)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    // Update only data, then update both fields - each index should use its own sequence
    ExecuteQuery(db, "UPDATE `/Root/Texts` SET Data=\"birds data\" WHERE Key=100");
    ExecuteQuery(db, "UPDATE `/Root/Texts` SET Data=\"wolves data\", Text=\"Wolves love rabbits.\" WHERE Key=200");

    // Check index tables
    {
        auto result = db.ExecuteQuery("SELECT * FROM `/Root/Texts/idx_data/indexImplTable`", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto idx = NYdb::FormatResultSetYson(result.GetResultSet(0));
        Cerr << "idx_data: " << idx << "\n";
        CompareYson(R"([
            [%true;18446744073709551614u;100u;"d";"birds"];
            [%false;18446744073709551613u;100u;"d";"cats"];
            [%true;18446744073709551615u;100u;"d";"cats"];
            [%false;18446744073709551611u;200u;"\xC8\1";"data"];
            [%true;18446744073709551612u;200u;"\xC8\1";"data"];
            [%false;18446744073709551613u;100u;"d";"data"];
            [%true;18446744073709551614u;100u;"d";"data"];
            [%true;18446744073709551615u;200u;"dd";"data"];
            [%false;18446744073709551611u;200u;"\xC8\1";"dogs"];
            [%true;18446744073709551615u;200u;"\xC8\1";"dogs"];
            [%true;18446744073709551612u;200u;"\xC8\1";"wolves"]
        ])", idx);
    }
    {
        auto result = db.ExecuteQuery("SELECT * FROM `/Root/Texts/idx_text/indexImplTable`", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto idx = NYdb::FormatResultSetYson(result.GetResultSet(0));
        Cerr << "idx_text: " << idx << "\n";
        CompareYson(R"([
            [%true;18446744073709551615u;100u;"d";"cats"];
            [%false;18446744073709551613u;200u;"\xC8\1";"dogs"];
            [%true;18446744073709551615u;200u;"\xC8\1";"dogs"];
            [%false;18446744073709551613u;200u;"\xC8\1";"foxes"];
            [%true;18446744073709551615u;200u;"\xC8\1";"foxes"];
            [%false;18446744073709551613u;200u;"\xC8\1";"love"];
            [%true;18446744073709551614u;200u;"\xC8\1";"love"];
            [%true;18446744073709551615u;200u;"dd";"love"];
            [%true;18446744073709551614u;200u;"\xC8\1";"rabbits"];
            [%true;18446744073709551614u;200u;"\xC8\1";"wolves"]
        ])", idx);
    }
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
    auto kikimr = KikimrWithCompact(true);
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
    auto kikimr = KikimrWithCompact(true);
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
    auto kikimr = KikimrWithCompact(true);
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
    auto kikimr = KikimrWithCompact(true);
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
    auto kikimr = KikimrWithCompact(true);
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

Y_UNIT_TEST(JsonCompactUpdateRequiresStreamWrite) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableCompactFulltextIndex(true);
    featureFlags.SetEnableJsonIndex(true);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(false);
    auto kikimr = TKikimrRunner(settings);
    auto db = kikimr.GetQueryClient();

    ExecuteQuery(db, R"sql(
        CREATE TABLE `/Root/Texts` (
            Key Uint64,
            Text Json,
            Data String,
            PRIMARY KEY (Key),
            INDEX json_idx GLOBAL USING json ON (Text)
        );
    )sql");

    TVector<TString> queries = {
        "INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES (150, '{\"nested\":\"value\"}', \"data3\")",
        "UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES (150, '{\"nested\":\"value\"}', \"data3\")",
        "UPDATE `/Root/Texts` SET Text='{\"nested\":\"value\"}' WHERE Key=150",
        "DELETE FROM `/Root/Texts` WHERE Key=150"
    };
    for (auto& query: queries) {
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::INTERNAL_ERROR, result.GetIssues().ToString());
    }
}

} // Y_UNIT_TEST_SUITE(KqpJsonCompact)

} // namespace NKikimr::NKqp
