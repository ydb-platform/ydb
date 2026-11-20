#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

TKikimrRunner MakeCompactIndexCopyRunner() {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    featureFlags.SetEnableJsonIndex(true);
    featureFlags.SetEnableJsonIndexAutoSelect(true);
    featureFlags.SetEnableCompactFulltextIndex(true);
    featureFlags.SetEnableFulltextIndexRowId(true);
    featureFlags.SetEnableAddUniqueIndex(true);

    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(
        NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
    return TKikimrRunner(settings);
}

void Execute(TQueryClient& db, const TString& query) {
    auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

TString SelectYson(TQueryClient& db, const TString& query) {
    auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return FormatResultSetYson(result.GetResultSet(0));
}

THashMap<TString, ui64> ReadRowIds(TQueryClient& db, const TString& table) {
    auto result = db.ExecuteQuery(TStringBuilder() << R"sql(
        SELECT Key, __ydb_row_id FROM `)sql" << table << R"sql(` ORDER BY Key;
    )sql", TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    THashMap<TString, ui64> rowIds;
    TResultSetParser parser(result.GetResultSet(0));
    while (parser.TryNextRow()) {
        const TString key(parser.ColumnParser("Key").GetUtf8());
        const ui64 rowId = parser.ColumnParser("__ydb_row_id").GetUint64();
        UNIT_ASSERT_C(rowId != 0, "generated row id must be non-zero for key " << key);
        UNIT_ASSERT_C(rowIds.emplace(key, rowId).second, "duplicate key in row-id oracle: " << key);
    }
    return rowIds;
}

void AssertSingleRowIdInfrastructure(TKikimrRunner& kikimr, const TString& table) {
    auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    auto describe = tableSession.DescribeTable(table).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, describe.GetIssues().ToString());

    ui32 rowIdColumns = 0;
    for (const auto& column : describe.GetTableDescription().GetColumns()) {
        rowIdColumns += column.Name == NTableIndex::NFulltext::RowIdColumn;
    }
    UNIT_ASSERT_VALUES_EQUAL_C(rowIdColumns, 1u,
        "copy must contain exactly one generated __ydb_row_id column");

    THashSet<TString> indexNames;
    for (const auto& index : describe.GetTableDescription().GetIndexDescriptions()) {
        UNIT_ASSERT_C(indexNames.insert(TString(index.GetIndexName())).second,
            "duplicate copied index " << index.GetIndexName());
    }
    UNIT_ASSERT_VALUES_EQUAL_C(indexNames.size(), 4u,
        "three search indexes must share exactly one row-id unique index");
    for (const TString& expected : {
            TString("ft_plain"), TString("ft_relevance"), TString("json_idx"),
            TString(NTableIndex::NFulltext::RowIdUniqueIndexName)}) {
        UNIT_ASSERT_C(indexNames.contains(expected), "missing copied index " << expected);
    }

    // The SDK does not expose private sequence paths through DescribePath/ListDirectory. The existing
    // low-level consistent-copy tests verify the exact DefaultFromSequence metadata and private path;
    // the post-copy INSERT below is the data-level proof that the copied default remains operational.
}

} // namespace

Y_UNIT_TEST_SUITE(KqpSchemeIndexCopy) {
    Y_UNIT_TEST(ConsistentCopyCompactSearchIndexesWithRowIdData) {
        auto kikimr = MakeCompactIndexCopyRunner();
        auto db = kikimr.GetQueryClient();

        Execute(db, R"sql(
            CREATE TABLE `/Root/Source` (
                Key Utf8 NOT NULL,
                Text String,
                Payload JsonDocument,
                PRIMARY KEY (Key),
                INDEX ft_plain GLOBAL USING fulltext_plain ON (Text)
                    WITH (tokenizer=standard, use_filter_lowercase=true),
                INDEX ft_relevance GLOBAL USING fulltext_relevance ON (Text)
                    WITH (tokenizer=standard, use_filter_lowercase=true),
                INDEX json_idx GLOBAL USING json ON (Payload)
            );
        )sql");
        Execute(db, R"sql(
            UPSERT INTO `/Root/Source` (Key, Text, Payload) VALUES
                ("a"u, "alpha cats",  JsonDocument('{"tag":"pet","rank":1}')),
                ("b"u, "beta dogs",   JsonDocument('{"tag":"pet","rank":2}')),
                ("c"u, "alpha birds", JsonDocument('{"tag":"sky","rank":3}'));
        )sql");

        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        std::vector<NYdb::NTable::TCopyItem> copies;
        copies.emplace_back("/Root/Source", "/Root/Copy");
        auto copy = tableSession.CopyTables(copies).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(copy.GetStatus(), EStatus::SUCCESS, copy.GetIssues().ToString());

        AssertSingleRowIdInfrastructure(kikimr, "/Root/Copy");

        CompareYson(
            SelectYson(db, R"sql(
                SELECT Key, Text, Payload, __ydb_row_id
                FROM `/Root/Source` ORDER BY Key;
            )sql"),
            SelectYson(db, R"sql(
                SELECT Key, Text, Payload, __ydb_row_id
                FROM `/Root/Copy` ORDER BY Key;
            )sql"));

        CompareYson(R"([["a"];["c"]])", SelectYson(db, R"sql(
            SELECT Key FROM `/Root/Copy` VIEW ft_plain
            WHERE FulltextMatch(Text, "alpha") ORDER BY Key;
        )sql"));
        CompareYson(R"([["a"];["c"]])", SelectYson(db, R"sql(
            SELECT Key FROM `/Root/Copy` VIEW ft_relevance
            WHERE FulltextScore(Text, "alpha") > 0 ORDER BY Key;
        )sql"));
        const TString expectedJson = R"([["a"];["b"]])";
        CompareYson(expectedJson, SelectYson(db, R"sql(
            SELECT Key FROM `/Root/Copy` VIEW json_idx
            WHERE JSON_VALUE(Payload, '$.tag' RETURNING Utf8) == "pet"u ORDER BY Key;
        )sql"));
        CompareYson(expectedJson, SelectYson(db, R"sql(
            SELECT Key FROM `/Root/Copy`
            WHERE JSON_VALUE(Payload, '$.tag' RETURNING Utf8) == "pet"u ORDER BY Key;
        )sql"));

        const auto before = ReadRowIds(db, "/Root/Copy");
        UNIT_ASSERT_VALUES_EQUAL(before.size(), 3u);

        Execute(db, R"sql(
            UPSERT INTO `/Root/Copy` (Key, Text, Payload) VALUES
                ("d"u, "alpha whales", JsonDocument('{"tag":"pet","rank":4}'));
            UPDATE `/Root/Copy`
                SET Text = "alpha dogs", Payload = JsonDocument('{"tag":"sky","rank":20}')
                WHERE Key = "b"u;
            DELETE FROM `/Root/Copy` WHERE Key = "c"u;
        )sql");

        const auto after = ReadRowIds(db, "/Root/Copy");
        UNIT_ASSERT_VALUES_EQUAL(after.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(after.at("a"), before.at("a"));
        UNIT_ASSERT_VALUES_EQUAL(after.at("b"), before.at("b"));
        UNIT_ASSERT_C(!after.contains("c"), "deleted row must not retain a row-id mapping");
        UNIT_ASSERT_C(after.contains("d"), "post-copy DML must allocate the next row id");
        UNIT_ASSERT_C(after.at("d") != 0, "new row id must be non-zero");
        UNIT_ASSERT_C(after.at("d") != before.at("a")
            && after.at("d") != before.at("b")
            && after.at("d") != before.at("c"),
            "copied sequence must allocate a fresh row id");

        const TString expectedFulltextAfter = R"([["a"];["b"];["d"]])";
        CompareYson(expectedFulltextAfter, SelectYson(db, R"sql(
            SELECT Key FROM `/Root/Copy` VIEW ft_plain
            WHERE FulltextMatch(Text, "alpha") ORDER BY Key;
        )sql"));
        CompareYson(expectedFulltextAfter, SelectYson(db, R"sql(
            SELECT Key FROM `/Root/Copy` VIEW ft_relevance
            WHERE FulltextScore(Text, "alpha") > 0 ORDER BY Key;
        )sql"));
        CompareYson(R"([["a"];["d"]])", SelectYson(db, R"sql(
            SELECT Key FROM `/Root/Copy` VIEW json_idx
            WHERE JSON_VALUE(Payload, '$.tag' RETURNING Utf8) == "pet"u ORDER BY Key;
        )sql"));
        CompareYson(R"([["a"];["d"]])", SelectYson(db, R"sql(
            SELECT Key FROM `/Root/Copy`
            WHERE JSON_VALUE(Payload, '$.tag' RETURNING Utf8) == "pet"u ORDER BY Key;
        )sql"));

        AssertSingleRowIdInfrastructure(kikimr, "/Root/Copy");
    }

    Y_UNIT_TEST(MoveCompactSearchIndexesPreservesSharedRowIdData) {
        auto kikimr = MakeCompactIndexCopyRunner();
        auto db = kikimr.GetQueryClient();

        Execute(db, R"sql(
            CREATE TABLE `/Root/BeforeMove` (
                Key Utf8 NOT NULL,
                Text String,
                Payload JsonDocument,
                PRIMARY KEY (Key),
                INDEX ft_plain GLOBAL USING fulltext_plain ON (Text)
                    WITH (tokenizer=standard, use_filter_lowercase=true),
                INDEX ft_relevance GLOBAL USING fulltext_relevance ON (Text)
                    WITH (tokenizer=standard, use_filter_lowercase=true),
                INDEX json_idx GLOBAL USING json ON (Payload)
            );
        )sql");
        Execute(db, R"sql(
            UPSERT INTO `/Root/BeforeMove` (Key, Text, Payload) VALUES
                ("a"u, "alpha cats",  JsonDocument('{"tag":"pet","rank":1}')),
                ("b"u, "beta dogs",   JsonDocument('{"tag":"pet","rank":2}')),
                ("c"u, "alpha birds", JsonDocument('{"tag":"sky","rank":3}'));
        )sql");

        const auto before = ReadRowIds(db, "/Root/BeforeMove");
        UNIT_ASSERT_VALUES_EQUAL(before.size(), 3u);
        AssertSingleRowIdInfrastructure(kikimr, "/Root/BeforeMove");

        Execute(db, R"sql(
            ALTER TABLE `/Root/BeforeMove` RENAME TO `/Root/AfterMove`;
        )sql");

        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto oldPath = tableSession.DescribeTable("/Root/BeforeMove").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(oldPath.GetStatus(), EStatus::SCHEME_ERROR,
            oldPath.GetIssues().ToString());
        AssertSingleRowIdInfrastructure(kikimr, "/Root/AfterMove");

        const auto moved = ReadRowIds(db, "/Root/AfterMove");
        UNIT_ASSERT_VALUES_EQUAL(moved, before);
        CompareYson(R"([["a"];["c"]])", SelectYson(db, R"sql(
            SELECT Key FROM `/Root/AfterMove` VIEW ft_plain
            WHERE FulltextMatch(Text, "alpha") ORDER BY Key;
        )sql"));
        CompareYson(R"([["a"];["c"]])", SelectYson(db, R"sql(
            SELECT Key FROM `/Root/AfterMove` VIEW ft_relevance
            WHERE FulltextScore(Text, "alpha") > 0 ORDER BY Key;
        )sql"));
        const TString expectedJson = R"([["a"];["b"]])";
        CompareYson(expectedJson, SelectYson(db, R"sql(
            SELECT Key FROM `/Root/AfterMove` VIEW json_idx
            WHERE JSON_VALUE(Payload, '$.tag' RETURNING Utf8) == "pet"u ORDER BY Key;
        )sql"));
        CompareYson(expectedJson, SelectYson(db, R"sql(
            SELECT Key FROM `/Root/AfterMove`
            WHERE JSON_VALUE(Payload, '$.tag' RETURNING Utf8) == "pet"u ORDER BY Key;
        )sql"));

        Execute(db, R"sql(
            UPSERT INTO `/Root/AfterMove` (Key, Text, Payload) VALUES
                ("d"u, "alpha whales", JsonDocument('{"tag":"pet","rank":4}'));
            UPDATE `/Root/AfterMove`
                SET Text = "alpha dogs", Payload = JsonDocument('{"tag":"sky","rank":20}')
                WHERE Key = "b"u;
            DELETE FROM `/Root/AfterMove` WHERE Key = "c"u;
        )sql");

        const auto after = ReadRowIds(db, "/Root/AfterMove");
        UNIT_ASSERT_VALUES_EQUAL(after.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(after.at("a"), before.at("a"));
        UNIT_ASSERT_VALUES_EQUAL(after.at("b"), before.at("b"));
        UNIT_ASSERT_C(!after.contains("c"), "deleted row must not retain a row-id mapping");
        UNIT_ASSERT_C(after.contains("d"), "moved sequence must allocate a row id");
        ui64 maxOldRowId = 0;
        for (const auto& [key, rowId] : before) {
            Y_UNUSED(key);
            if (rowId > maxOldRowId) {
                maxOldRowId = rowId;
            }
        }
        UNIT_ASSERT_C(after.at("d") > maxOldRowId,
            "moved sequence must continue after all allocated row ids");

        const TString expectedFulltextAfter = R"([["a"];["b"];["d"]])";
        CompareYson(expectedFulltextAfter, SelectYson(db, R"sql(
            SELECT Key FROM `/Root/AfterMove` VIEW ft_plain
            WHERE FulltextMatch(Text, "alpha") ORDER BY Key;
        )sql"));
        CompareYson(expectedFulltextAfter, SelectYson(db, R"sql(
            SELECT Key FROM `/Root/AfterMove` VIEW ft_relevance
            WHERE FulltextScore(Text, "alpha") > 0 ORDER BY Key;
        )sql"));
        CompareYson(R"([["a"];["d"]])", SelectYson(db, R"sql(
            SELECT Key FROM `/Root/AfterMove` VIEW json_idx
            WHERE JSON_VALUE(Payload, '$.tag' RETURNING Utf8) == "pet"u ORDER BY Key;
        )sql"));
        CompareYson(R"([["a"];["d"]])", SelectYson(db, R"sql(
            SELECT Key FROM `/Root/AfterMove`
            WHERE JSON_VALUE(Payload, '$.tag' RETURNING Utf8) == "pet"u ORDER BY Key;
        )sql"));
        AssertSingleRowIdInfrastructure(kikimr, "/Root/AfterMove");
    }
}

} // namespace NKikimr::NKqp
