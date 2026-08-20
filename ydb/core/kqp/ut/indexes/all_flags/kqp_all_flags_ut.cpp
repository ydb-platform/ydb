#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NTable;

namespace {

enum class EDisabledFeature {
    None,
    FulltextPrefix,
    FulltextRowId,
    CompactFulltext,
    JsonIndex,
    JsonIndexAutoSelect,
    AddUniqueIndex,
    HybridSearch,
};

TKikimrRunner KikimrWithAllExperimentalIndexes(EDisabledFeature disabled = EDisabledFeature::None) {
    NKikimrConfig::TFeatureFlags featureFlags;
    // Keep this list in sync with the documented all-flags deployment configuration.
    featureFlags.SetEnableFulltextIndexPrefix(disabled != EDisabledFeature::FulltextPrefix);
    featureFlags.SetEnableFulltextIndexRowId(disabled != EDisabledFeature::FulltextRowId);
    featureFlags.SetEnableCompactFulltextIndex(disabled != EDisabledFeature::CompactFulltext);
    featureFlags.SetEnableJsonIndex(disabled != EDisabledFeature::JsonIndex);
    featureFlags.SetEnableJsonIndexAutoSelect(disabled != EDisabledFeature::JsonIndexAutoSelect);
    featureFlags.SetEnableAddUniqueIndex(disabled != EDisabledFeature::AddUniqueIndex);

    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableHybridSearch(
        disabled != EDisabledFeature::HybridSearch);

    // Compact indexes are maintained through stream writes. BackportMode=All makes the test cluster
    // expose the same write path as a current production configuration; neither is a feature under test.
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(
        NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    return TKikimrRunner(settings);
}

void Execute(TQueryClient& db, const TString& query) {
    auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

TString SelectYson(TQueryClient& db, const TString& query) {
    auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
    return FormatResultSetYson(result.GetResultSet(0));
}

TString ExecuteFail(TQueryClient& db, const TString& query) {
    auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(!result.IsSuccess(), "Query unexpectedly succeeded: " << query);
    return result.GetIssues().ToString();
}

void AssertFailsWith(TQueryClient& db, const TString& query, const TString& expectedIssue) {
    UNIT_ASSERT_STRING_CONTAINS(ExecuteFail(db, query), expectedIssue);
}

void AssertIndexTypes(TKikimrRunner& kikimr, const TString& table,
        const THashMap<TString, EIndexType>& expected) {
    auto tableClient = kikimr.GetTableClient();
    auto createSession = tableClient.CreateSession().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(createSession.GetStatus(), EStatus::SUCCESS, createSession.GetIssues().ToString());
    auto describe = createSession.GetSession().DescribeTable(table).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, describe.GetIssues().ToString());

    THashMap<TString, EIndexType> actual;
    for (const auto& index : describe.GetTableDescription().GetIndexDescriptions()) {
        actual[TString{index.GetIndexName()}] = index.GetIndexType();
    }
    for (const auto& [name, type] : expected) {
        UNIT_ASSERT_C(actual.contains(name), "Missing index " << name);
        UNIT_ASSERT_VALUES_EQUAL_C(actual.at(name), type, "Unexpected type for index " << name);
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpAllExperimentalIndexes) {

Y_UNIT_TEST(FeatureGateFulltextPrefix) {
    auto kikimr = KikimrWithAllExperimentalIndexes(EDisabledFeature::FulltextPrefix);
    auto db = kikimr.GetQueryClient();

    Execute(db, R"sql(
        CREATE TABLE `/Root/Docs` (
            Key Uint64, Tenant Utf8, Text Utf8, PRIMARY KEY (Key)
        );
    )sql");
    AssertFailsWith(db, R"sql(
        ALTER TABLE `/Root/Docs` ADD INDEX ft_idx
            GLOBAL USING fulltext_plain ON (Tenant, Text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql", "Prefixed fulltext/json index support is disabled");
}

Y_UNIT_TEST(FeatureGateFulltextRowId) {
    auto kikimr = KikimrWithAllExperimentalIndexes(EDisabledFeature::FulltextRowId);
    auto db = kikimr.GetQueryClient();

    Execute(db, R"sql(
        CREATE TABLE `/Root/Docs` (
            Key Utf8 NOT NULL, Text Utf8, PRIMARY KEY (Key)
        );
    )sql");
    AssertFailsWith(db, R"sql(
        ALTER TABLE `/Root/Docs` ADD INDEX ft_idx
            GLOBAL USING fulltext_plain ON (Text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql", "requires the __ydb_row_id doc_id feature, which is disabled");
}

Y_UNIT_TEST(FeatureGateCompactFulltextFallsBackToLegacy) {
    auto kikimr = KikimrWithAllExperimentalIndexes(EDisabledFeature::CompactFulltext);
    auto db = kikimr.GetQueryClient();

    Execute(db, R"sql(
        CREATE TABLE `/Root/Docs` (
            Key Uint64, Text Utf8, PRIMARY KEY (Key)
        );
    )sql");
    Execute(db, R"sql(
        UPSERT INTO `/Root/Docs` (Key, Text) VALUES (1, "cats play"u);
    )sql");
    Execute(db, R"sql(
        ALTER TABLE `/Root/Docs` ADD INDEX ft_idx
            GLOBAL USING fulltext_plain ON (Text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql");
    CompareYson("[[[1u]]]", SelectYson(db, R"sql(
        SELECT Key FROM `/Root/Docs` VIEW ft_idx WHERE FulltextMatch(Text, "cats");
    )sql"));

    // __ydb_generation is part of the compact posting key and does not exist in the legacy layout.
    AssertFailsWith(db, R"sql(
        SELECT __ydb_generation FROM `/Root/Docs/ft_idx/indexImplTable`;
    )sql", "__ydb_generation");
}

Y_UNIT_TEST(FeatureGateJsonIndex) {
    auto kikimr = KikimrWithAllExperimentalIndexes(EDisabledFeature::JsonIndex);
    auto db = kikimr.GetQueryClient();

    Execute(db, R"sql(
        CREATE TABLE `/Root/Docs` (
            Key Uint64, Payload JsonDocument, PRIMARY KEY (Key)
        );
    )sql");
    AssertFailsWith(db, R"sql(
        ALTER TABLE `/Root/Docs` ADD INDEX json_idx GLOBAL USING json ON (Payload);
    )sql", "JSON index support is disabled");
}

Y_UNIT_TEST(FeatureGateJsonIndexAutoSelect) {
    auto kikimr = KikimrWithAllExperimentalIndexes(EDisabledFeature::JsonIndexAutoSelect);
    auto db = kikimr.GetQueryClient();

    Execute(db, R"sql(
        CREATE TABLE `/Root/Docs` (
            Key Uint64, Payload JsonDocument, PRIMARY KEY (Key)
        );
    )sql");
    Execute(db, R"sql(
        UPSERT INTO `/Root/Docs` (Key, Payload) VALUES
            (1, JsonDocument('{"kind":"animal"}')),
            (2, JsonDocument('{"kind":"plant"}'));
    )sql");
    Execute(db, R"sql(
        ALTER TABLE `/Root/Docs` ADD INDEX json_idx GLOBAL USING json ON (Payload);
    )sql");

    const TString query = R"sql(
        SELECT Key FROM `/Root/Docs`
        WHERE JSON_VALUE(Payload, '$.kind' RETURNING Utf8) = "animal"
        ORDER BY Key;
    )sql";
    auto explain = db.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
        TExecuteQuerySettings().ExecMode(EExecMode::Explain)).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(explain.GetStatus(), EStatus::SUCCESS, explain.GetIssues().ToString());
    UNIT_ASSERT_C(explain.GetStats() && explain.GetStats()->GetPlan(), "EXPLAIN returned no plan");
    UNIT_ASSERT_C(explain.GetStats()->GetPlan()->find("json_idx") == std::string::npos,
        "JSON index was selected while EnableJsonIndexAutoSelect=false: "
            << *explain.GetStats()->GetPlan());

    // The optimizer switch must not disable the index itself: explicit VIEW remains usable.
    CompareYson("[[[1u]]]", SelectYson(db, R"sql(
        SELECT Key FROM `/Root/Docs` VIEW json_idx
        WHERE JSON_VALUE(Payload, '$.kind' RETURNING Utf8) = "animal";
    )sql"));
}

Y_UNIT_TEST(FeatureGateAddUniqueIndex) {
    auto kikimr = KikimrWithAllExperimentalIndexes(EDisabledFeature::AddUniqueIndex);
    auto db = kikimr.GetQueryClient();

    Execute(db, R"sql(
        CREATE TABLE `/Root/Docs` (
            Key Uint64, ExternalId Utf8, PRIMARY KEY (Key)
        );
    )sql");
    AssertFailsWith(db, R"sql(
        ALTER TABLE `/Root/Docs` ADD INDEX external_id_uidx
            GLOBAL UNIQUE ON (ExternalId);
    )sql", "Adding a unique index to an existing table is disabled");
}

Y_UNIT_TEST(FeatureGateHybridSearch) {
    auto kikimr = KikimrWithAllExperimentalIndexes(EDisabledFeature::HybridSearch);
    auto db = kikimr.GetQueryClient();

    Execute(db, R"sql(
        CREATE TABLE `/Root/Docs` (
            Key Uint64, Text Utf8, Embedding String, PRIMARY KEY (Key)
        );
    )sql");
    AssertFailsWith(db, R"sql(
        $target = Untag(Knn::ToBinaryStringUint8(Cast([1, 2] AS List<Uint8>)), "Uint8Vector");
        SELECT Key FROM `/Root/Docs`
        ORDER BY HybridRank(FullTextScore(Text, "cats"),
            Knn::CosineDistance(Embedding, $target))
        LIMIT 1;
    )sql", "hybrid search is disabled");
}

Y_UNIT_TEST(CompactFulltextJsonAndUniqueCoexist) {
    auto kikimr = KikimrWithAllExperimentalIndexes();
    auto db = kikimr.GetQueryClient();

    Execute(db, R"sql(
        CREATE TABLE `/Root/Docs` (
            Key Uint64,
            Text Utf8,
            Payload JsonDocument,
            ExternalId Utf8,
            PRIMARY KEY (Key)
        );
    )sql");
    Execute(db, R"sql(
        UPSERT INTO `/Root/Docs` (Key, Text, Payload, ExternalId) VALUES
            (1, "cats love milk"u, JsonDocument('{"kind":"animal","active":true}'), "one"u),
            (2, "dogs chase cats"u, JsonDocument('{"kind":"animal"}'), "two"u),
            (3, "birds can fly"u, JsonDocument('{"kind":"bird"}'), "three"u);
    )sql");
    Execute(db, R"sql(
        ALTER TABLE `/Root/Docs` ADD INDEX ft_plain
            GLOBAL USING fulltext_plain ON (Text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql");
    Execute(db, R"sql(
        ALTER TABLE `/Root/Docs` ADD INDEX ft_relevance
            GLOBAL USING fulltext_relevance ON (Text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql");
    Execute(db, R"sql(
        ALTER TABLE `/Root/Docs` ADD INDEX json_idx GLOBAL USING json ON (Payload);
    )sql");
    Execute(db, R"sql(
        ALTER TABLE `/Root/Docs` ADD INDEX external_id_uidx GLOBAL UNIQUE ON (ExternalId);
    )sql");

    // The public schema maps compact layouts to their logical index kinds. Successful reads from all
    // compact implementation shapes below additionally verify that the exact flag set is usable together.
    AssertIndexTypes(kikimr, "/Root/Docs", {
        {"ft_plain", EIndexType::GlobalFulltextPlain},
        {"ft_relevance", EIndexType::GlobalFulltextRelevance},
        {"json_idx", EIndexType::GlobalJson},
        {"external_id_uidx", EIndexType::GlobalUnique},
    });

    CompareYson("[[[1u]];[[2u]]]", SelectYson(db, R"sql(
        SELECT Key FROM `/Root/Docs` VIEW ft_plain
        WHERE FulltextMatch(Text, "cats") ORDER BY Key;
    )sql"));
    CompareYson("[[[1u]];[[2u]]]", SelectYson(db, R"sql(
        SELECT Key FROM `/Root/Docs` VIEW ft_relevance
        WHERE FulltextMatch(Text, "cats") ORDER BY Key;
    )sql"));
    CompareYson("[[[1u]];[[2u]]]", SelectYson(db, R"sql(
        SELECT Key FROM `/Root/Docs`
        WHERE JSON_VALUE(Payload, '$.kind' RETURNING Utf8) = "animal" ORDER BY Key;
    )sql"));

    // Smoke online maintenance for all three compact indexes in a single statement.
    Execute(db, R"sql(
        UPSERT INTO `/Root/Docs` (Key, Text, Payload, ExternalId) VALUES
            (2, "dogs only"u, JsonDocument('{"kind":"canine"}'), "two"u),
            (4, "cats sleep"u, JsonDocument('{"kind":"animal"}'), "four"u);
        DELETE FROM `/Root/Docs` WHERE Key = 1;
    )sql");
    CompareYson("[[[4u]]]", SelectYson(db, R"sql(
        SELECT Key FROM `/Root/Docs` VIEW ft_plain
        WHERE FulltextMatch(Text, "cats") ORDER BY Key;
    )sql"));
    CompareYson("[[[4u]]]", SelectYson(db, R"sql(
        SELECT Key FROM `/Root/Docs`
        WHERE JSON_VALUE(Payload, '$.kind' RETURNING Utf8) = "animal" ORDER BY Key;
    )sql"));

    auto duplicate = db.ExecuteQuery(R"sql(
        INSERT INTO `/Root/Docs` (Key, Text, Payload, ExternalId) VALUES
            (5, "duplicate"u, JsonDocument('{}'), "two"u);
    )sql", NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(!duplicate.IsSuccess(), "Unique index accepted a duplicate key");
    CompareYson("[[0u]]", SelectYson(db, "SELECT COUNT(*) FROM `/Root/Docs` WHERE Key = 5;"));
}

Y_UNIT_TEST(PrefixedCompactIndexesReuseAutoRowId) {
    auto kikimr = KikimrWithAllExperimentalIndexes();
    auto db = kikimr.GetQueryClient();

    Execute(db, R"sql(
        CREATE TABLE `/Root/TenantDocs` (
            Org Utf8 NOT NULL,
            Pk Utf8 NOT NULL,
            Tenant Utf8,
            Text Utf8,
            Payload Json,
            PRIMARY KEY (Org, Pk)
        );
    )sql");
    Execute(db, R"sql(
        UPSERT INTO `/Root/TenantDocs` (Org, Pk, Tenant, Text, Payload) VALUES
            ("acme"u, "a"u, "red"u,  "cats play"u, Json('{"tag":"pet"}')),
            ("acme"u, "b"u, "blue"u, "cats sleep"u, Json('{"tag":"pet"}')),
            ("globex"u, "c"u, "red"u, "dogs run"u, Json('{"tag":"work"}'));
    )sql");
    Execute(db, R"sql(
        ALTER TABLE `/Root/TenantDocs` ADD INDEX ft_by_tenant
            GLOBAL USING fulltext_plain ON (Tenant, Text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql");
    Execute(db, R"sql(
        ALTER TABLE `/Root/TenantDocs` ADD INDEX json_by_tenant
            GLOBAL USING json ON (Tenant, Payload);
    )sql");

    AssertIndexTypes(kikimr, "/Root/TenantDocs", {
        {"__ydb_unique_row_id", EIndexType::GlobalUnique},
        {"ft_by_tenant", EIndexType::GlobalFulltextPlain},
        {"json_by_tenant", EIndexType::GlobalJson},
    });
    CompareYson("[[\"a\"]]", SelectYson(db, R"sql(
        SELECT Pk FROM `/Root/TenantDocs` VIEW ft_by_tenant
        WHERE Tenant = "red" AND FulltextMatch(Text, "cats") ORDER BY Pk;
    )sql"));
    CompareYson("[[\"a\"]]", SelectYson(db, R"sql(
        SELECT Pk FROM `/Root/TenantDocs` VIEW json_by_tenant
        WHERE Tenant = "red" AND JSON_VALUE(Payload, '$.tag' RETURNING Utf8) = "pet" ORDER BY Pk;
    )sql"));

    auto rowIds = db.ExecuteQuery(R"sql(
        SELECT __ydb_row_id FROM `/Root/TenantDocs` ORDER BY Org, Pk;
    )sql", NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(rowIds.GetStatus(), EStatus::SUCCESS, rowIds.GetIssues().ToString());
    THashSet<ui64> seen;
    TResultSetParser parser(rowIds.GetResultSet(0));
    while (parser.TryNextRow()) {
        const ui64 rowId = parser.ColumnParser("__ydb_row_id").GetUint64();
        UNIT_ASSERT_C(rowId != 0, "Auto-generated __ydb_row_id must be non-zero");
        UNIT_ASSERT_C(seen.insert(rowId).second, "Auto-generated __ydb_row_id must be unique");
    }
    UNIT_ASSERT_VALUES_EQUAL(seen.size(), 3);

    Execute(db, R"sql(
        UPDATE `/Root/TenantDocs` SET Tenant = "blue"u, Text = "birds fly"u,
            Payload = Json('{"tag":"sky"}')
        WHERE Org = "acme"u AND Pk = "a"u;
        DELETE FROM `/Root/TenantDocs` WHERE Org = "globex"u AND Pk = "c"u;
    )sql");
    CompareYson("[]", SelectYson(db, R"sql(
        SELECT Pk FROM `/Root/TenantDocs` VIEW ft_by_tenant
        WHERE Tenant = "red" AND FulltextMatch(Text, "cats") ORDER BY Pk;
    )sql"));
    CompareYson("[[\"a\"]]", SelectYson(db, R"sql(
        SELECT Pk FROM `/Root/TenantDocs` VIEW json_by_tenant
        WHERE Tenant = "blue" AND JSON_VALUE(Payload, '$.tag' RETURNING Utf8) = "sky" ORDER BY Pk;
    )sql"));
}

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr::NKqp
