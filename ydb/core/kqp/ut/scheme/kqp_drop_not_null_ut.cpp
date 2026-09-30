#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

TKikimrSettings IndexSettings(bool compact) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    auto& flags = *settings.AppConfig.MutableFeatureFlags();
    flags.SetEnableFulltextIndex(true);
    flags.SetEnableFulltextIndexPrefix(true);
    flags.SetEnableFulltextIndexRowId(true);
    flags.SetEnableJsonIndex(true);
    flags.SetEnableAddUniqueIndex(true);
    flags.SetEnableCompactFulltextIndex(compact);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    return settings;
}

void TestIndex(const TString& kind, bool compact = false) {
    TKikimrRunner kikimr(IndexSettings(compact));
    auto client = kikimr.GetQueryClient();
    auto execute = [&](const TString& query, bool ddl = false) {
        auto result = client.ExecuteQuery(query,
            ddl ? TTxControl::NoTx() : TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
        return result;
    };
    auto checkAsyncIndex = [&](const TString& column, const TString& expected) {
        if (kind != "async") {
            return;
        }
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        TString actual;
        do {
            auto result = client.ExecuteQuery("SELECT COUNT(*) FROM TestTable VIEW idx WHERE " + column + " IS NULL;",
                TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            actual = FormatResultSetYson(result.GetResultSet(0));
            if (actual == expected) {
                return;
            }
            Sleep(TDuration::MilliSeconds(100));
        } while (TInstant::Now() < deadline);
        CompareYson(expected, actual);
    };
    const bool json = kind == "json";
    const bool vector = kind == "vector_kmeans_tree";
    const bool fulltext = kind.StartsWith("fulltext");
    const bool prefixed = kind != "fulltext_relevance" || compact;
    const TString type = json ? "Json" : vector ? "String" : "Utf8";
    const TString value = json ? R"(Json('{"text":"cat"}'))"
        : vector ? R"(Untag(Knn::ToBinaryStringUint8(CAST([1, 2] AS List<Uint8>)), "Uint8Vector"))"
        : R"("cat"u)";
    const TString index = kind == "async" ? "GLOBAL ASYNC" : kind == "unique" ? "GLOBAL UNIQUE"
        : kind == "sync" ? "GLOBAL" : "GLOBAL USING " + kind;
    const TString options = fulltext ? " WITH (tokenizer=standard)"
        : vector ? " WITH (distance=cosine, vector_type=uint8, vector_dimension=2, clusters=2, levels=1)" : "";
    execute(TStringBuilder() << R"(
        CREATE TABLE TestTable (
            Key Uint64 NOT NULL, Prefix Uint64 NOT NULL,
            Value )" << type << R"( NOT NULL, Payload Int32 NOT NULL,
            PRIMARY KEY (Key), INDEX idx )" << index << (prefixed ? " ON (Prefix, Value)" : " ON (Value)")
            << (json ? "" : " COVER (Payload)") << options << ");", true);
    execute(TStringBuilder() << "UPSERT INTO TestTable (Key, Prefix, Value, Payload) VALUES (1, 10, " << value << ", 7);");

    for (const TString& column : {TString("Value"), TString("Prefix"), TString("Payload")}) {
        const TString nullValue = column == "Value" ? TString("NULL") : value;
        const TString prefix1 = column == "Prefix" ? "NULL" : "10";
        const TString prefix2 = column == "Prefix" ? "NULL" : "20";
        const TString payload = column == "Payload" ? "NULL" : "7";
        const TString write = TStringBuilder() << "UPSERT INTO TestTable (Key, Prefix, Value, Payload) VALUES "
            << "(1, " << prefix1 << ", " << nullValue << ", " << payload << "), "
            << "(2, " << prefix2 << ", " << nullValue << ", " << payload << ");";
        auto rejected = client.ExecuteQuery(write, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(!rejected.IsSuccess(), column << " accepted NULL before DROP NOT NULL");
        UNIT_ASSERT_STRING_CONTAINS(rejected.GetIssues().ToString(), "Failed to convert type");
        execute("ALTER TABLE TestTable ALTER COLUMN " + column + " DROP NOT NULL;", true);
        execute(write);
        auto result = execute("SELECT COUNT(*) FROM TestTable VIEW PRIMARY KEY WHERE " + column + " IS NULL;");
        CompareYson("[[2u]]", FormatResultSetYson(result.GetResultSet(0)));
        checkAsyncIndex(column, "[[2u]]");
    }
    if (fulltext || json) {
        auto result = execute(TStringBuilder() << "SELECT Key FROM TestTable VIEW idx WHERE "
            << (prefixed ? "Prefix = 10 AND " : "")
            << (json ? R"(JSON_VALUE(Value, '$.text' RETURNING Utf8) = "cat"u)" : R"(FulltextMatch(Value, "cat"))")
            << " ORDER BY Key;");
        CompareYson(prefixed ? "[[1u]]" : "[[1u];[2u]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    const TString nullKeyWrite = TStringBuilder()
        << "UPSERT INTO TestTable (Key, Prefix, Value, Payload) VALUES (NULL, 30, " << value << ", 7);";
    auto rejected = client.ExecuteQuery(nullKeyWrite, TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT(!rejected.IsSuccess());
    auto dropKey = client.ExecuteQuery("ALTER TABLE TestTable ALTER COLUMN Key DROP NOT NULL;",
        TTxControl::NoTx()).GetValueSync();
    if (compact && (fulltext || json)) {
        UNIT_ASSERT_C(!dropKey.IsSuccess(), "Compact document ids cannot be NULL");
        UNIT_ASSERT_STRING_CONTAINS(dropKey.GetIssues().ToString(), "requires a non-null document id");
        auto stillRejected = client.ExecuteQuery(nullKeyWrite, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(!stillRejected.IsSuccess());
    } else {
        UNIT_ASSERT_C(dropKey.IsSuccess(), dropKey.GetIssues().ToString());
        execute(nullKeyWrite);
        auto result = execute("SELECT COUNT(*) FROM TestTable VIEW PRIMARY KEY WHERE Key IS NULL;");
        CompareYson("[[1u]]", FormatResultSetYson(result.GetResultSet(0)));
        checkAsyncIndex("Key", "[[1u]]");
    }
}

void TestDocumentId(const TString& kind, bool compact) {
    TKikimrRunner kikimr(IndexSettings(compact));
    auto client = kikimr.GetQueryClient();
    auto execute = [&](const TString& query) {
        auto result = client.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
    };
    // Reuse an explicitly declared row-id column and its unique index.
    execute(R"(CREATE TABLE Docs (
        Key Utf8 NOT NULL, Text Utf8, JsonValue Json,
        __ydb_row_id Uint64 NOT NULL, PRIMARY KEY (Key),
        INDEX row_id GLOBAL UNIQUE ON (__ydb_row_id)
    );)");
    execute("ALTER TABLE Docs ADD INDEX idx GLOBAL USING " + kind
        + (kind == "json" ? " ON (JsonValue);" : " ON (Text) WITH (tokenizer=standard);"));
    auto rejected = client.ExecuteQuery("ALTER TABLE Docs ALTER COLUMN __ydb_row_id DROP NOT NULL;",
        TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(!rejected.IsSuccess(), "A document id must remain NOT NULL");
    UNIT_ASSERT_STRING_CONTAINS(rejected.GetIssues().ToString(), "requires a non-null document id");
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    for (const TString& path : {TString("/Root/Docs"), TString("/Root/Docs/row_id/indexImplTable")}) {
        auto describe = session.DescribeTable(path).GetValueSync();
        UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
        bool found = false;
        for (const auto& column : describe.GetTableDescription().GetTableColumns()) {
            if (column.Name == "__ydb_row_id") {
                UNIT_ASSERT_VALUES_EQUAL_C(column.Type.ToString(), "Uint64", path);
                found = true;
            }
        }
        UNIT_ASSERT(found);
    }

    // With a separate document id, the primary key itself may become nullable.
    const TString nullKeyWrite = R"(UPSERT INTO Docs (Key, Text, JsonValue)
        VALUES (NULL, "cat"u, Json('{"text":"cat"}'));)";
    auto nullKeyRejected = client.ExecuteQuery(nullKeyWrite, TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT(!nullKeyRejected.IsSuccess());
    UNIT_ASSERT_STRING_CONTAINS(nullKeyRejected.GetIssues().ToString(), "Failed to convert type");
    execute("ALTER TABLE Docs ALTER COLUMN Key DROP NOT NULL;");
    auto nullKeyAccepted = client.ExecuteQuery(nullKeyWrite, TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(nullKeyAccepted.IsSuccess(), nullKeyAccepted.GetIssues().ToString());
    auto read = client.ExecuteQuery("SELECT COUNT(*) FROM Docs VIEW PRIMARY KEY WHERE Key IS NULL;",
        TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
    CompareYson("[[1u]]", FormatResultSetYson(read.GetResultSet(0)));
}

} // namespace

Y_UNIT_TEST_SUITE(KqpDropNotNull) {
    Y_UNIT_TEST(LocalRowBloomIndex) {
        TKikimrRunner kikimr(IndexSettings(false));
        auto client = kikimr.GetQueryClient();
        auto execute = [&](const TString& query, bool ddl = false) {
            auto result = client.ExecuteQuery(query,
                ddl ? TTxControl::NoTx() : TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result;
        };
        execute(R"(CREATE TABLE LocalTable (
            Key Uint64 NOT NULL, Value Utf8, PRIMARY KEY (Key),
            INDEX bloom LOCAL USING bloom_filter ON (Key)
        );)", true);
        const TString write = R"(UPSERT INTO LocalTable (Key, Value) VALUES (NULL, "cat"u);)";
        auto rejected = client.ExecuteQuery(write, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(!rejected.IsSuccess());
        execute("ALTER TABLE LocalTable ALTER COLUMN Key DROP NOT NULL;", true);
        execute(write);
        auto result = execute("SELECT Key FROM LocalTable WHERE Key IS NULL;");
        CompareYson("[[#]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(LocalColumnIndexes) {
        auto settings = IndexSettings(false).SetColumnShardAlterObjectEnabled(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalMinMaxIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalBloomNgramFilterIndex(true);
        settings.AppConfig.MutableFeatureFlags()->SetEnableLocalIndexAsSchemeObject(true);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        auto execute = [&](const TString& query) {
            auto result = client.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
            return result;
        };
        execute(R"(CREATE TABLE LocalTable (
            Key Uint64 NOT NULL, Value Utf8 NOT NULL, PRIMARY KEY (Key),
            INDEX mm LOCAL USING min_max ON (Value),
            INDEX bloom LOCAL USING bloom_filter ON (Value),
            INDEX ngram LOCAL USING bloom_ngram_filter ON (Value)
                WITH (ngram_size=3, false_positive_probability=0.01, case_sensitive=false)
        ) PARTITION BY HASH(Key) WITH (STORE=COLUMN, PARTITION_COUNT=1);)");
        execute(R"(ALTER OBJECT `/Root/LocalTable` (TYPE TABLE) SET
            (ACTION=UPSERT_INDEX, NAME=cms, TYPE=COUNT_MIN_SKETCH,
             FEATURES=`{"column_names":["Value"]}`);)");
        const TString write = "UPSERT INTO LocalTable (Key, Value) VALUES (1, NULL);";
        auto rejected = client.ExecuteQuery(write, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!rejected.IsSuccess());
        execute("ALTER TABLE LocalTable ALTER COLUMN Value DROP NOT NULL;");
        execute(write);
        auto result = execute("SELECT Value FROM LocalTable WHERE Key=1;");
        CompareYson("[[#]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(Sync) { TestIndex("sync"); }
    Y_UNIT_TEST(Async) { TestIndex("async"); }
    Y_UNIT_TEST(Unique) { TestIndex("unique"); }
    Y_UNIT_TEST(Vector) { TestIndex("vector_kmeans_tree"); }
    Y_UNIT_TEST_TWIN(FulltextPlain, Compact) { TestIndex("fulltext_plain", Compact); }
    Y_UNIT_TEST_TWIN(FulltextRelevance, Compact) { TestIndex("fulltext_relevance", Compact); }
    Y_UNIT_TEST_TWIN(Json, Compact) { TestIndex("json", Compact); }
    Y_UNIT_TEST_TWIN(FulltextPlainDocumentId, Compact) { TestDocumentId("fulltext_plain", Compact); }
    Y_UNIT_TEST_TWIN(FulltextRelevanceDocumentId, Compact) { TestDocumentId("fulltext_relevance", Compact); }
    Y_UNIT_TEST_TWIN(JsonDocumentId, Compact) { TestDocumentId("json", Compact); }
}

} // namespace NKikimr::NKqp
