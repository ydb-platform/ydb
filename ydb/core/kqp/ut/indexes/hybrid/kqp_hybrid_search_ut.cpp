#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/json/json_reader.h>

#include <array>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

TKikimrRunner MakeRunner(bool enableHybridSearch = true, bool enableVectorSearchActor = true) {
    // Fix the kmeans-tree build sampling seed so the index tree is reproducible run-to-run (otherwise it
    // seeds from the tablet id). Combined with the exhaustive search probe in TargetDecl below, this makes
    // the vector branch fully deterministic. See gVectorIndexSeed in schemeshard_impl.h (tests only).
    NSchemeShard::gVectorIndexSeed = 1337;

    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    // EnableHybridSearch is on by default; the explicit set both documents the dependency and lets
    // DisabledByFlag exercise the off path.
    settings.AppConfig.MutableTableServiceConfig()->SetEnableHybridSearch(enableHybridSearch);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableVectorSearchActor(enableVectorSearchActor);
    return TKikimrRunner(settings);
}

TKikimrRunner MakeRunnerWithCompact(bool compact, bool enableFulltextPrefix = false) {
    NSchemeShard::gVectorIndexSeed = 1337;
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    featureFlags.SetEnableCompactFulltextIndex(compact);
    featureFlags.SetEnableFulltextIndexPrefix(enableFulltextPrefix);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableHybridSearch(true);
    return TKikimrRunner(settings);
}

void ExecOk(TQueryClient& db, const TString& sql) {
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

// 2D uint8 vectors packed into the Knn binary format. Target is [250,10]; distances to target rank:
//   Doc2 (exact) < Doc1 (near) < Doc4 (mid) < Doc3 (opposite).
// Fulltext "cats" matches only Doc1 ("cats" x3) and Doc3 ("cats" x1); Doc2/Doc4 are absent from the
// fulltext branch and so get the penalty rank there. The fusion therefore puts the text-relevant docs
// {1,3} above the text-irrelevant {2,4} — even though Doc2 is the nearest vector match — which is the
// whole point. (The order *within* each group depends on the approximate k-means ranking and is not
// asserted; see FusesBothBranches.)
const char* Vec(int idx) {
    switch (idx) {
        case 1: return "[240, 15]";
        case 2: return "[250, 10]";
        case 3: return "[10, 250]";
        case 4: return "[200, 60]";
    }
    return "[0, 0]";
}

TString Emb(int idx) {
    return Sprintf(R"(Untag(Knn::ToBinaryStringUint8(Cast(%s AS List<Uint8>)), "Uint8Vector"))", Vec(idx));
}

TString Emb(ui32 x, ui32 y) {
    return Sprintf(R"(Untag(Knn::ToBinaryStringUint8(Cast([%u, %u] AS List<Uint8>)), "Uint8Vector"))", x, y);
}

void CreateDocs(TQueryClient& db, const TString& table = "/Root/Docs", bool categoryNotNull = true) {
    ExecOk(db, Sprintf(R"sql(
        CREATE TABLE `%s` (
            Key Uint64,
            Text Utf8,
            Embedding String,
            Category Utf8 %s,
            PRIMARY KEY (Key)
        );
    )sql", table.c_str(), categoryNotNull ? "NOT NULL" : ""));
}

void UpsertDocs(TQueryClient& db, const TString& table = "/Root/Docs") {
    ExecOk(db, Sprintf(R"sql(
        UPSERT INTO `%s` (Key, Text, Embedding, Category) VALUES
            (1u, "cats cats cats love", %s, "a"),
            (2u, "dogs and foxes run",  %s, "a"),
            (3u, "cats sleep",          %s, "b"),
            (4u, "birds fly high",      %s, "b");
    )sql", table.c_str(), Emb(1).c_str(), Emb(2).c_str(), Emb(3).c_str(), Emb(4).c_str()));
}

void AddFulltextIndex(TQueryClient& db, const TString& table = "/Root/Docs", const TString& name = "ft_idx") {
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `%s` ADD INDEX %s
            GLOBAL USING fulltext_relevance
            ON (Text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql", table.c_str(), name.c_str()));
}

void AddPrefixedFulltextIndex(TQueryClient& db, const TString& table = "/Root/Docs", const TString& name = "ft_idx") {
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `%s` ADD INDEX %s
            GLOBAL USING fulltext_relevance
            ON (Category, Text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql", table.c_str(), name.c_str()));
}

void AddVectorIndex(TQueryClient& db, const TString& table = "/Root/Docs", const TString& name = "vec_idx",
    const TString& metric = "distance=cosine")
{
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `%s` ADD INDEX %s
            GLOBAL USING vector_kmeans_tree
            ON (Embedding)
            WITH (%s, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
    )sql", table.c_str(), name.c_str(), metric.c_str()));
}

// A prefixed vector index (a prefix column before the vector column).
void AddPrefixedVectorIndex(TQueryClient& db, const TString& table = "/Root/Docs", const TString& name = "vp_idx") {
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `%s` ADD INDEX %s
            GLOBAL USING vector_kmeans_tree
            ON (Category, Embedding)
            WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
    )sql", table.c_str(), name.c_str()));
}

void CreateMultiPrefixDocs(TQueryClient& db) {
    ExecOk(db, R"sql(
        CREATE TABLE `/Root/MultiDocs` (
            Key Uint64,
            Region Utf8 NOT NULL,
            Category Utf8 NOT NULL,
            Text Utf8,
            Embedding String,
            PRIMARY KEY (Key)
        );
    )sql");
    ExecOk(db, Sprintf(R"sql(
        UPSERT INTO `/Root/MultiDocs` (Key, Region, Category, Text, Embedding) VALUES
            (1u, "r1", "a", "cats cats cats love", %s),
            (2u, "r1", "a", "dogs and foxes run",  %s),
            (3u, "r2", "a", "cats sleep",          %s),
            (4u, "r1", "b", "birds fly high",      %s);
    )sql", Emb(1).c_str(), Emb(2).c_str(), Emb(3).c_str(), Emb(4).c_str()));
}

void AddMultiPrefixedFulltextIndex(TQueryClient& db, const TString& name = "ft_multi") {
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `/Root/MultiDocs` ADD INDEX %s
            GLOBAL USING fulltext_relevance
            ON (Region, Category, Text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql", name.c_str()));
}

void AddMultiPrefixedVectorIndex(TQueryClient& db, const TString& name = "vec_multi") {
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `/Root/MultiDocs` ADD INDEX %s
            GLOBAL USING vector_kmeans_tree
            ON (Region, Category, Embedding)
            WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
    )sql", name.c_str()));
}

void SetupUserPkPrefixedDocs(TQueryClient& db) {
    ExecOk(db, R"sql(
        CREATE TABLE `/Root/UserDocs` (
            pk Uint64,
            user Utf8 NOT NULL,
            text Utf8,
            embedding String,
            PRIMARY KEY (pk)
        );
    )sql");
    ExecOk(db, Sprintf(R"sql(
        UPSERT INTO `/Root/UserDocs` (pk, user, text, embedding) VALUES
            (1u, "alice", "cats cats cats love", %s),
            (2u, "alice", "dogs and foxes run",  %s),
            (3u, "bob",   "cats sleep",          %s),
            (4u, "bob",   "birds fly high",      %s);
    )sql", Emb(1).c_str(), Emb(2).c_str(), Emb(3).c_str(), Emb(4).c_str()));
    ExecOk(db, R"sql(
        ALTER TABLE `/Root/UserDocs` ADD INDEX ft_idx
            GLOBAL USING fulltext_relevance
            ON (text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql");
    ExecOk(db, R"sql(
        ALTER TABLE `/Root/UserDocs` ADD INDEX vec_idx
            GLOBAL USING vector_kmeans_tree
            ON (user, pk, embedding)
            WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
    )sql");
}

// The standard fixture used by most tests: 4 docs with a fulltext and a (non-prefixed) vector index.
void SetupDocs(TQueryClient& db) {
    CreateDocs(db);
    UpsertDocs(db);
    AddFulltextIndex(db);
    AddVectorIndex(db);
}

void SetupLargeDocs(TQueryClient& db, ui32 count) {
    ExecOk(db, R"sql(
        CREATE TABLE `/Root/LargeDocs` (
            Key Uint64,
            Text Utf8,
            Embedding String,
            PRIMARY KEY (Key)
        );
    )sql");

    TStringBuilder upsert;
    upsert << "UPSERT INTO `/Root/LargeDocs` (Key, Text, Embedding) VALUES\n";
    for (ui32 key = 1; key <= count; ++key) {
        if (key != 1) {
            upsert << ",\n";
        }
        const char* text = key % 5 == 0 ? "needle needle" : "haystack";
        upsert << "(" << key << "u, \"" << text << "\", "
            << Emb((key * 37) % 256, (key * 53) % 256) << ")";
    }
    upsert << ";";
    ExecOk(db, upsert);
    AddFulltextIndex(db, "/Root/LargeDocs");
    AddVectorIndex(db, "/Root/LargeDocs");
}

void SetupTiedScoreDocs(TQueryClient& db) {
    ExecOk(db, R"sql(
        CREATE TABLE `/Root/TiedDocs` (
            Key Uint64,
            Text Utf8,
            Embedding String,
            PRIMARY KEY (Key)
        );
    )sql");
    ExecOk(db, Sprintf(R"sql(
        UPSERT INTO `/Root/TiedDocs` (Key, Text, Embedding) VALUES
            (1u, "same", %s),
            (2u, "same", %s),
            (3u, "same", %s),
            (4u, "same", %s);
    )sql", Emb(127, 128).c_str(), Emb(129, 128).c_str(), Emb(128, 127).c_str(), Emb(128, 129).c_str()));
    AddFulltextIndex(db, "/Root/TiedDocs");
    AddVectorIndex(db, "/Root/TiedDocs", "vec_idx", "distance=euclidean");
}

struct THybridPrefixMatrixShape {
    bool MultiPrefix;
    bool NullablePrefix;
    bool PkSuffix;
};

TString HybridPrefixMatrixTable(const THybridPrefixMatrixShape& shape) {
    return Sprintf("/Root/HybridPrefixMatrix_M%d_N%d_P%d",
        shape.MultiPrefix, shape.NullablePrefix, shape.PkSuffix);
}

void SetupHybridPrefixMatrixFixture(TQueryClient& db, const THybridPrefixMatrixShape& shape) {
    const TString table = HybridPrefixMatrixTable(shape);
    const char* nullability = shape.NullablePrefix ? "" : "NOT NULL";
    ExecOk(db, Sprintf(R"sql(
        CREATE TABLE `%s` (
            Key Uint64,
            Tenant Utf8 %s,
            Region Utf8 %s,
            Text Utf8,
            Embedding String,
            PRIMARY KEY (Key)
        );
    )sql", table.c_str(), nullability, nullability));
    ExecOk(db, Sprintf(R"sql(
        UPSERT INTO `%s` (Key, Tenant, Region, Text, Embedding) VALUES
            (1u, "a", "r1", "cats cats cats love", %s),
            (2u, "a", "r1", "dogs and foxes run",  %s),
            (3u, "b", "r1", "cats sleep",          %s),
            (4u, "b", "r2", "birds fly high",      %s);
    )sql", table.c_str(), Emb(1).c_str(), Emb(2).c_str(), Emb(3).c_str(), Emb(4).c_str()));
    AddFulltextIndex(db, table);

    TString indexColumns = "Tenant";
    if (shape.MultiPrefix) {
        indexColumns += ", Region";
    }
    if (shape.PkSuffix) {
        indexColumns += ", Key";
    }
    indexColumns += ", Embedding";
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `%s` ADD INDEX vec_idx
            GLOBAL USING vector_kmeans_tree
            ON (%s)
            WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
    )sql", table.c_str(), indexColumns.c_str()));
}

// The kmeans-tree search-probe pragma. Widens the probe to cover all clusters at every level
// (clusters=2, levels=2 => up to 4 leaf clusters) so the 4-doc vector branch is exhaustive: it returns
// all candidates ordered by their true distance, deterministically, instead of an approximate subset that
// can vary run-to-run.
//
// This MUST be >= the number of leaf clusters (4 here), not just the per-level cluster count (2). At "2"
// the probe prunes the far branch of the tree and drops the opposite-direction doc (doc3, vector
// [10,250]), so the vector branch returns only {2,1,4}. The fusion then ranks doc3 below the
// text-irrelevant doc2 -- giving [1,2,3,4] and defeating the "text-relevant docs lead" guarantee these
// tests assert. With "4" the probe visits every cluster, doc3 is recovered, and the fused order is the
// intended [1,3,2,4]. (The previous "2" only ever passed because an unordered-Top bug emitted rows in an
// arbitrary order that coincidentally matched; once the order became deterministic the undersized probe
// surfaced -- see FinalRankPreservesOrder.)
//
// PRAGMA must come before any DECLARE, which must come before any other statement -- so the prologue order
// is always: pragma, [declare], $target.
const TString SearchPragma = R"sql(
    pragma ydb.KMeansTreeSearchTopSize = "4";
)sql";

const TString TargetExpr = R"sql(
    $target = Untag(Knn::ToBinaryStringUint8(Cast([250, 10] AS List<Uint8>)), "Uint8Vector");
)sql";

// Standard query prologue (no parameters): pragma + $target.
const TString TargetDecl = SearchPragma + TargetExpr;

// Prologue for queries that DECLARE parameters: pragma + declare(s) + $target (DECLARE must precede $target).
TString TargetDeclWith(const TString& declares) {
    return SearchPragma + declares + TargetExpr;
}

std::vector<ui64> RunKeys(TQueryClient& db, const TString& sql) {
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    std::vector<ui64> keys;
    TResultSetParser parser(result.GetResultSet(0));
    while (parser.TryNextRow()) {
        keys.push_back(*parser.ColumnParser("Key").GetOptionalUint64());
    }
    return keys;
}

std::vector<ui64> RunKeysWithContext(TQueryClient& db, const TString& sql, const TString& context) {
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
        TStringBuilder() << context << ": " << result.GetIssues().ToString());
    std::vector<ui64> keys;
    TResultSetParser parser(result.GetResultSet(0));
    while (parser.TryNextRow()) {
        keys.push_back(*parser.ColumnParser("Key").GetOptionalUint64());
    }
    return keys;
}

std::vector<ui64> RunUint64Column(TQueryClient& db, const TString& sql, const TString& column) {
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    std::vector<ui64> values;
    TResultSetParser parser(result.GetResultSet(0));
    while (parser.TryNextRow()) {
        values.push_back(*parser.ColumnParser(column).GetOptionalUint64());
    }
    return values;
}

std::vector<ui64> RunUint64Column(TQueryClient& db, const TString& sql, const TString& column, const TParams& params) {
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx(), params).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    std::vector<ui64> values;
    TResultSetParser parser(result.GetResultSet(0));
    while (parser.TryNextRow()) {
        values.push_back(*parser.ColumnParser(column).GetOptionalUint64());
    }
    return values;
}

std::vector<ui64> RunKeys(TQueryClient& db, const TString& sql, const TParams& params) {
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx(), params).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    std::vector<ui64> keys;
    TResultSetParser parser(result.GetResultSet(0));
    while (parser.TryNextRow()) {
        keys.push_back(*parser.ColumnParser("Key").GetOptionalUint64());
    }
    return keys;
}

TString RunFailIssues(TQueryClient& db, const TString& sql) {
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.GetStatus() != EStatus::SUCCESS, "expected the query to fail, but it succeeded");
    return result.GetIssues().ToString();
}

TString RunBadRequestIssues(TQueryClient& db, const TString& sql) {
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    return result.GetIssues().ToString();
}

TString RunBadRequestIssues(TQueryClient& db, const TString& sql, const TParams& params) {
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx(), params).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    return result.GetIssues().ToString();
}

} // namespace

Y_UNIT_TEST_SUITE(KqpHybridSearch) {

    // Core RRF behaviour. Docs 1 and 3 contain "cats" => present in BOTH the fulltext and vector result
    // sets; docs 2 and 4 have no text match => present in the vector set only (penalised in fulltext) —
    // and doc 2 is even the exact (nearest) vector match. RRF must still rank the in-both docs {1,3}
    // above the in-one docs {2,4}: fusing the fulltext signal is the whole point.
    // With the exhaustive search probe (see SearchPragma) and the fixed build seed the vector branch
    // returns all four docs ordered by true cosine distance to [250,10] (doc2 exact < doc1 < doc4 < doc3),
    // so the fused RRF order is fully deterministic:
    //   doc1: ft 1/(60+1) + vec 1/(60+2) = 0.0325   (best)
    //   doc3: ft 1/(60+2) + vec 1/(60+4) = 0.0317
    //   doc2:             + vec 1/(60+1) = 0.0164
    //   doc4:             + vec 1/(60+3) = 0.0159
    // i.e. exactly [1, 3, 2, 4].
    Y_UNIT_TEST_TWIN(FusesBothBranches, Compact) {
        auto kikimr = MakeRunnerWithCompact(Compact);
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 3u, 2u, 4u}), keys);
    }

    Y_UNIT_TEST(FulltextFloatNamedOptionsAreApplied) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        const TString query = TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats love dogs",
                    "or" AS DefaultOperator,
                    "2" AS MinimumShouldMatch,
                    1.2f AS K1,
                    0.75f AS B),
                Knn::CosineDistance(Embedding, $target),
                (4, 1) AS Limits)
            LIMIT 4;
        )sql";

        const auto keys = RunKeys(db, query);
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2);
        UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u}),
            TStringBuilder() << "unexpected keys; result count: " << keys.size());

        auto explainSettings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
        auto result = db.ExecuteQuery(query, TTxControl::NoTx(), explainSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats());
        auto planOpt = result.GetStats()->GetPlan();
        UNIT_ASSERT(planOpt.has_value());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*planOpt, &plan, true);
        const auto read = FindPlanNodeByKv(plan, "Name", "ReadFullTextIndex");
        UNIT_ASSERT_C(read.IsDefined(), TStringBuilder() << "ReadFullTextIndex operator not found in plan:\n" << *planOpt);
        UNIT_ASSERT(FindPlanNodeByKv(read, "DefaultOperator", "\"or\"").IsDefined());
        UNIT_ASSERT(FindPlanNodeByKv(read, "MinimumShouldMatch", "\"2\"").IsDefined());
        UNIT_ASSERT(FindPlanNodeByKv(read, "K1Factor", "\"1.2\"").IsDefined());
        UNIT_ASSERT(FindPlanNodeByKv(read, "BFactor", "\"0.75\"").IsDefined());
    }

    Y_UNIT_TEST(CandidateLimitsTruncateEachBranch) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        const auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                (1, 1) AS Limits)
            LIMIT 4;
        )sql");

        // The best fulltext candidate is doc1 and the best vector candidate is doc2. With one candidate
        // admitted from each branch, fusion must see exactly their two-row union even though LIMIT is 4.
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2u);
        UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u}),
            "per-branch Limits must truncate before fusion");
    }

    Y_UNIT_TEST(LargeCandidateSetIsNotSilentlyTruncated) {
        constexpr ui32 DocCount = 128;
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupLargeDocs(db, DocCount);

        const auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/LargeDocs`
            ORDER BY HybridRank(
                FullTextScore(Text, "needle"),
                Knn::CosineDistance(Embedding, $target),
                (128, 128) AS Limits)
            LIMIT 128;
        )sql");

        UNIT_ASSERT_VALUES_EQUAL_C(keys.size(), static_cast<size_t>(DocCount),
            "the vector branch should contribute every row to the fused candidate set");
        const std::set<ui64> uniqueKeys(keys.begin(), keys.end());
        UNIT_ASSERT_VALUES_EQUAL_C(uniqueKeys.size(), static_cast<size_t>(DocCount),
            "large candidate fusion must not duplicate primary keys");
        for (ui64 key = 1; key <= DocCount; ++key) {
            UNIT_ASSERT_C(uniqueKeys.contains(key), TStringBuilder() << "missing Key=" << key);
        }
    }

    Y_UNIT_TEST(FulltextNamedOptionParametersAreApplied) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        const TString query = TargetDeclWith(R"sql(
            DECLARE $defaultOperator AS String;
            DECLARE $minimumShouldMatch AS String;
            DECLARE $k1 AS Double;
            DECLARE $b AS Double;
        )sql") + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats love dogs",
                    $defaultOperator AS DefaultOperator,
                    $minimumShouldMatch AS MinimumShouldMatch,
                    $k1 AS K1,
                    $b AS B),
                Knn::CosineDistance(Embedding, $target),
                (4, 1) AS Limits)
            LIMIT 4;
        )sql";
        const auto params = TParamsBuilder()
            .AddParam("$defaultOperator").String("or").Build()
            .AddParam("$minimumShouldMatch").String("2").Build()
            .AddParam("$k1").Double(1.2).Build()
            .AddParam("$b").Double(0.75).Build()
            .Build();

        auto result = db.ExecuteQuery(query, TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        std::vector<ui64> keys;
        TResultSetParser parser(result.GetResultSet(0));
        while (parser.TryNextRow()) {
            keys.push_back(*parser.ColumnParser("Key").GetOptionalUint64());
        }
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2);
        UNIT_ASSERT((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u}));
    }

    Y_UNIT_TEST(RejectsUnsupportedFulltextNamedOption) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        const TString query = TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats", 1 AS Unknown),
                Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql";
        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "unsupported FullTextScore named argument 'Unknown'");

        const TString badTypeQuery = TargetDeclWith(R"sql(
            DECLARE $k1 AS Utf8;
        )sql") + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats", $k1 AS K1),
                Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql";
        const auto params = TParamsBuilder().AddParam("$k1").Utf8("not a number").Build().Build();
        result = db.ExecuteQuery(badTypeQuery, TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "FullTextScore named argument 'K1'");
    }

    // Regression guard: the final RRF fusion stage must keep its sort, so the result rows come back
    // ordered by the fused score and not merely as the correct top-N *set* in arbitrary order.
    //
    // The hybrid rewrite emits a TopSort (DESC by __ydb_hybrid_rrf) over the fused candidates wrapped in
    // a projection. If that projection is a plain Map (instead of OrderedMap) the sorted constraint is
    // dropped, and a downstream optimizer downgrades the TopSort to an unordered Top -- the final physical
    // plan then collects via an unordered DqCnUnionAll/WideTop with no order-preserving merge. The exact-
    // order assertions elsewhere (FusesBothBranches expects [1,3,2,4]) do not catch this because the tiny
    // single-partition fixture happens to emit Top in sorted order. So assert on the plan shape directly.
    //
    // Fingerprint of the correct (ordered) plan: a TopSort keyed on __ydb_hybrid_rrf feeding a *descending*
    // DqCnMerge. The buggy (unordered) plan has neither -- the only Merge in it is the ascending one from
    // the per-branch vector lookup, and the final fusion uses WideTop over UnionAll.
    Y_UNIT_TEST(FinalRankPreservesOrder) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
        auto res = db.ExecuteQuery(TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql", NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS, res.GetIssues().ToString());

        auto astOpt = res.GetStats()->GetAst();
        UNIT_ASSERT(astOpt.has_value());
        const TString ast = TString(*astOpt);

        // The fused candidates must be re-ranked with a TopSort (not an order-dropping Top) ...
        UNIT_ASSERT_C(ast.Contains("(TopSort (FlatMap") && ast.Contains("__ydb_hybrid_rrf"),
            TStringBuilder() << "final RRF re-rank must be a TopSort over __ydb_hybrid_rrf "
                << "(an unordered Top means OrderedMap regressed to Map); AST:\n" << ast);
        // ... and collected through an order-preserving descending merge.
        UNIT_ASSERT_C(ast.Contains("(DqCnMerge") && ast.Contains("'\"Desc\""),
            TStringBuilder() << "final fused result must flow through a descending DqCnMerge so the "
                << "RRF order survives to the result; AST:\n" << ast);
    }

    // Argument order is no longer significant: each branch is classified by inspecting the expression
    // (a FullTextScore is a fulltext branch, a Knn distance/similarity is a vector branch), so writing the
    // vector argument first fuses identically to the canonical fulltext-first order. RRF sums one term per
    // branch, so the fused score -- and the result [1, 3, 2, 4] from FusesBothBranches -- is unchanged.
    Y_UNIT_TEST(ArgumentOrderDoesNotMatter) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto forward = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql");
        auto reversed = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(Knn::CosineDistance(Embedding, $target), FullTextScore(Text, "cats"))
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 3u, 2u, 4u}), forward);
        UNIT_ASSERT_VALUES_EQUAL(forward, reversed);
    }

    // More than two branches fuse: a fulltext relevance branch plus two vector branches (cosine distance
    // and cosine similarity over the same vector index). Each branch resolves its index independently and
    // contributes one term to the per-document SUM. With (1, 1, 0) AS Weights the similarity branch is
    // zeroed out, recovering the two-branch [1, 3, 2, 4] order and exercising N-length Weights parsing.
    Y_UNIT_TEST(ThreeBranchFusion) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        for (const TString& mode : {TString("rrf"), TString("linear")}) {
            auto keys = RunKeys(db, TargetDecl + Sprintf(R"sql(
                SELECT Key FROM `/Root/Docs`
                ORDER BY HybridRank(
                    FullTextScore(Text, "cats"),
                    Knn::CosineDistance(Embedding, $target),
                    Knn::CosineSimilarity(Embedding, $target),
                    "%s" AS Mode)
                LIMIT 4;
            )sql", mode.c_str()));
            UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
                TStringBuilder() << "three-branch fusion (" << mode << ") returns the full candidate union");
            UNIT_ASSERT_C(keys[0] == 1u || keys[0] == 3u,
                TStringBuilder() << "a text-relevant doc must lead in three-branch fusion (" << mode << ")");
        }

        // Zeroing the third (similarity) branch via an N-length Weights tuple recovers the two-branch order.
        auto weighted = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                Knn::CosineSimilarity(Embedding, $target),
                (1, 1, 0) AS Weights)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 3u, 2u, 4u}), weighted);
    }

    // Alternative fusion: weighted linear combination of scores instead of RRF, with and without min-max
    // normalization. A text-relevant doc must still lead under the (default) normalized variant.
    Y_UNIT_TEST(LinearModeFuses) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto normalized = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                "linear" AS Mode)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_C((std::set<ui64>(normalized.begin(), normalized.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
            "normalized linear fusion returns the union of both branches");
        UNIT_ASSERT_C(normalized[0] == 1u || normalized[0] == 3u,
            "a text-relevant doc must lead under linear fusion too");

        // Without normalization the raw scores are fused (the magnitudes are not comparable, but the path
        // must still run and produce the candidate union).
        auto raw = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                "linear" AS Mode, (0.2, 0.8) AS Weights, false AS Normalize)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_C((std::set<ui64>(raw.begin(), raw.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
            "raw (non-normalized) linear fusion with weights must run and fuse both branches");
    }

    Y_UNIT_TEST(TiedAndBoundaryScoresKeepEveryCandidate) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupTiedScoreDocs(db);
        const TString tiedTargetDecl = SearchPragma + R"sql(
            $target = Untag(Knn::ToBinaryStringUint8(Cast([128, 128] AS List<Uint8>)), "Uint8Vector");
        )sql";

        const auto assertAllKeys = [](const std::vector<ui64>& keys, TStringBuf context) {
            UNIT_ASSERT_VALUES_EQUAL_C(keys.size(), 4u, context);
            UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
                context);
        };

        // All fulltext scores are equal and all four Euclidean distances are exactly 1. RRF may break
        // branch ties in any order, but it must retain every candidate exactly once.
        const auto rrf = RunKeysWithContext(db, tiedTargetDecl + R"sql(
            SELECT Key FROM `/Root/TiedDocs`
            ORDER BY HybridRank(
                FullTextScore(Text, "same"),
                Knn::EuclideanDistance(Embedding, $target))
            LIMIT 4;
        )sql", "equal raw scores under RRF");
        assertAllKeys(rrf, "equal raw scores under RRF");

        // Both branches have min == max. Linear normalization must take its zero-span path instead of
        // dividing by zero or losing rows; every fused contribution is consequently tied at zero.
        const auto linear = RunKeysWithContext(db, tiedTargetDecl + R"sql(
            SELECT Key FROM `/Root/TiedDocs`
            ORDER BY HybridRank(
                FullTextScore(Text, "same"),
                Knn::EuclideanDistance(Embedding, $target),
                "linear" AS Mode)
            LIMIT 4;
        )sql", "zero-span normalized linear scores");
        assertAllKeys(linear, "zero-span normalized linear scores");

        const auto zero = RunKeysWithContext(db, tiedTargetDecl + R"sql(
            SELECT Key FROM `/Root/TiedDocs`
            ORDER BY HybridRank(
                FullTextScore(Text, "same"),
                Knn::EuclideanDistance(Embedding, $target),
                ($scores) -> { RETURN 0.0; } AS ScoreLambda)
            LIMIT 4;
        )sql", "zero ScoreLambda result");
        assertAllKeys(zero, "zero ScoreLambda result");

        const auto negative = RunKeysWithContext(db, tiedTargetDecl + R"sql(
            SELECT Key FROM `/Root/TiedDocs`
            ORDER BY HybridRank(
                FullTextScore(Text, "same"),
                Knn::EuclideanDistance(Embedding, $target),
                ($scores) -> { RETURN -COALESCE($scores[1], 0.0); } AS ScoreLambda)
            LIMIT 4;
        )sql", "equal negative ScoreLambda results");
        assertAllKeys(negative, "equal negative ScoreLambda results");
    }

    // The vector signal may be a similarity (larger = better) instead of a distance: the branch is sorted
    // descending and fusion normalizes accordingly. Over a cosine index, CosineSimilarity ranks the same
    // way as CosineDistance, so the fused result matches.
    Y_UNIT_TEST(SimilarityFunctionFuses) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        for (const TString& mode : {TString("rrf"), TString("linear")}) {
            auto keys = RunKeys(db, TargetDecl + Sprintf(R"sql(
                SELECT Key FROM `/Root/Docs`
                ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineSimilarity(Embedding, $target),
                    "%s" AS Mode)
                LIMIT 4;
            )sql", mode.c_str()));
            UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
                TStringBuilder() << "CosineSimilarity (" << mode << ") must fuse both branches");
            UNIT_ASSERT_C(keys[0] == 1u || keys[0] == 3u,
                TStringBuilder() << "a text-relevant doc must lead with CosineSimilarity (" << mode << ")");
        }
    }

    // Weights take effect: a zero vector weight reduces the score to the fulltext term alone (1/(k+ftRank)
    // for RRF, normFt for linear), so ranking follows the fulltext signal and the highest-BM25 doc 1 leads
    // deterministically — in both modes.
    Y_UNIT_TEST(WeightsBiasRanking) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto rrf = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                (1, 0) AS Weights)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL_C(rrf[0], 1u, "RRF, vec weight 0 => ranked by fulltext => doc 1 (max BM25) leads");
        UNIT_ASSERT_C((std::set<ui64>{rrf[0], rrf[1]} == std::set<ui64>{1u, 3u}),
            "the two fulltext-matching docs still take the top positions");

        auto linear = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                "Linear" AS Mode, (1, 0) AS Weights)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL_C(linear[0], 1u, "linear, vec weight 0 => ranked by normFt => doc 1 (max BM25) leads");
    }

    // The spec writes Mode as "RRF"/"Linear"; the parser must accept that casing (not only lowercase).
    Y_UNIT_TEST(ModeAcceptsCanonicalCasing) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        for (const TString& mode : {TString("RRF"), TString("Linear")}) {
            auto keys = RunKeys(db, TargetDecl + Sprintf(R"sql(
                SELECT Key FROM `/Root/Docs`
                ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                    "%s" AS Mode)
                LIMIT 4;
            )sql", mode.c_str()));
            UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
                TStringBuilder() << "capitalized Mode \"" << mode << "\" must be accepted and fuse both branches");
        }
    }

    Y_UNIT_TEST(PlanShowsHybridSearch) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto explainSettings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
        auto result = db.ExecuteQuery(TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql", TTxControl::NoTx(), explainSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats());
        auto planOpt = result.GetStats()->GetPlan();
        UNIT_ASSERT(planOpt.has_value());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*planOpt, &plan, true);
        auto hybrid = FindPlanNodeByKv(plan, "Name", "HybridSearch");
        UNIT_ASSERT_C(hybrid.IsDefined(), TStringBuilder() << "HybridSearch operator not found in plan:\n" << *planOpt);
    }

    // LIMIT smaller than the candidate set: only the top fused docs are returned. We assert that the LIMIT
    // is respected (exactly two rows) and that those rows come from the candidate union {1,2,3,4}.
    //
    // We deliberately do NOT assert the specific pair is {1,3}: the per-branch candidate pool scales with the
    // LIMIT (LIMIT * HybridSearchFactor), and the approximate kmeans-tree vector branch may legitimately
    // return fewer rows for a smaller pool -- a known property of approximate vector search, not a bug. So a
    // small LIMIT can drop a candidate that a larger LIMIT would keep (e.g. LIMIT 2 here yields {1,2} rather
    // than the {1,3} that LIMIT 4 ranks on top -- see FusesBothBranches). The guarantee under test is only
    // that LIMIT caps the result and the survivors are valid candidates.
    Y_UNIT_TEST(RespectsLimit) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 2;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL_C(keys.size(), 2u, "LIMIT 2 must cap the fused result at two rows");
        const std::set<ui64> got(keys.begin(), keys.end());
        UNIT_ASSERT_VALUES_EQUAL_C(got.size(), 2u, "the two returned keys must be distinct");
        for (ui64 k : keys) {
            UNIT_ASSERT_C((std::set<ui64>{1u, 2u, 3u, 4u}.contains(k)),
                TStringBuilder() << "returned key " << k << " must be one of the four candidate docs");
        }
    }

    // WHERE on a main-table column is re-applied after the fused lookup.
    Y_UNIT_TEST(AppliesWherePredicate) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            WHERE Category = "a"
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 10;
        )sql");
        // Only docs 1 and 2 are category "a"; the WHERE is re-applied after the fused lookup.
        UNIT_ASSERT_C(!keys.empty(), "expected at least one category-a doc in the fused result");
        for (ui64 k : keys) {
            UNIT_ASSERT_C(k == 1u || k == 2u,
                TStringBuilder() << "WHERE must filter out category-b docs, but got key " << k);
        }
    }

    // A non-indexed column (Text) must be fetched via the main-table lookup.
    Y_UNIT_TEST(ProjectsNonKeyColumn) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto result = db.ExecuteQuery(TargetDecl + R"sql(
            SELECT Key, Text FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 1;
        )sql", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        const THashMap<ui64, TString> textByKey = {
            {1u, "cats cats cats love"}, {2u, "dogs and foxes run"},
            {3u, "cats sleep"}, {4u, "birds fly high"},
        };
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        const ui64 key = *parser.ColumnParser("Key").GetOptionalUint64();
        const TString text{*parser.ColumnParser("Text").GetOptionalUtf8()};
        UNIT_ASSERT_C(textByKey.contains(key), TStringBuilder() << "unexpected key " << key);
        // The non-indexed Text column must be fetched correctly via the main-table lookup.
        UNIT_ASSERT_VALUES_EQUAL(text, textByKey.at(key));
    }

    Y_UNIT_TEST(NamedIndexesOverride) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_idx", "vec_idx") AS Indexes,
                (100, 200) AS Limits,
                60.0 AS K)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
            "explicit indexes produce the same fused union");
        UNIT_ASSERT_C(keys[0] == 1u || keys[0] == 3u, "a text-relevant doc must rank first");
    }

    Y_UNIT_TEST_TWIN(NamedIndexesDisambiguate, Compact) {
        auto kikimr = MakeRunnerWithCompact(Compact);
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddFulltextIndex(db, "/Root/Docs", "ft_idx");
        AddFulltextIndex(db, "/Root/Docs", "ft_idx2");  // second fulltext index on the same column
        AddVectorIndex(db);

        // Auto-detect is ambiguous now (two fulltext indexes match column Text).
        auto issues = RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql");
        UNIT_ASSERT_STRING_CONTAINS(issues, "multiple fulltext relevance indexes");

        // An explicit AS Indexes override resolves the ambiguity.
        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                ("ft_idx2", "vec_idx") AS Indexes)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
            "the explicit index disambiguates and produces the fused result");
    }

    Y_UNIT_TEST(AutoSelectsVectorIndexByMetric) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddFulltextIndex(db);
        AddVectorIndex(db, "/Root/Docs", "vec_cosine", "distance=cosine");
        AddVectorIndex(db, "/Root/Docs", "vec_euclidean", "distance=euclidean");

        const auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 3u, 2u, 4u}), keys);
    }

    Y_UNIT_TEST(NonCosineVectorMetricsExecuteEndToEnd) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        // Target [100,100]. The points deliberately have different L1, L2, and inner-product orders:
        //   Manhattan:    3,1,2,4
        //   Euclidean:    3,2,1,4
        //   InnerProduct: 4,2,1,3
        ExecOk(db, Sprintf(R"sql(
            UPSERT INTO `/Root/Docs` (Key, Text, Embedding, Category) VALUES
                (1u, "cats", %s, "a"),
                (2u, "cats", %s, "a"),
                (3u, "cats", %s, "a"),
                (4u, "cats", %s, "a");
        )sql", Emb(100, 130).c_str(), Emb(120, 120).c_str(), Emb(101, 101).c_str(), Emb(200, 200).c_str()));
        AddFulltextIndex(db);

        struct TMetricCase {
            const char* IndexName;
            const char* IndexMetric;
            const char* KnnFunction;
            const char* FusedScore;
            std::vector<ui64> ExpectedOrder;
        };
        const std::array<TMetricCase, 3> metricCases{{
            {"vec_manhattan", "distance=manhattan", "ManhattanDistance",
                "-COALESCE($scores[1], 1000000.0)", {3u, 1u, 2u, 4u}},
            {"vec_euclidean", "distance=euclidean", "EuclideanDistance",
                "-COALESCE($scores[1], 1000000.0)", {3u, 2u, 1u, 4u}},
            {"vec_inner_product", "similarity=inner_product", "InnerProductSimilarity",
                "COALESCE($scores[1], -1000000.0)", {4u, 2u, 1u, 3u}},
        }};

        for (const auto& metricCase : metricCases) {
            AddVectorIndex(db, "/Root/Docs", metricCase.IndexName, metricCase.IndexMetric);
        }

        const TString metricTargetDecl = SearchPragma + R"sql(
            $target = Untag(Knn::ToBinaryStringUint8(Cast([100, 100] AS List<Uint8>)), "Uint8Vector");
        )sql";
        for (const auto& metricCase : metricCases) {
            const TString query = metricTargetDecl + Sprintf(R"sql(
                SELECT Key FROM `/Root/Docs`
                ORDER BY HybridRank(
                    FullTextScore(Text, "cats"),
                    Knn::%s(Embedding, $target),
                    ($scores) -> { RETURN %s; } AS ScoreLambda)
                LIMIT 4;
            )sql", metricCase.KnnFunction, metricCase.FusedScore);
            const auto keys = RunKeys(db, query);
            UNIT_ASSERT_VALUES_EQUAL_C(metricCase.ExpectedOrder, keys,
                TStringBuilder() << "unexpected HybridRank order for " << metricCase.KnnFunction);

            auto explainSettings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
            auto explain = db.ExecuteQuery(query, TTxControl::NoTx(), explainSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explain.GetStatus(), EStatus::SUCCESS, explain.GetIssues().ToString());
            UNIT_ASSERT(explain.GetStats());
            const auto plan = explain.GetStats()->GetPlan();
            UNIT_ASSERT_C(plan.has_value(), TStringBuilder() << "missing plan for " << metricCase.KnnFunction);
            for (const auto& candidate : metricCases) {
                UNIT_ASSERT_VALUES_EQUAL_C(plan->find(candidate.IndexName) != std::string::npos,
                    TStringBuf(candidate.IndexName) == metricCase.IndexName,
                    TStringBuilder() << "wrong vector index in plan for " << metricCase.KnnFunction
                        << ":\n" << *plan);
            }
        }
    }

    Y_UNIT_TEST(RejectsExplicitVectorIndexWithIncompatibleMetric) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddFulltextIndex(db);
        AddVectorIndex(db, "/Root/Docs", "vec_euclidean", "distance=euclidean");

        const auto issues = RunBadRequestIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_idx", "vec_euclidean") AS Indexes)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_STRING_CONTAINS(issues, "incompatible metric");
        UNIT_ASSERT_STRING_CONTAINS(issues, "Knn::EuclideanDistance");
    }

    // Malformed HybridRank usages that share the standard fixture must each fail with a clear message.
    Y_UNIT_TEST(RejectsMalformedQueries) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        // A single scoring argument is not a hybrid query: there is nothing to fuse.
        UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"))
            LIMIT 3;
        )sql"), "at least 2");

        // HybridRank nested inside a larger sort expression (would silently change the ordering).
        UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY -HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql"), "must be the entire ORDER BY key");

        // A per-branch override tuple (here Weights) must have exactly one entry per scoring argument.
        UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                (1, 2, 3) AS Weights)
            LIMIT 4;
        )sql"), "Weights has 3 entries but there are 2 scoring arguments");

        // An explicit index name that does not exist.
        UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                ("does_not_exist", "vec_idx") AS Indexes)
            LIMIT 4;
        )sql"), "fulltext index 'does_not_exist' was not found");

        // A parameterised (non-literal) LIMIT cannot size the branch candidate pools.
        auto params = TParamsBuilder().AddParam("$lim").Uint64(3).Build().Build();
        auto limitResult = db.ExecuteQuery(TargetDeclWith("DECLARE $lim AS Uint64;\n") + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT $lim;
        )sql", TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(limitResult.GetStatus() != EStatus::SUCCESS, "expected failure for a parameterised LIMIT");
        UNIT_ASSERT_STRING_CONTAINS(limitResult.GetIssues().ToString(), "requires a literal LIMIT");
    }

    Y_UNIT_TEST(RejectsWrappedFulltextScore) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        const auto issues = RunBadRequestIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                2.0 * FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql");
        UNIT_ASSERT_STRING_CONTAINS(issues, "must be a bare FullTextScore expression");
    }

    // The TableServiceConfig.EnableHybridSearch kill-switch. It is on by default (so every other test
    // exercises the enabled path); with it off, a HybridRank query must fail with a clear message rather
    // than being rewritten.
    Y_UNIT_TEST(DisabledByFlag) {
        auto kikimr = MakeRunner(/*enableHybridSearch=*/false);
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql"), "hybrid search is disabled");
    }

    // HybridRank needs both a fulltext relevance index and a vector index; missing either is an error.
    Y_UNIT_TEST(RejectsWhenIndexMissing) {
        const TString query = TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 3;
        )sql";
        {   // vector index only -> no fulltext relevance index
            auto kikimr = MakeRunner();
            auto db = kikimr.GetQueryClient();
            CreateDocs(db);
            UpsertDocs(db);
            AddVectorIndex(db);
            UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, query), "no ready fulltext relevance index");
        }
        {   // fulltext index only -> no vector index
            auto kikimr = MakeRunner();
            auto db = kikimr.GetQueryClient();
            CreateDocs(db);
            UpsertDocs(db);
            AddFulltextIndex(db);
            UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, query), "no ready vector");
        }
    }

    Y_UNIT_TEST(UsesPrefixedVectorIndex) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddPrefixedFulltextIndex(db);
        AddPrefixedVectorIndex(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            WHERE Category = "a"
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_idx", "vp_idx") AS Indexes)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 2u}), keys);
    }

    Y_UNIT_TEST(UsesFullPrefixWithPkInVectorIndex) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupUserPkPrefixedDocs(db);

        const auto params = TParamsBuilder()
            .AddParam("$filter")
                .BeginStruct()
                    .AddMember("pk").Uint64(1)
                .EndStruct()
                .Build()
            .Build();
        const auto keys = RunUint64Column(db, TargetDeclWith(R"sql(
            DECLARE $filter AS Struct<pk: Uint64>;
        )sql") + R"sql(
            SELECT pk FROM `/Root/UserDocs`
            WHERE user = "alice" AND pk = $filter.pk
            ORDER BY HybridRank(
                FullTextScore(text, "cats"),
                Knn::CosineDistance(embedding, $target),
                ("ft_idx", "vec_idx") AS Indexes)
            LIMIT 4;
        )sql", "pk", params);
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u}), keys);
    }

    Y_UNIT_TEST(UsesLeadingSubPrefixWithPkInVectorIndex) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupUserPkPrefixedDocs(db);

        const auto keys = RunUint64Column(db, TargetDecl + R"sql(
            SELECT pk FROM `/Root/UserDocs`
            WHERE user = "alice"
            ORDER BY HybridRank(
                FullTextScore(text, "cats"),
                Knn::CosineDistance(embedding, $target),
                ("ft_idx", "vec_idx") AS Indexes)
            LIMIT 4;
        )sql", "pk");
        UNIT_ASSERT_C((std::set<ui64>{keys.begin(), keys.end()} == std::set<ui64>{1u, 2u}),
            TStringBuilder() << "unexpected keys; result count: " << keys.size());
    }

    Y_UNIT_TEST(UsesPrefixedCompactFulltextIndexWithPlainVectorIndex) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddPrefixedFulltextIndex(db);
        AddVectorIndex(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            WHERE Category = "a"
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_idx", "vec_idx") AS Indexes)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 2u}), keys);
    }

    Y_UNIT_TEST(AutoDetectsPrefixedIndexes) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddPrefixedFulltextIndex(db);
        AddPrefixedVectorIndex(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            WHERE Category = "a"
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 2u}), keys);
    }

    Y_UNIT_TEST(FulltextScoreOnPrefixColumnSelectsItsOwnIndex) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        ExecOk(db, R"sql(
            ALTER TABLE `/Root/Docs` ADD INDEX ft_category
                GLOBAL USING fulltext_relevance
                ON (Category)
                WITH (tokenizer=standard, use_filter_lowercase=true);
        )sql");
        AddPrefixedFulltextIndex(db, "/Root/Docs", "ft_text_prefixed");
        AddVectorIndex(db);

        const auto params = TParamsBuilder().AddParam("$category").Utf8("a").Build().Build();
        const auto keys = RunKeys(db, TargetDeclWith(R"sql(
            DECLARE $category AS Utf8;
        )sql") + R"sql(
            SELECT Key FROM `/Root/Docs`
            WHERE Category = $category
            ORDER BY HybridRank(
                FullTextScore(Category, "a"),
                Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql", params);
        UNIT_ASSERT_C((std::set<ui64>{keys.begin(), keys.end()} == std::set<ui64>{1u, 2u}),
            TStringBuilder() << "unexpected keys; result count: " << keys.size());
    }

    Y_UNIT_TEST(UsesParameterizedPrefix) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddPrefixedFulltextIndex(db);
        AddPrefixedVectorIndex(db);

        const auto params = TParamsBuilder().AddParam("$category").Utf8("a").Build().Build();
        auto keys = RunKeys(db, TargetDeclWith("DECLARE $category AS Utf8;\n") + R"sql(
            SELECT Key FROM `/Root/Docs`
            WHERE Category = $category
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_idx", "vp_idx") AS Indexes)
            LIMIT 4;
        )sql", params);
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 2u}), keys);
    }

    Y_UNIT_TEST(UsesStructParameterMemberAsPrefixValue) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddPrefixedFulltextIndex(db);
        AddPrefixedVectorIndex(db);

        const auto params = TParamsBuilder()
            .AddParam("$filter")
                .BeginStruct()
                    .AddMember("A").Uint64(42)
                    .AddMember("Category").Utf8("a")
                .EndStruct()
                .Build()
            .Build();
        const auto keys = RunKeys(db, TargetDeclWith(R"sql(
            DECLARE $filter AS Struct<A: Uint64, Category: Utf8>;
        )sql") + R"sql(
            SELECT Key FROM `/Root/Docs`
            WHERE Category = $filter.Category
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_idx", "vp_idx") AS Indexes)
            LIMIT 4;
        )sql", params);
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 2u}), keys);
    }

    Y_UNIT_TEST(RejectsOptionalStructParameterMemberAsPrefixValue) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddPrefixedFulltextIndex(db);
        AddPrefixedVectorIndex(db);

        const auto params = TParamsBuilder()
            .AddParam("$filter")
                .BeginOptional()
                    .BeginStruct()
                        .AddMember("A").Uint64(42)
                        .AddMember("Category").Utf8("a")
                    .EndStruct()
                .EndOptional()
                .Build()
            .Build();
        const auto issues = RunBadRequestIssues(db, TargetDeclWith(R"sql(
            DECLARE $filter AS Struct<A: Uint64, Category: Utf8>?;
        )sql") + R"sql(
            SELECT Key FROM `/Root/Docs`
            WHERE Category = $filter.Category
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_idx", "vp_idx") AS Indexes)
            LIMIT 4;
        )sql", params);
        UNIT_ASSERT_STRING_CONTAINS(issues,
            "prefixed fulltext index 'ft_idx' requires equality predicates on every prefix column");
    }

    Y_UNIT_TEST(DoesNotTreatStructParameterMemberAsTablePrefix) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddFulltextIndex(db, "/Root/Docs", "ft_plain");
        AddPrefixedFulltextIndex(db, "/Root/Docs", "ft_prefixed");
        AddVectorIndex(db, "/Root/Docs", "vec_plain");
        AddPrefixedVectorIndex(db, "/Root/Docs", "vec_prefixed");

        const auto params = TParamsBuilder()
            .AddParam("$left")
                .BeginStruct()
                    .AddMember("Category").Utf8("a")
                .EndStruct()
                .Build()
            .AddParam("$right")
                .BeginStruct()
                    .AddMember("Category").Utf8("a")
                .EndStruct()
                .Build()
            .Build();
        const auto result = db.ExecuteQuery(R"sql(
            DECLARE $left AS Struct<Category: Utf8>;
            DECLARE $right AS Struct<Category: Utf8>;

            SELECT Key FROM `/Root/Docs` VIEW `ft_prefixed`
            WHERE $left.Category = $right.Category
                AND FulltextScore(Text, "cats") > 0
            ORDER BY Key;
        )sql", TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());

        const TString hybridDecl = TargetDeclWith(R"sql(
            DECLARE $left AS Struct<Category: Utf8>;
            DECLARE $right AS Struct<Category: Utf8>;
        )sql");
        const auto fulltextIssues = RunBadRequestIssues(db, hybridDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            WHERE $left.Category = $right.Category
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_prefixed", "vec_plain") AS Indexes)
            LIMIT 4;
        )sql", params);
        UNIT_ASSERT_STRING_CONTAINS(fulltextIssues,
            "prefixed fulltext index 'ft_prefixed' requires equality predicates on every prefix column");

        const auto vectorIssues = RunBadRequestIssues(db, hybridDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            WHERE $left.Category = $right.Category
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_plain", "vec_prefixed") AS Indexes)
            LIMIT 4;
        )sql", params);
        UNIT_ASSERT_STRING_CONTAINS(vectorIssues,
            "prefixed vector index 'vec_prefixed' requires equality predicates on a contiguous leading prefix");
    }

    Y_UNIT_TEST(UsesNullablePrefix) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateDocs(db, "/Root/NullableDocs", /*categoryNotNull=*/false);
        UpsertDocs(db, "/Root/NullableDocs");
        AddPrefixedFulltextIndex(db, "/Root/NullableDocs");
        AddPrefixedVectorIndex(db, "/Root/NullableDocs");

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/NullableDocs`
            WHERE Category = "a"
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_idx", "vp_idx") AS Indexes)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 2u}), keys);
    }

    Y_UNIT_TEST(UsesMultiColumnPrefixesInIndexOrder) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateMultiPrefixDocs(db);
        AddMultiPrefixedFulltextIndex(db);
        AddMultiPrefixedVectorIndex(db);

        // WHERE order is deliberately reversed relative to the index prefix (Region, Category).
        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/MultiDocs`
            WHERE Category = "a" AND Region = "r1"
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_multi", "vec_multi") AS Indexes)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 2u}), keys);
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorCrossProduct, EnableVectorSearchActor) {
        enum class EExpectedOrder {
            TextFirst,
            VectorFirst,
        };
        struct TFusionCase {
            const char* Name;
            const char* KnnFunction;
            const char* Options;
            EExpectedOrder ExpectedOrder;
        };
        const std::array fusionCases = {
            TFusionCase{"rrf", "CosineDistance", "", EExpectedOrder::TextFirst},
            TFusionCase{"linear", "CosineDistance", R"sql(,
                    "linear" AS Mode)sql", EExpectedOrder::VectorFirst},
            TFusionCase{"score_lambda", "CosineDistance", R"sql(,
                    ($scores) -> {
                        RETURN -COALESCE($scores[1], 1000000.0);
                    } AS ScoreLambda)sql", EExpectedOrder::VectorFirst},
            TFusionCase{"rank_lambda", "CosineDistance", R"sql(,
                    ($ranks) -> {
                        RETURN   1.0 / (60 + COALESCE($ranks[0], 100000))
                             + 100.0 / (60 + COALESCE($ranks[1], 100000));
                    } AS RankLambda)sql", EExpectedOrder::VectorFirst},
            TFusionCase{"cosine_similarity", "CosineSimilarity", "", EExpectedOrder::TextFirst},
        };

        ui32 executedQueries = 0;
        auto kikimr = MakeRunner(/*enableHybridSearch=*/true, EnableVectorSearchActor);
        auto db = kikimr.GetQueryClient();
        for (const bool multiPrefix : {false, true}) {
            for (const bool nullablePrefix : {false, true}) {
                for (const bool pkSuffix : {false, true}) {
                    const THybridPrefixMatrixShape shape{multiPrefix, nullablePrefix, pkSuffix};
                    SetupHybridPrefixMatrixFixture(db, shape);
                    const TString table = HybridPrefixMatrixTable(shape);
                    const TString predicates = multiPrefix
                        ? R"sql(Tenant = "a" AND Region = "r1")sql"
                        : R"sql(Tenant = "a")sql";

                    for (const auto& fusionCase : fusionCases) {
                        const TString context = TStringBuilder()
                            << "actor=" << EnableVectorSearchActor
                            << ", multiPrefix=" << multiPrefix
                            << ", nullablePrefix=" << nullablePrefix
                            << ", pkSuffix=" << pkSuffix
                            << ", fusion=" << fusionCase.Name;
                        const auto keys = RunKeysWithContext(db, TargetDecl + Sprintf(R"sql(
                            SELECT Key FROM `%s`
                            WHERE %s
                            ORDER BY HybridRank(
                                FullTextScore(Text, "cats"),
                                Knn::%s(Embedding, $target),
                                ("ft_idx", "vec_idx") AS Indexes%s)
                            LIMIT 4;
                        )sql", table.c_str(), predicates.c_str(), fusionCase.KnnFunction,
                            fusionCase.Options), context);
                        ++executedQueries;

                        if (fusionCase.ExpectedOrder == EExpectedOrder::TextFirst) {
                            UNIT_ASSERT_VALUES_EQUAL_C((std::vector<ui64>{1u, 2u}), keys, context);
                        } else {
                            UNIT_ASSERT_VALUES_EQUAL_C((std::vector<ui64>{2u, 1u}), keys, context);
                        }
                    }
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(executedQueries, 40u);
    }

    Y_UNIT_TEST(RejectsPartiallyBoundMultiColumnPrefixes) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateMultiPrefixDocs(db);
        AddFulltextIndex(db, "/Root/MultiDocs", "ft_plain");
        AddMultiPrefixedFulltextIndex(db);
        AddVectorIndex(db, "/Root/MultiDocs", "vec_plain");
        AddMultiPrefixedVectorIndex(db);

        auto fulltextIssues = RunBadRequestIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/MultiDocs`
            WHERE Category = "a"
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_multi", "vec_plain") AS Indexes)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_STRING_CONTAINS(fulltextIssues,
            "prefixed fulltext index 'ft_multi' requires equality predicates on every prefix column");

        auto vectorIssues = RunBadRequestIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/MultiDocs`
            WHERE Category = "a"
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_plain", "vec_multi") AS Indexes)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_STRING_CONTAINS(vectorIssues,
            "prefixed vector index 'vec_multi' requires equality predicates on a contiguous leading prefix");
    }

    Y_UNIT_TEST(RejectsPrefixEqualityUnderOr) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddPrefixedFulltextIndex(db);
        AddVectorIndex(db);

        auto issues = RunBadRequestIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            WHERE Category = "a" OR Category = "b"
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ("ft_idx", "vec_idx") AS Indexes)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_STRING_CONTAINS(issues,
            "prefixed fulltext index 'ft_idx' requires equality predicates on every prefix column");
    }

    Y_UNIT_TEST(AutoDetectionSkipsUnboundPrefixedIndexes) {
        auto kikimr = MakeRunnerWithCompact(/*compact=*/true, /*enableFulltextPrefix=*/true);
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddFulltextIndex(db);
        AddPrefixedFulltextIndex(db, "/Root/Docs", "ft_prefixed");
        AddVectorIndex(db);
        AddPrefixedVectorIndex(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 3u, 2u, 4u}), keys);
    }

    // A prefixed vector index cannot be used without a predicate that binds its prefix.
    Y_UNIT_TEST(ErrorWhenPrefixedVectorIndex) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddFulltextIndex(db);
        AddPrefixedVectorIndex(db);  // only a prefixed vector index exists

        // Auto-detect filters out the prefixed index, so no usable vector index is found.
        auto issues = RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql");
        UNIT_ASSERT_STRING_CONTAINS(issues, "no ready vector");

        // Naming it explicitly reports the missing prefix binding precisely.
        auto issues2 = RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                ("ft_idx", "vp_idx") AS Indexes)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_STRING_CONTAINS(issues2, "prefixed vector index");
    }

    // An explicit Limits override is the escape hatch: it lets a parameterised LIMIT work.
    Y_UNIT_TEST(ParameterizedLimitWithExplicitLimits) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto params = TParamsBuilder().AddParam("$lim").Uint64(4).Build().Build();
        auto result = db.ExecuteQuery(TargetDeclWith("DECLARE $lim AS Uint64;\n") + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                (100, 200) AS Limits)
            LIMIT $lim;
        )sql", TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        std::vector<ui64> keys;
        TResultSetParser parser(result.GetResultSet(0));
        while (parser.TryNextRow()) {
            keys.push_back(*parser.ColumnParser("Key").GetOptionalUint64());
        }
        UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
            "explicit Limits allow a parameterised LIMIT and still fuse both branches");
    }

    // A custom `... AS RankLambda` lambda receives the document's per-branch ranks as $ranks (branch index
    // -> 1-based rank; a branch the document is absent from has no entry, so $ranks[i] is NULL). Spelling RRF
    // out by hand -- equal weights, k=60, a large penalty for the absent branch via COALESCE -- must
    // reproduce the built-in rrf order [1, 3, 2, 4] from FusesBothBranches.
    Y_UNIT_TEST(RankLambdaReproducesRrf) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),                 -- branch 0
                Knn::CosineDistance(Embedding, $target),     -- branch 1
                ($ranks) -> {
                    RETURN 1.0 / (60 + COALESCE($ranks[0], 100000))
                         + 1.0 / (60 + COALESCE($ranks[1], 100000));
                } AS RankLambda)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 3u, 2u, 4u}), keys);
    }

    // The lambda can weight branches asymmetrically: heavily favouring the vector branch overrides the
    // text signal and recovers the pure vector order (doc2 exact < doc1 near < doc4 mid < doc3 opposite),
    // i.e. [2, 1, 4, 3] -- a different order from the balanced RRF [1, 3, 2, 4].
    Y_UNIT_TEST(RankLambdaCustomWeightsReorder) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ($ranks) -> {
                    RETURN   1.0 / (60 + COALESCE($ranks[0], 100000))
                         + 100.0 / (60 + COALESCE($ranks[1], 100000));
                } AS RankLambda)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{2u, 1u, 4u, 3u}), keys);
    }

    // Three branches fuse through one lambda that indexes $ranks[0..2]. Zeroing the third (cosine
    // similarity) term reduces to the two-branch RRF and recovers [1, 3, 2, 4], while still exercising the
    // full three-slot rank-vector assembly (a third rank column grouped by pk).
    Y_UNIT_TEST(RankLambdaThreeBranches) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),                 -- branch 0
                Knn::CosineDistance(Embedding, $target),     -- branch 1
                Knn::CosineSimilarity(Embedding, $target),   -- branch 2
                ($ranks) -> {
                    RETURN 1.0 / (60 + COALESCE($ranks[0], 100000))
                         + 1.0 / (60 + COALESCE($ranks[1], 100000))
                         + 0.0 / (60 + COALESCE($ranks[2], 100000));
                } AS RankLambda)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 3u, 2u, 4u}), keys);
    }

    // A genuine three-branch fusion where the *third* slot drives the outcome: heavily weighting the cosine
    // similarity branch ($ranks[2]) overrides the text signal and recovers the pure vector order
    // [2, 1, 4, 3] (doc2 exact < doc1 near < doc4 mid < doc3 opposite). Unlike RankLambdaThreeBranches,
    // which zeroes the third term, here the third branch's rank changes the result -- so the test fails if
    // the third rank column is dropped, mis-indexed, or shifted. Over the cosine index CosineSimilarity
    // ranks the same as CosineDistance, so branches 1 and 2 carry identical per-doc ranks; the 100x weight
    // on branch 2 therefore dominates the balanced text+vector contribution of branches 0 and 1.
    //   doc2: 1/(60+inf) + 1/(60+1) + 100/(60+1) = 1.6557   (best; exact vector match, weight on vector)
    //   doc1: 1/(60+1)   + 1/(60+2) + 100/(60+2) = 1.6454
    //   doc4: 1/(60+inf) + 1/(60+3) + 100/(60+3) = 1.6032
    //   doc3: 1/(60+2)   + 1/(60+4) + 100/(60+4) = 1.5943
    Y_UNIT_TEST(RankLambdaThreeBranchesThirdSlotDrives) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),                 -- branch 0
                Knn::CosineDistance(Embedding, $target),     -- branch 1
                Knn::CosineSimilarity(Embedding, $target),   -- branch 2
                ($ranks) -> {
                    RETURN   1.0 / (60 + COALESCE($ranks[0], 100000))
                         +   1.0 / (60 + COALESCE($ranks[1], 100000))
                         + 100.0 / (60 + COALESCE($ranks[2], 100000));
                } AS RankLambda)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{2u, 1u, 4u, 3u}), keys);
    }

    // ScoreLambda fuses the *raw* per-branch scores instead of the ranks: $scores[i] is the branch's score
    // value as a Double (the fulltext relevance, or the vector distance/similarity), NULL if the document is
    // absent from that branch. Negating the cosine *distance* (smaller is closer) makes it a larger-is-better
    // score, so ignoring text and ranking by -distance recovers the pure vector order [2, 1, 4, 3]
    // (doc2 exact < doc1 near < doc4 mid < doc3 opposite). This depends only on the fixed relative ordering
    // of the distances -- not their exact magnitudes -- so it is deterministic.
    Y_UNIT_TEST(ScoreLambdaRanksByRawVectorScore) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),                 -- branch 0 (unused by the lambda)
                Knn::CosineDistance(Embedding, $target),     -- branch 1
                ($scores) -> {
                    RETURN -COALESCE($scores[1], 1000000.0);
                } AS ScoreLambda)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{2u, 1u, 4u, 3u}), keys);
    }

    Y_UNIT_TEST(ScoreLambdaHandlesCompletelyMissingBranch) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        const auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "quokka"),              -- no document enters branch 0
                Knn::CosineDistance(Embedding, $target),     -- every document enters branch 1
                ($scores) -> {
                    RETURN COALESCE($scores[0], -1000000.0)
                         - COALESCE($scores[1],  1000000.0);
                } AS ScoreLambda)
            LIMIT 4;
        )sql");

        // Every text score is absent and every resulting fused score is negative. The missing dictionary
        // slot must reach COALESCE, while the vector term still determines the exact result order.
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{2u, 1u, 4u, 3u}), keys);
    }

    // ScoreLambda over the fulltext relevance score: ranking by the raw BM25 alone puts the highest-relevance
    // doc 1 ("cats" x3) first, then doc 3 ("cats" x1); docs 2 and 4 have no text match, so $scores[0] is NULL
    // and the COALESCE sentinel sends them to the bottom. We assert the deterministic top (doc 1 leads, and
    // {1,3} take the two text-relevant slots) without pinning the exact BM25 magnitudes.
    Y_UNIT_TEST(ScoreLambdaRanksByRawTextScore) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),                 -- branch 0
                Knn::CosineDistance(Embedding, $target),     -- branch 1 (unused by the lambda)
                ($scores) -> {
                    RETURN COALESCE($scores[0], -1.0);
                } AS ScoreLambda)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL_C(keys[0], 1u, "raw BM25 ranking puts the highest-relevance doc 1 first");
        UNIT_ASSERT_C((std::set<ui64>{keys[0], keys[1]} == std::set<ui64>{1u, 3u}),
            "the two text-relevant docs take the top positions under raw text-score fusion");
    }

    // A custom fusion lambda replaces the built-in fusion, so combining it with the built-in fusion knobs
    // (Mode / Weights / K / Normalize) is rejected with a clear message. The message names the lambda kind.
    Y_UNIT_TEST(CustomLambdaRejectsConflictingOptions) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        for (const TString& opt : {TString("\"rrf\" AS Mode"), TString("(1, 2) AS Weights"),
                                   TString("60.0 AS K"), TString("true AS Normalize")}) {
            auto issues = RunFailIssues(db, TargetDecl + Sprintf(R"sql(
                SELECT Key FROM `/Root/Docs`
                ORDER BY HybridRank(
                    FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                    %s,
                    ($ranks) -> { RETURN 1.0 / (60 + COALESCE($ranks[0], 100000)); } AS RankLambda)
                LIMIT 4;
            )sql", opt.c_str()));
            UNIT_ASSERT_STRING_CONTAINS_C(issues, "cannot be combined with a custom RankLambda",
                TStringBuilder() << "option " << opt << " should conflict with RankLambda");
        }

        // The conflict message names ScoreLambda when that is the lambda kind in play.
        UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                "rrf" AS Mode,
                ($scores) -> { RETURN COALESCE($scores[0], 0.0); } AS ScoreLambda)
            LIMIT 4;
        )sql"), "cannot be combined with a custom ScoreLambda");
    }

    // At most one fusion lambda may be given: RankLambda and ScoreLambda together is rejected by the SQL
    // frontend before type checking.
    Y_UNIT_TEST(CustomLambdaRejectsBothKinds) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                ($ranks)  -> { RETURN 1.0 / (60 + COALESCE($ranks[0], 100000)); } AS RankLambda,
                ($scores) -> { RETURN COALESCE($scores[0], 0.0); } AS ScoreLambda)
            LIMIT 4;
        )sql"), "at most one of RankLambda or ScoreLambda");
    }

    // A fusion lambda must be a lambda that returns a numeric score; a non-lambda value and a non-numeric
    // result are both rejected up front during type annotation. A non-lambda is caught by ConvertToLambda
    // ("Expected lambda"); a non-numeric return is reported with the lambda-kind name.
    Y_UNIT_TEST(CustomLambdaRejectsBadLambda) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        // A non-lambda AS RankLambda.
        UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                5 AS RankLambda)
            LIMIT 4;
        )sql"), "Expected lambda");

        // A non-lambda AS ScoreLambda.
        UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                5 AS ScoreLambda)
            LIMIT 4;
        )sql"), "Expected lambda");

        // A RankLambda that returns a non-numeric value.
        UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                ($ranks) -> { RETURN "not a number"; } AS RankLambda)
            LIMIT 4;
        )sql"), "must return a numeric score");
    }

    // Note: the composite-primary-key guard in the optimizer is defensive only. A fulltext-relevance
    // index cannot be created on a composite-PK table at all, so a hybrid query never reaches it
    // (auto-detect fails to find a fulltext index first); there is no valid setup to exercise it here.
}

} // namespace NKikimr::NKqp
