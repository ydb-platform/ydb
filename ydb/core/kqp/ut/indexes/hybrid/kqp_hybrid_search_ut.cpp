#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tx.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/json/json_reader.h>

#include <cmath>
#include <limits>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

TKikimrRunner MakeRunner(bool enableHybridSearch = true, bool enableCompactFulltextIndex = false,
        bool enableIndexStreamWrite = false, bool useRealThreads = true) {
    // Fix the kmeans-tree build sampling seed so the index tree is reproducible run-to-run (otherwise it
    // seeds from the tablet id). Combined with the exhaustive search probe in TargetDecl below, this makes
    // the vector branch fully deterministic. See gVectorIndexSeed in schemeshard_impl.h (tests only).
    NSchemeShard::gVectorIndexSeed = 1337;

    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    featureFlags.SetEnableCompactFulltextIndex(enableCompactFulltextIndex);
    auto settings = TKikimrSettings()
        .SetFeatureFlags(featureFlags)
        .SetUseRealThreads(useRealThreads)
        .SetEnableStrictSerializableIsolation(true);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    // EnableHybridSearch is on by default; the explicit set both documents the dependency and lets
    // DisabledByFlag exercise the off path.
    settings.AppConfig.MutableTableServiceConfig()->SetEnableHybridSearch(enableHybridSearch);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(enableIndexStreamWrite);
    return TKikimrRunner(settings);
}

TKikimrRunner MakeRunnerWithCompact(bool compact) {
    NSchemeShard::gVectorIndexSeed = 1337;
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    featureFlags.SetEnableCompactFulltextIndex(compact);
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

void CreateDocs(TQueryClient& db) {
    ExecOk(db, R"sql(
        CREATE TABLE `/Root/Docs` (
            Key Uint64,
            Text Utf8,
            Embedding String,
            Category Utf8,
            PRIMARY KEY (Key)
        );
    )sql");
}

void UpsertDocs(TQueryClient& db) {
    ExecOk(db, Sprintf(R"sql(
        UPSERT INTO `/Root/Docs` (Key, Text, Embedding, Category) VALUES
            (1u, "cats cats cats love", %s, "a"),
            (2u, "dogs and foxes run",  %s, "a"),
            (3u, "cats sleep",          %s, "b"),
            (4u, "birds fly high",      %s, "b");
    )sql", Emb(1).c_str(), Emb(2).c_str(), Emb(3).c_str(), Emb(4).c_str()));
}

void UpsertNewSnapshotDocs(TQueryClient& db) {
    // Deliberately move both branch winners. The old hybrid order is [1,3,2,4], while this state
    // makes keys 2/3 text-relevant and keys 4/2 vector-nearest. A mixed old/new branch read cannot
    // accidentally reproduce both complete baseline orders.
    ExecOk(db, Sprintf(R"sql(
        UPSERT INTO `/Root/Docs` (Key, Text, Embedding, Category) VALUES
            (1u, "dogs only",          %s, "a"),
            (2u, "cats cats cats new", %s, "a"),
            (3u, "cats new",           %s, "b"),
            (4u, "birds only",         %s, "b");
    )sql", Emb(3).c_str(), Emb(1).c_str(), Emb(4).c_str(), Emb(2).c_str()));
}

void AddFulltextIndex(TQueryClient& db, const TString& table = "/Root/Docs", const TString& name = "ft_idx") {
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `%s` ADD INDEX %s
            GLOBAL USING fulltext_relevance
            ON (Text)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql", table.c_str(), name.c_str()));
}

void AddVectorIndex(TQueryClient& db, const TString& table = "/Root/Docs", const TString& name = "vec_idx") {
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `%s` ADD INDEX %s
            GLOBAL USING vector_kmeans_tree
            ON (Embedding)
            WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
    )sql", table.c_str(), name.c_str()));
}

void AddManhattanVectorIndex(TQueryClient& db, const TString& table = "/Root/Docs",
        const TString& name = "manhattan_idx") {
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `%s` ADD INDEX %s
            GLOBAL USING vector_kmeans_tree
            ON (Embedding)
            WITH (distance=manhattan, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
    )sql", table.c_str(), name.c_str()));
}

void AddEuclideanVectorIndex(TQueryClient& db, const TString& table = "/Root/Docs",
        const TString& name = "euclidean_idx") {
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `%s` ADD INDEX %s
            GLOBAL USING vector_kmeans_tree
            ON (Embedding)
            WITH (distance=euclidean, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
    )sql", table.c_str(), name.c_str()));
}

void AddInnerProductVectorIndex(TQueryClient& db, const TString& table = "/Root/Docs",
        const TString& name = "inner_product_idx") {
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `%s` ADD INDEX %s
            GLOBAL USING vector_kmeans_tree
            ON (Embedding)
            WITH (similarity=inner_product, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
    )sql", table.c_str(), name.c_str()));
}

// A prefixed vector index (a prefix column before the vector column). HybridRank does not support these
// yet (the kmeans-tree lowering needs an OptionalIf prefix predicate the rewrite doesn't build).
void AddPrefixedVectorIndex(TQueryClient& db, const TString& table = "/Root/Docs", const TString& name = "vp_idx") {
    ExecOk(db, Sprintf(R"sql(
        ALTER TABLE `%s` ADD INDEX %s
            GLOBAL USING vector_kmeans_tree
            ON (Category, Embedding)
            WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
    )sql", table.c_str(), name.c_str()));
}

// The standard fixture used by most tests: 4 docs with a fulltext and a (non-prefixed) vector index.
void SetupDocs(TQueryClient& db) {
    CreateDocs(db);
    UpsertDocs(db);
    AddFulltextIndex(db);
    AddVectorIndex(db);
}

void SetupMultiBranchDocs(TQueryClient& db) {
    ExecOk(db, R"sql(
        CREATE TABLE `/Root/MultiDocs` (
            Key Uint64,
            TextA Utf8,
            TextB Utf8,
            EmbeddingA String,
            EmbeddingB String,
            PRIMARY KEY (Key)
        );
    )sql");
    ExecOk(db, Sprintf(R"sql(
        UPSERT INTO `/Root/MultiDocs` (Key, TextA, TextB, EmbeddingA, EmbeddingB) VALUES
            (1u, "alpha alpha alpha", "plain", %s, %s),
            (2u, "plain", "beta beta beta", %s, %s),
            (3u, "alpha", "beta", %s, %s),
            (4u, "plain", "plain", %s, %s);
    )sql", Emb(1).c_str(), Emb(3).c_str(), Emb(2).c_str(), Emb(3).c_str(),
        Emb(3).c_str(), Emb(1).c_str(), Emb(4).c_str(), Emb(2).c_str()));
    ExecOk(db, R"sql(
        ALTER TABLE `/Root/MultiDocs` ADD INDEX ft_a
            GLOBAL USING fulltext_relevance
            ON (TextA)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql");
    ExecOk(db, R"sql(
        ALTER TABLE `/Root/MultiDocs` ADD INDEX ft_b
            GLOBAL USING fulltext_relevance
            ON (TextB)
            WITH (tokenizer=standard, use_filter_lowercase=true);
    )sql");
    ExecOk(db, R"sql(
        ALTER TABLE `/Root/MultiDocs` ADD INDEX vec_a
            GLOBAL USING vector_kmeans_tree
            ON (EmbeddingA)
            WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
    )sql");
    ExecOk(db, R"sql(
        ALTER TABLE `/Root/MultiDocs` ADD INDEX vec_b
            GLOBAL USING vector_kmeans_tree
            ON (EmbeddingB)
            WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
    )sql");
}

void RestartSchemeShard(TKikimrRunner& kikimr, const TString& path) {
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    runtime.Send(MakePipePerNodeCacheID(false), NActors::TActorId(),
        new TEvPipeCache::TEvForward(new TEvents::TEvPoisonPill(), TTestTxConfig::SchemeShard, false));
    Sleep(TDuration::Seconds(3));
    Tests::TClient::RefreshPathCache(&runtime, path);
}

// A real config-dispatcher update is delivered independently to the KQP proxy (which rebuilds the
// optimizer configuration used by workers) and the compile service (which invalidates cached plans when
// ShouldInvalidateCompileCache detects a relevant TableServiceConfig change). Deliver to both local
// subscribers and wait for their acknowledgements, exactly as the dispatcher would, so a stale cached
// HybridRank plan cannot mask the kill switch.
void UpdateHybridSearchConfig(TKikimrRunner& kikimr, bool enabled) {
    auto& runtime = *kikimr.GetTestServer().GetRuntime();
    const auto edgeActor = runtime.AllocateEdgeActor();

    NKikimrConfig::TAppConfig config;
    config.MutableFeatureFlags()->SetEnableFulltextIndex(true);
    auto* tableServiceConfig = config.MutableTableServiceConfig();
    tableServiceConfig->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    tableServiceConfig->SetEnableHybridSearch(enabled);

    for (const auto& service : {
            MakeKqpProxyID(runtime.GetNodeId()),
            MakeKqpCompileServiceID(runtime.GetNodeId())}) {
        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        *request->Record.MutableConfig() = config;
        runtime.Send(service, edgeActor, request.Release());
        auto response = runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(
            edgeActor, TDuration::Seconds(10));
        UNIT_ASSERT_C(response, "KQP service must acknowledge the runtime TableServiceConfig update");
    }
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

TString RunFailIssues(TQueryClient& db, const TString& sql) {
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.GetStatus() != EStatus::SUCCESS, "expected the query to fail, but it succeeded");
    return result.GetIssues().ToString();
}

} // namespace

Y_UNIT_TEST_SUITE(KqpHybridSearch) {

    // Pin the HybridSearch UDF contracts independently of the HybridRank optimizer. In particular,
    // weights are optional per branch: missing entries default to 1.0, while surplus entries are ignored.
    Y_UNIT_TEST(RrfUdfNumericEdges) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"sql(
            SELECT
                HybridSearch::RRF(
                    Cast([] AS List<Uint64>), Cast([] AS List<Double>), 60.0) AS Empty,
                HybridSearch::RRF(
                    Cast([1, 2] AS List<Uint64>), Cast([] AS List<Double>), 0.0) AS Defaults,
                HybridSearch::RRF(
                    Cast([1, 2] AS List<Uint64>), Cast([2.0] AS List<Double>), 0.0) AS ShortWeights,
                HybridSearch::RRF(
                    Cast([1, 2] AS List<Uint64>), Cast([2.0, 3.0, 100.0] AS List<Double>), 0.0) AS LongWeights,
                HybridSearch::RRF(
                    Cast([2, 3] AS List<Uint64>), Cast([] AS List<Double>), -1.0) AS NegativeK,
                HybridSearch::RRF(
                    Cast([1] AS List<Uint64>), Cast([1e300] AS List<Double>), 0.0) AS LargeFiniteWeight,
                HybridSearch::RRF(
                    Cast([1] AS List<Uint64>), Cast([1.0] AS List<Double>), -0.999) AS NearZeroDenominator;
        )sql", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("Empty").GetDouble(), 0.0, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("Defaults").GetDouble(), 1.5, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("ShortWeights").GetDouble(), 2.5, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("LongWeights").GetDouble(), 3.5, 1e-12);
        // The UDF deliberately performs the formula as supplied; it does not validate K. Use ranks that
        // avoid a zero denominator so this test records that contract without pinning infinity/NaN output.
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("NegativeK").GetDouble(), 1.5, 1e-12);
        UNIT_ASSERT_C(std::isfinite(parser.ColumnParser("LargeFiniteWeight").GetDouble()),
            "a representable large weight must remain finite");
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("NearZeroDenominator").GetDouble(), 1000.0, 1e-7);
    }

    // Cover min-max normalization, raw fusion and all parallel-list length rules directly at the UDF
    // boundary. A zero/negative span contributes zero; omitted weights/similarity flags use their defaults.
    Y_UNIT_TEST(LinearFuseUdfNumericEdges) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"sql(
            SELECT
                HybridSearch::LinearFuse(
                    Cast([] AS List<Double>), Cast([] AS List<Double>), Cast([] AS List<Double>),
                    Cast([] AS List<Double>), Cast([] AS List<Bool>), true) AS Empty,
                HybridSearch::LinearFuse(
                    Cast([2.0, 2.0] AS List<Double>), Cast([0.0, 0.0] AS List<Double>),
                    Cast([10.0, 10.0] AS List<Double>), Cast([] AS List<Double>),
                    Cast([true, false] AS List<Bool>), true) AS SimilarityAndDistance,
                HybridSearch::LinearFuse(
                    Cast([5.0, 7.0] AS List<Double>), Cast([5.0, 9.0] AS List<Double>),
                    Cast([5.0, 8.0] AS List<Double>), Cast([100.0, 100.0] AS List<Double>),
                    Cast([true, false] AS List<Bool>), true) AS NonPositiveSpans,
                HybridSearch::LinearFuse(
                    Cast([2.0, 3.0] AS List<Double>), Cast([] AS List<Double>),
                    Cast([] AS List<Double>), Cast([] AS List<Double>),
                    Cast([] AS List<Bool>), false) AS RawDefaults,
                HybridSearch::LinearFuse(
                    Cast([2.0, 3.0] AS List<Double>), Cast([] AS List<Double>),
                    Cast([] AS List<Double>), Cast([2.0] AS List<Double>),
                    Cast([true, false] AS List<Bool>), false) AS ShortWeights,
                HybridSearch::LinearFuse(
                    Cast([2.0] AS List<Double>), Cast([] AS List<Double>),
                    Cast([] AS List<Double>), Cast([3.0, 100.0] AS List<Double>),
                    Cast([true, false] AS List<Bool>), false) AS LongParallelLists,
                HybridSearch::LinearFuse(
                    Cast([1e150] AS List<Double>), Cast([] AS List<Double>), Cast([] AS List<Double>),
                    Cast([1e150] AS List<Double>), Cast([true] AS List<Bool>), false) AS LargeFiniteRaw;
        )sql", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("Empty").GetDouble(), 0.0, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("SimilarityAndDistance").GetDouble(), 1.0, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("NonPositiveSpans").GetDouble(), 0.0, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("RawDefaults").GetDouble(), 5.0, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("ShortWeights").GetDouble(), 1.0, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("LongParallelLists").GetDouble(), 6.0, 1e-12);
        UNIT_ASSERT_C(std::isfinite(parser.ColumnParser("LargeFiniteRaw").GetDouble()),
            "representable raw score and weight multiplication must remain finite");
    }

    Y_UNIT_TEST(UdfsRejectNonFiniteInputsAndResults) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();

        const auto assertFails = [&](const TString& expression, double value, TStringBuf issue) {
            const TString query = TStringBuilder()
                << "DECLARE $value AS Double; SELECT " << expression << " AS Value;";
            auto params = TParamsBuilder().AddParam("$value").Double(value).Build().Build();
            auto result = db.ExecuteQuery(query, TTxControl::NoTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() != EStatus::SUCCESS, "non-finite hybrid input must be rejected");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), issue);
        };

        assertFails(
            "HybridSearch::RRF(Cast([1] AS List<Uint64>), Cast([1.0] AS List<Double>), $value)",
            std::numeric_limits<double>::quiet_NaN(), "finite K");
        assertFails(
            "HybridSearch::RRF(Cast([1] AS List<Uint64>), Cast([1.0] AS List<Double>), $value)",
            -std::numeric_limits<double>::infinity(), "finite K");
        assertFails(
            "HybridSearch::RRF(Cast([1] AS List<Uint64>), Cast([$value] AS List<Double>), 60.0)",
            std::numeric_limits<double>::infinity(), "finite weights");
        assertFails(
            "HybridSearch::RRF(Cast([1] AS List<Uint64>), Cast([1.0] AS List<Double>), $value)",
            -1.0, "non-zero K + rank");
        assertFails(
            "HybridSearch::RRF(Cast([1, 1] AS List<Uint64>), "
            "Cast([$value, $value] AS List<Double>), 0.0)",
            std::numeric_limits<double>::max(), "non-finite result");
        assertFails(
            "HybridSearch::LinearFuse(Cast([$value] AS List<Double>), Cast([0.0] AS List<Double>), "
            "Cast([1.0] AS List<Double>), Cast([1.0] AS List<Double>), Cast([true] AS List<Bool>), true)",
            std::numeric_limits<double>::quiet_NaN(), "finite scores");
        assertFails(
            "HybridSearch::LinearFuse(Cast([1.0] AS List<Double>), Cast([0.0] AS List<Double>), "
            "Cast([1.0] AS List<Double>), Cast([$value] AS List<Double>), Cast([true] AS List<Bool>), false)",
            std::numeric_limits<double>::infinity(), "finite scores");
        assertFails(
            "HybridSearch::LinearFuse(Cast([1.0] AS List<Double>), Cast([-$value] AS List<Double>), "
            "Cast([$value] AS List<Double>), Cast([1.0] AS List<Double>), Cast([true] AS List<Bool>), true)",
            std::numeric_limits<double>::max(), "finite normalization span");
        assertFails(
            "HybridSearch::LinearFuse(Cast([$value] AS List<Double>), Cast([] AS List<Double>), "
            "Cast([] AS List<Double>), Cast([$value] AS List<Double>), Cast([true] AS List<Bool>), false)",
            std::numeric_limits<double>::max(), "non-finite contribution");
        assertFails(
            "HybridSearch::LinearFuse(Cast([$value, $value] AS List<Double>), Cast([] AS List<Double>), "
            "Cast([] AS List<Double>), Cast([1.0, 1.0] AS List<Double>), "
            "Cast([true, true] AS List<Bool>), false)",
            std::numeric_limits<double>::max(), "non-finite result");
    }

    // ListMap produces computed lists rather than passing list literals directly. This exercises the UDFs'
    // iterator path, which is separate from GetElements() used for compact literal lists.
    Y_UNIT_TEST(UdfsAcceptIteratorBackedLists) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"sql(
            $ranks = ListMap(Cast([1, 2] AS List<Uint64>), ($x) -> { RETURN $x; });
            $weights = ListMap(Cast([2.0, 3.0] AS List<Double>), ($x) -> { RETURN $x; });
            $scores = ListMap(Cast([2.0, 3.0] AS List<Double>), ($x) -> { RETURN $x; });
            $mins = ListMap(Cast([0.0, 0.0] AS List<Double>), ($x) -> { RETURN $x; });
            $maxs = ListMap(Cast([10.0, 10.0] AS List<Double>), ($x) -> { RETURN $x; });
            $similarities = ListMap(Cast([true, true] AS List<Bool>), ($x) -> { RETURN $x; });

            SELECT
                HybridSearch::RRF($ranks, $weights, 0.0) AS Rrf,
                HybridSearch::LinearFuse(
                    $scores, $mins, $maxs, $weights, $similarities, true) AS Linear;
        )sql", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("Rrf").GetDouble(), 3.5, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(parser.ColumnParser("Linear").GetDouble(), 1.3, 1e-12);
    }

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

    // Branch classification and index resolution are per scoring expression, rather than restricted to
    // one branch of each kind. Use distinct columns and indexes so this cannot accidentally pass by
    // reading the same implementation twice. Documents 1 and 3 overlap the two text branches; the final
    // union must still contain each primary key exactly once.
    Y_UNIT_TEST(TwoDistinctFulltextBranches) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupMultiBranchDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/MultiDocs`
            ORDER BY HybridRank(
                FullTextScore(TextA, "alpha"),
                FullTextScore(TextB, "beta"),
                Knn::CosineDistance(EmbeddingA, $target),
                (4, 4, 4) AS Limits)
            LIMIT 10;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL_C(keys.size(), 4u,
            "overlapping candidates from two distinct fulltext indexes must be deduplicated");
        UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
            "two fulltext branches and a vector branch must produce their complete candidate union");
        UNIT_ASSERT_VALUES_EQUAL_C(keys.front(), 3u,
            "the document present in both fulltext branches must lead the RRF result");
    }

    // Likewise, two vector branches may target different columns and therefore different kmeans-tree
    // indexes. The text branch overlaps their candidate pools; assert the deduplicated union and avoid
    // depending on approximate-vector tie order.
    Y_UNIT_TEST(TwoDistinctVectorBranches) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupMultiBranchDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/MultiDocs`
            ORDER BY HybridRank(
                FullTextScore(TextA, "alpha"),
                Knn::CosineDistance(EmbeddingA, $target),
                Knn::CosineDistance(EmbeddingB, $target),
                (4, 4, 4) AS Limits)
            LIMIT 10;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL_C(keys.size(), 4u,
            "overlapping candidates from two distinct vector indexes must be deduplicated");
        UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
            "two vector branches and a fulltext branch must produce their complete candidate union");
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

    // OFFSET is applied after the fused score order, and LIMIT+OFFSET must retain enough branch
    // candidates to produce a full page. Explicit branch limits make the expected page independent of
    // HybridSearchFactor and the approximate kmeans candidate-pool sizing.
    Y_UNIT_TEST(RespectsOffsetAfterFusion) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto offsetOnly = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                (4, 4) AS Limits)
            LIMIT 100 OFFSET 1;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{3u, 2u, 4u}), offsetOnly);

        auto page = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                (4, 4) AS Limits)
            LIMIT 2 OFFSET 1;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{3u, 2u}), page);
    }

    // Nullable indexed columns are absent from the corresponding index branch, but remain eligible via
    // another non-NULL branch. Key 5 has only a vector value and key 6 only a fulltext value; both must
    // survive fusion, while a row that is NULL in both branches contributes no candidate.
    Y_UNIT_TEST(NullableScoredColumnsUseOtherBranch) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);
        ExecOk(db, Sprintf(R"sql(
            UPSERT INTO `/Root/Docs` (Key, Text, Embedding, Category) VALUES
                (5u, NULL, %s, "nullable"),
                (6u, "cats nullable", NULL, "nullable"),
                (7u, NULL, NULL, "nullable");
        )sql", Emb(2).c_str()));

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                (10, 10) AS Limits)
            LIMIT 10;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL_C(keys.size(), 6u,
            "rows present in at least one nullable branch must occur once in the fused union");
        UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u, 5u, 6u}),
            "a NULL in one scored column must not hide a candidate supplied by another branch");
    }

    // A branch may legitimately have no candidates. The fusion result must then be exactly the other
    // branch rather than becoming empty or producing invalid normalization values. Both built-in modes
    // recover the pure vector order: doc2 exact, then doc1, doc4 and the opposite-direction doc3.
    Y_UNIT_TEST(EmptyBranchFallsBackToCandidateBranch) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        for (const TString& mode : {TString("rrf"), TString("linear")}) {
            auto keys = RunKeys(db, TargetDecl + Sprintf(R"sql(
                SELECT Key FROM `/Root/Docs`
                ORDER BY HybridRank(
                    FullTextScore(Text, "term-that-is-not-present"),
                    Knn::CosineDistance(Embedding, $target),
                    "%s" AS Mode,
                    (4, 4) AS Limits)
                LIMIT 4;
            )sql", mode.c_str()));
            UNIT_ASSERT_VALUES_EQUAL_C((std::vector<ui64>{2u, 1u, 4u, 3u}), keys,
                TStringBuilder() << "an empty fulltext branch must leave the pure vector order in " << mode);
        }
    }

    // Explicit per-branch Limits control the candidate union independently of the final LIMIT. With a
    // one-row fulltext pool (doc3 for "sleep") and a two-row vector pool ({2,1}) the branches are disjoint;
    // changing the term to "cats" makes doc1 overlap and therefore deduplicates the union to two rows.
    Y_UNIT_TEST(ExplicitLimitsCoverDisjointAndOverlappingBranches) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto disjoint = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "sleep"),
                Knn::CosineDistance(Embedding, $target),
                (1, 2) AS Limits)
            LIMIT 10;
        )sql");
        UNIT_ASSERT_C((std::set<ui64>(disjoint.begin(), disjoint.end()) == std::set<ui64>{1u, 2u, 3u}),
            "disjoint one- and two-candidate branches must produce their complete three-row union");

        auto overlapping = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                (1, 2) AS Limits)
            LIMIT 10;
        )sql");
        UNIT_ASSERT_C((std::set<ui64>(overlapping.begin(), overlapping.end()) == std::set<ui64>{1u, 2u}),
            "the document present in both truncated branches must occur only once in the fused union");
    }

    // The optimizer derives each default branch pool as final LIMIT * HybridSearchFactor. Compare that
    // path with the equivalent explicit Limits at the smallest useful factor and at a factor larger than
    // the corpus. This checks both truncation and saturation without depending on approximate tie order.
    Y_UNIT_TEST(HybridSearchFactorMatchesExplicitCandidateLimits) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        const auto runWithFactor = [&](ui64 factor) {
            return RunKeys(db, SearchPragma + "pragma ydb.HybridSearchFactor = \"" + ToString(factor) + "\";\n"
                + TargetExpr + R"sql(
                    SELECT Key FROM `/Root/Docs`
                    ORDER BY HybridRank(
                        FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
                    LIMIT 2;
                )sql");
        };
        const auto runExplicit = [&](ui64 branchLimit) {
            const TString limit = ToString(branchLimit);
            return RunKeys(db, TargetDecl + Sprintf(R"sql(
                    SELECT Key FROM `/Root/Docs`
                    ORDER BY HybridRank(
                        FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                        (%s, %s) AS Limits)
                    LIMIT 2;
                )sql", limit.c_str(), limit.c_str()));
        };

        UNIT_ASSERT_VALUES_EQUAL(runExplicit(2), runWithFactor(1));
        UNIT_ASSERT_VALUES_EQUAL(runExplicit(200), runWithFactor(100));
    }

    // Boundary final limits are independent from branch Limits: zero emits no rows, one emits exactly one
    // valid candidate, and a limit larger than the corpus emits the complete deduplicated candidate union.
    Y_UNIT_TEST(FinalLimitBoundaries) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto zero = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                (4, 4) AS Limits)
            LIMIT 0;
        )sql");
        UNIT_ASSERT_C(zero.empty(), "LIMIT 0 must return no fused rows");

        auto one = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                (4, 4) AS Limits)
            LIMIT 1;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL_C(one.size(), 1u, "LIMIT 1 must return exactly one fused row");
        UNIT_ASSERT_C((std::set<ui64>{1u, 2u, 3u, 4u}.contains(one.front())),
            "LIMIT 1 must return a member of the candidate union");

        auto aboveCorpus = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                (10, 10) AS Limits)
            LIMIT 100;
        )sql");
        UNIT_ASSERT_C((std::set<ui64>(aboveCorpus.begin(), aboveCorpus.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
            "a final limit above the corpus must return the complete candidate union");
        UNIT_ASSERT_VALUES_EQUAL_C(aboveCorpus.size(), 4u, "the candidate union must stay deduplicated");
    }

    // Manhattan distance exercises metric-aware vector-index resolution beyond the cosine distance and
    // similarity paths. For this fixture it has the same strict nearest-neighbour order as cosine.
    Y_UNIT_TEST(ManhattanDistanceFuses) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddFulltextIndex(db);
        AddManhattanVectorIndex(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "term-that-is-not-present"),
                Knn::ManhattanDistance(Embedding, $target),
                (4, 4) AS Limits)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{2u, 1u, 4u, 3u}), keys);
    }

    // Cover the remaining metrics accepted by vector_kmeans_tree. Euclidean is a distance (ascending),
    // while inner product is a similarity (descending); HybridRank must resolve both index shapes and
    // normalize their direction. The fixture has a strict metric order, so no tie ordering is involved.
    Y_UNIT_TEST_TWIN(EuclideanAndInnerProductFuses, InnerProduct) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        CreateDocs(db);
        UpsertDocs(db);
        AddFulltextIndex(db);
        if (InnerProduct) {
            AddInnerProductVectorIndex(db);
        } else {
            AddEuclideanVectorIndex(db);
        }

        const TString function = InnerProduct ? "Knn::InnerProductSimilarity" : "Knn::EuclideanDistance";
        auto keys = RunKeys(db, TargetDecl + Sprintf(R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "term-that-is-not-present"),
                %s(Embedding, $target),
                (4, 4) AS Limits)
            LIMIT 4;
        )sql", function.c_str()));
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{2u, 1u, 4u, 3u}), keys);
    }

    // Equal fused scores intentionally have no secondary-order contract. Assert only completeness and
    // uniqueness; pinning a key sequence here would make the test depend on hash/join emission order.
    Y_UNIT_TEST(EqualScoresReturnCompleteUnorderedCandidateSet) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                (0, 0) AS Weights,
                (4, 4) AS Limits)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 4u);
        UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
            "equal scores must retain the complete deduplicated union, in unspecified order");
    }

    // A very large but finite branch weight remains supported. Zeroing the vector branch makes the
    // expected leader independent of the approximate vector order while exercising weight parsing at a
    // magnitude where accidental narrowing or integer conversion would be visible.
    Y_UNIT_TEST(LargeFiniteWeightKeepsFulltextLeader) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                (1e300, 0) AS Weights,
                (4, 4) AS Limits)
            LIMIT 1;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u}), keys);
    }

    // HybridRank must remain the complete ORDER BY key. A secondary key would otherwise be misleading:
    // the hybrid rewrite ranks its own candidate stream and cannot promise a stable tie-break contract.
    Y_UNIT_TEST(RejectsSecondaryOrderByKey) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto issues = RunFailIssues(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target),
                (0, 0) AS Weights), Key
            LIMIT 4;
        )sql");
        UNIT_ASSERT_STRING_CONTAINS(issues, "must be the entire ORDER BY key");
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

    Y_UNIT_TEST_TWIN(SnapshotConsistencyAcrossConcurrentBranchReads, Compact) {
        // Fake threads let the test stop exactly on TEvRead boundaries. Stream-index writes are
        // required for online maintenance of the compact fulltext layout; the legacy twin uses the
        // same path so the only variable is the relevance storage format.
        auto kikimr = MakeRunner(
            /*enableHybridSearch=*/true,
            /*enableCompactFulltextIndex=*/Compact,
            /*enableIndexStreamWrite=*/true,
            /*useRealThreads=*/false);
        auto db = kikimr.GetQueryClient();
        kikimr.RunCall([&] {
            SetupDocs(db);
            return true;
        });

        auto hybridSession = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });
        auto writerSession = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        const TString query = TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                (4, 4) AS Limits)
            LIMIT 4;
        )sql";
        auto parseHybrid = [&](TExecuteQueryResult result) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            std::vector<ui64> keys;
            TResultSetParser parser(result.GetResultSet(0));
            while (parser.TryNextRow()) {
                keys.push_back(*parser.ColumnParser("Key").GetOptionalUint64());
            }
            return keys;
        };
        auto executeHybrid = [&](TSession& session) {
            return parseHybrid(session.ExecuteQuery(
                query, TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync());
        };

        const auto oldKeys = kikimr.RunCall([&] { return executeHybrid(hybridSession); });
        kikimr.RunCall([&] {
            UpsertNewSnapshotDocs(db);
            return true;
        });
        const auto newKeys = kikimr.RunCall([&] { return executeHybrid(hybridSession); });
        UNIT_ASSERT_VALUES_UNEQUAL_C(oldKeys, newKeys,
            "controlled old/new corpora must have distinguishable hybrid orders");
        kikimr.RunCall([&] {
            UpsertDocs(db);
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(oldKeys, kikimr.RunCall([&] { return executeHybrid(hybridSession); }));

        const auto edge = runtime.AllocateEdgeActor();
        auto pathId = [&](const TString& path) {
            return DescribeTable(&kikimr.GetTestServer(), edge, path).GetPathId();
        };
        const THashSet<ui64> fulltextPathIds = {
            pathId("/Root/Docs/ft_idx/indexImplTable"),
        };
        const THashSet<ui64> vectorPathIds = {
            pathId("/Root/Docs/vec_idx/indexImplPostingTable"),
        };

        std::vector<std::unique_ptr<IEventHandle>> blockedVectorReads;
        bool allowVectorReads = false;
        bool observeHybridReads = true;
        size_t fulltextReads = 0;
        size_t vectorReads = 0;
        auto observer = [&](TAutoPtr<IEventHandle>& event) {
            if (observeHybridReads && event->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
                auto* read = event->Get<TEvDataShard::TEvRead>();
                const ui64 tableId = read->Record.GetTableId().GetTableId();
                if (fulltextPathIds.contains(tableId)) {
                    ++fulltextReads;
                } else if (vectorPathIds.contains(tableId)) {
                    ++vectorReads;
                    if (!allowVectorReads) {
                        blockedVectorReads.emplace_back(event.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto previousObserver = runtime.SetObserverFunc(observer);
        Y_DEFER {
            runtime.SetObserverFunc(previousObserver);
            for (auto& event : blockedVectorReads) {
                if (event) {
                    runtime.Send(event.release());
                }
            }
        };

        auto future = kikimr.RunInThreadPool([&] {
            return db.ExecuteQuery(
                query, TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).ExtractValueSync();
        });
        // Both branches belong to one execution stage, so waiting for a complete fulltext result while
        // a vector posting read is blocked deadlocks that stage. The closest deterministic boundary is
        // after both mutable posting reads have been emitted but before the captured vector read reaches
        // DataShard. SnapshotRO also uses different wire markers for the two implementations, therefore
        // the observable old/new result oracle below is the authoritative consistency check.
        TDispatchOptions dispatch;
        dispatch.FinalEvents.emplace_back([&](IEventHandle&) {
            return !blockedVectorReads.empty() && fulltextReads > 0;
        });
        runtime.DispatchEvents(dispatch);
        UNIT_ASSERT_C(!blockedVectorReads.empty(), "vector branch read must reach the deterministic barrier");
        UNIT_ASSERT_C(fulltextReads > 0 && vectorReads > 0,
            "both hybrid posting branches must reach the observed read boundary");

        // Let writer-internal reads pass, but retain the already captured vector request. The write
        // atomically changes both indexes and the main row versions while the hybrid snapshot is pinned.
        allowVectorReads = true;
        observeHybridReads = false;
        auto writeResult = kikimr.RunCall([&] {
            return writerSession.ExecuteQuery(
                Sprintf(R"sql(
                    UPSERT INTO `/Root/Docs` (Key, Text, Embedding, Category) VALUES
                        (1u, "dogs only",          %s, "a"),
                        (2u, "cats cats cats new", %s, "a"),
                        (3u, "cats new",           %s, "b"),
                        (4u, "birds only",         %s, "b");
                )sql", Emb(3).c_str(), Emb(1).c_str(), Emb(4).c_str(), Emb(2).c_str()),
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        });
        UNIT_ASSERT_VALUES_EQUAL_C(writeResult.GetStatus(), EStatus::SUCCESS, writeResult.GetIssues().ToString());

        for (auto& event : blockedVectorReads) {
            runtime.Send(event.release());
        }
        const auto interleavedKeys = parseHybrid(runtime.WaitFuture(future));
        UNIT_ASSERT_VALUES_EQUAL_C(interleavedKeys, oldKeys,
            "a query whose fulltext branch started before DML must not mix in the new vector/index version");
        UNIT_ASSERT_VALUES_EQUAL(newKeys, kikimr.RunCall([&] { return executeHybrid(hybridSession); }));
    }

    // EnableCompactFulltextIndex changes fulltext_relevance into the compact relevance layout. Hybrid
    // auto-detection must treat that index as a relevance index just like the legacy layout and lower the
    // fulltext branch through its compact implementation tables. Exercise both built-in fusion paths: RRF
    // and normalized linear fusion.
    Y_UNIT_TEST(CompactRelevanceAutoDetection) {
        auto kikimr = MakeRunner(/*enableHybridSearch=*/true, /*enableCompactFulltextIndex=*/true);
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        for (const TString& mode : {TString("rrf"), TString("linear")}) {
            auto keys = RunKeys(db, TargetDecl + Sprintf(R"sql(
                SELECT Key FROM `/Root/Docs`
                ORDER BY HybridRank(
                    FullTextScore(Text, "cats"),
                    Knn::CosineDistance(Embedding, $target),
                    "%s" AS Mode)
                LIMIT 4;
            )sql", mode.c_str()));
            UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
                TStringBuilder() << "compact relevance auto-detection (" << mode
                    << ") must fuse the full candidate union");
            UNIT_ASSERT_C(keys[0] == 1u || keys[0] == 3u,
                TStringBuilder() << "a text-relevant doc must lead with compact relevance auto-detection ("
                    << mode << ")");
        }
    }

    // Explicit AS Indexes is also required to accept a compact relevance index. Besides covering the
    // explicit resolution path, this guards disambiguation in deployments that have more than one index
    // over the text column.
    Y_UNIT_TEST(CompactRelevanceNamedIndex) {
        auto kikimr = MakeRunner(/*enableHybridSearch=*/true, /*enableCompactFulltextIndex=*/true);
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        for (const TString& mode : {TString("rrf"), TString("linear")}) {
            auto keys = RunKeys(db, TargetDecl + Sprintf(R"sql(
                SELECT Key FROM `/Root/Docs`
                ORDER BY HybridRank(
                    FullTextScore(Text, "cats"),
                    Knn::CosineDistance(Embedding, $target),
                    "%s" AS Mode,
                    ("ft_idx", "vec_idx") AS Indexes)
                LIMIT 4;
            )sql", mode.c_str()));
            UNIT_ASSERT_C((std::set<ui64>(keys.begin(), keys.end()) == std::set<ui64>{1u, 2u, 3u, 4u}),
                TStringBuilder() << "explicit compact relevance index (" << mode
                    << ") must fuse the full candidate union");
            UNIT_ASSERT_C(keys[0] == 1u || keys[0] == 3u,
                TStringBuilder() << "a text-relevant doc must lead with an explicit compact relevance index ("
                    << mode << ")");
        }
    }

    // Custom rank fusion must receive ranks produced by a compact relevance branch in exactly the same
    // slots as a legacy relevance branch. Reproducing the built-in RRF formula pins the complete result,
    // rather than merely checking that the compact index can be resolved.
    Y_UNIT_TEST(CompactRelevanceRankLambda) {
        auto kikimr = MakeRunner(/*enableHybridSearch=*/true, /*enableCompactFulltextIndex=*/true);
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ($ranks) -> {
                    RETURN 1.0 / (60 + COALESCE($ranks[0], 100000))
                         + 1.0 / (60 + COALESCE($ranks[1], 100000));
                } AS RankLambda)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 3u, 2u, 4u}), keys);
    }

    // ScoreLambda gets raw BM25 values from the compact relevance implementation. Selecting the text
    // score alone makes the observable result independent of vector-score magnitudes: doc 1 (three
    // occurrences) must lead doc 3 (one), and both must precede documents absent from the text branch.
    Y_UNIT_TEST(CompactRelevanceScoreLambda) {
        auto kikimr = MakeRunner(/*enableHybridSearch=*/true, /*enableCompactFulltextIndex=*/true);
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target),
                ($scores) -> {
                    RETURN COALESCE($scores[0], -1.0);
                } AS ScoreLambda)
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL_C(keys[0], 1u,
            "compact BM25 score must put the document with three occurrences first");
        UNIT_ASSERT_VALUES_EQUAL_C(keys[1], 3u,
            "the other compact fulltext match must precede documents absent from the text branch");
        UNIT_ASSERT_C((std::set<ui64>{keys[2], keys[3]} == std::set<ui64>{2u, 4u}),
            "documents absent from the compact fulltext branch must occupy the final positions");
    }

    // SchemeShard owns the index metadata used by hybrid index resolution. Restart it after both compact
    // relevance and vector indexes are ready, refresh the scheme cache, and compile the HybridRank query
    // through a fresh SDK client. This guards recovery of the compact index type and implementation-table
    // metadata rather than relying on metadata retained by the client that created the indexes.
    Y_UNIT_TEST(CompactRelevanceAfterSchemeShardRestart) {
        auto kikimr = MakeRunner(/*enableHybridSearch=*/true, /*enableCompactFulltextIndex=*/true);
        {
            auto setupDb = kikimr.GetQueryClient();
            SetupDocs(setupDb);
        }

        RestartSchemeShard(kikimr, "/Root/Docs");

        auto db = kikimr.GetQueryClient();
        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(
                FullTextScore(Text, "cats"),
                Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 3u, 2u, 4u}), keys);
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

    // Exercise the dynamic kill switch in one running cluster and through the same SDK client/query text.
    // Execute twice before the update so the second execution can use the compile cache. The notification
    // to the compile service must clear that cached successful plan: otherwise the disabled query would
    // keep executing and bypass the optimizer-side flag check. Re-enabling must make a fresh compilation
    // and restore the original result.
    Y_UNIT_TEST(DynamicFlagInvalidatesCompileCache) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        const TString query = TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql";
        const std::vector<ui64> expected{1u, 3u, 2u, 4u};
        UNIT_ASSERT_VALUES_EQUAL(expected, RunKeys(db, query));
        UNIT_ASSERT_VALUES_EQUAL(expected, RunKeys(db, query));

        UpdateHybridSearchConfig(kikimr, /*enabled=*/false);
        UNIT_ASSERT_STRING_CONTAINS(RunFailIssues(db, query), "hybrid search is disabled");

        UpdateHybridSearchConfig(kikimr, /*enabled=*/true);
        UNIT_ASSERT_VALUES_EQUAL(expected, RunKeys(db, query));
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

    // Prefixed vector indexes are not supported yet: auto-detect skips them; an explicit reference errors.
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

        // Naming it explicitly reports the unsupported shape precisely.
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
