#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

TKikimrRunner MakeRunner(bool enableHybridSearch = true) {
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
    Y_UNIT_TEST(FusesBothBranches) {
        auto kikimr = MakeRunner();
        auto db = kikimr.GetQueryClient();
        SetupDocs(db);

        auto keys = RunKeys(db, TargetDecl + R"sql(
            SELECT Key FROM `/Root/Docs`
            ORDER BY HybridRank(FullTextScore(Text, "cats"), Knn::CosineDistance(Embedding, $target))
            LIMIT 4;
        )sql");
        UNIT_ASSERT_VALUES_EQUAL((std::vector<ui64>{1u, 3u, 2u, 4u}), keys);
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

    Y_UNIT_TEST(NamedIndexesDisambiguate) {
        auto kikimr = MakeRunner();
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

    // Note: the composite-primary-key guard in the optimizer is defensive only. A fulltext-relevance
    // index cannot be created on a composite-PK table at all, so a hybrid query never reaches it
    // (auto-detect fails to find a fulltext index first); there is no valid setup to exercise it here.
}

} // namespace NKikimr::NKqp
