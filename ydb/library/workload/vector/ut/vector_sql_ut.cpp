#include <ydb/library/workload/vector/vector_sql.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYdbWorkload {
Y_UNIT_TEST_SUITE(VectorWorkloadSql) {
    Y_UNIT_TEST(HnswOptionsGenerateDdlAndQueryPragma) {
        TVectorWorkloadParams params;
        NLastGetopt::TOpts opts;
        params.ConfigureIndexOpts(opts);
        const char* args[] = {"ut", "--index-type", "hnsw", "--min-rows", "1", "--M", "24",
                              "--ef-construction", "100", "--delta-rows", "4294967296"};
        for (auto parser = NLastGetopt::TOptsParser(&opts, sizeof(args) / sizeof(*args), args); parser.Next();) {
        }
        UNIT_ASSERT_VALUES_EQUAL(params.GetIndexTypeDDL(), "hnsw");
        const auto ddl = params.GetHnswSettingsDDL();
        UNIT_ASSERT_STRING_CONTAINS(ddl, "min_rows=1");
        UNIT_ASSERT_STRING_CONTAINS(ddl, "M=24");
        UNIT_ASSERT_STRING_CONTAINS(ddl, "ef_construction=100");
        UNIT_ASSERT_STRING_CONTAINS(ddl, "delta_rows=4294967296");
        UNIT_ASSERT(ddl.find("hnsw_search_candidates") == TString::npos);
        params.TableOpts.Name = "vectors";
        params.KeyColumns = {"id"};
        params.EmbeddingColumn = "embedding";
        params.Metric = NYdb::NTable::TVectorIndexSettings::EMetric::InnerProduct;
        params.Hnsw = true;
        UNIT_ASSERT_STRING_CONTAINS(MakeSelect(params, "ann"), "PRAGMA ydb.HNSWEfSearch=\"15\"");
        params.HnswEfSearch = 50;
        UNIT_ASSERT_STRING_CONTAINS(MakeSelect(params, "ann"), "PRAGMA ydb.HNSWEfSearch=\"50\"");
    }

    Y_UNIT_TEST(HnswUsesIndexViewAndBruteForceBaseline) {
        TVectorWorkloadParams params;
        params.TableOpts.Name = "vectors";
        params.KeyColumns = {"id"};
        params.EmbeddingColumn = "embedding";
        params.Metric = NYdb::NTable::TVectorIndexSettings::EMetric::InnerProduct;
        params.Hnsw = true;
        const auto indexed = MakeSelect(params, "ann");
        UNIT_ASSERT_STRING_CONTAINS(indexed, "FROM `vectors`\nVIEW ann");
        UNIT_ASSERT_STRING_CONTAINS(indexed, "Knn::InnerProductSimilarity");
        UNIT_ASSERT(indexed.find("indexImplPostingTable") == std::string::npos);
        const auto baseline = MakeSelect(params, "");
        UNIT_ASSERT_STRING_CONTAINS(baseline, "FROM `vectors`");
        UNIT_ASSERT(baseline.find("VIEW") == std::string::npos);
        UNIT_ASSERT(baseline.find("indexImplPostingTable") == std::string::npos);
    }

    Y_UNIT_TEST(KmeansAndPrefixedHnswRetainIndexView) {
        TVectorWorkloadParams params;
        params.TableOpts.Name = "vectors";
        params.KeyColumns = {"id"};
        params.EmbeddingColumn = "embedding";
        params.Metric = NYdb::NTable::TVectorIndexSettings::EMetric::Euclidean;
        UNIT_ASSERT_STRING_CONTAINS(MakeSelect(params, "ann"), "VIEW ann");
        UNIT_ASSERT(MakeSelect(params, "ann").find("HNSWEfSearch") == std::string::npos);
        params.Hnsw = true;
        params.PrefixColumn = "category";
        params.PrefixType = "Uint64";
        const auto query = MakeSelect(params, "ann");
        UNIT_ASSERT_STRING_CONTAINS(query, "VIEW ann");
        UNIT_ASSERT_STRING_CONTAINS(query, "WHERE category = $PrefixValue");
        UNIT_ASSERT(query.find("indexImplPostingTable") == std::string::npos);
    }
}
}
