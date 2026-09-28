#include <ydb/library/workload/vector/vector_sql.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYdbWorkload {
Y_UNIT_TEST_SUITE(VectorWorkloadSql) {
    Y_UNIT_TEST(HnswFullPartitionsAndBruteForceBaseline) {
        TVectorWorkloadParams params;
        params.TableOpts.Name = "vectors";
        params.KeyColumns = {"id"};
        params.EmbeddingColumn = "embedding";
        params.Metric = NYdb::NTable::TVectorIndexSettings::EMetric::InnerProduct;
        params.DistributedHnsw = true;
        params.HnswFullRange = true;
        params.HnswFullRange = false;
        UNIT_ASSERT_STRING_CONTAINS(MakeSelect(params, "ann"), "VIEW ann");
        params.HnswFullRange = true;
        const auto indexed = MakeSelect(params, "ann");
        UNIT_ASSERT_STRING_CONTAINS(indexed, "FROM `vectors/ann/indexImplPostingTable`");
        UNIT_ASSERT_STRING_CONTAINS(indexed, "Knn::InnerProductSimilarity");
        UNIT_ASSERT(indexed.find("VIEW") == std::string::npos);
        const auto baseline = MakeSelect(params, "");
        UNIT_ASSERT_STRING_CONTAINS(baseline, "FROM `vectors`");
        UNIT_ASSERT(baseline.find("indexImplPostingTable") == std::string::npos);
    }

    Y_UNIT_TEST(KmeansAndPrefixedHnswRetainIndexView) {
        TVectorWorkloadParams params;
        params.TableOpts.Name = "vectors";
        params.KeyColumns = {"id"};
        params.EmbeddingColumn = "embedding";
        params.Metric = NYdb::NTable::TVectorIndexSettings::EMetric::Euclidean;
        UNIT_ASSERT_STRING_CONTAINS(MakeSelect(params, "ann"), "VIEW ann");
        params.DistributedHnsw = true;
        params.HnswFullRange = true;
        params.HasOverlappingPostings = true;
        UNIT_ASSERT_STRING_CONTAINS(MakeSelect(params, "ann"), "VIEW ann");
        params.HasOverlappingPostings = false;
        params.PrefixColumn = "category";
        params.PrefixType = "Uint64";
        const auto query = MakeSelect(params, "ann");
        UNIT_ASSERT_STRING_CONTAINS(query, "VIEW ann");
        UNIT_ASSERT_STRING_CONTAINS(query, "WHERE category = $PrefixValue");
        UNIT_ASSERT(query.find("indexImplPostingTable") == std::string::npos);
    }
}
}
