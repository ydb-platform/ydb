#include <ydb/core/tx/datashard/hnsw_index.h>

#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/random/fast.h>

namespace NKikimr::NDataShard {

namespace {

TString SerializeFloatVector(std::initializer_list<float> values) {
    TString result;
    TStringOutput output(result);
    NKnnVectorSerialization::TSerializer<float> serializer(&output);
    for (const auto& value : values) {
        serializer.HandleElement(value);
    }
    serializer.Finish();
    return result;
}

TString RandomFloatVector(TFastRng<ui64>& rng, ui32 dimensions) {
    TString result;
    TStringOutput output(result);
    NKnnVectorSerialization::TSerializer<float> serializer(&output);
    for (ui32 i = 0; i < dimensions; ++i) {
        serializer.HandleElement(static_cast<float>(rng.GenRandReal1()) * 2.0f - 1.0f);
    }
    serializer.Finish();
    return result;
}

Ydb::Table::VectorIndexSettings MakeSettings(
    Ydb::Table::VectorIndexSettings::Metric metric,
    Ydb::Table::VectorIndexSettings::VectorType vectorType,
    ui32 dimensions)
{
    Ydb::Table::VectorIndexSettings settings;
    settings.set_metric(metric);
    settings.set_vector_type(vectorType);
    settings.set_vector_dimension(dimensions);
    return settings;
}

TString KeyFor(ui32 i) {
    return "key-" + ToString(i);
}

// Returns the set of keys among `numRows` random vectors that are the true
// (brute-force) top-K nearest neighbors of `target` under `settings`.
THashSet<TString> BruteForceTopK(
    const Ydb::Table::VectorIndexSettings& settings,
    const std::vector<std::pair<TString, TString>>& keysAndVectors,
    const TString& target,
    size_t k)
{
    TString error;
    auto clusters = NKMeans::CreateClusters(settings, 0, error);
    UNIT_ASSERT_C(clusters, error);

    std::vector<std::pair<double, TString>> distances;
    distances.reserve(keysAndVectors.size());
    for (const auto& [key, vec] : keysAndVectors) {
        distances.emplace_back(clusters->CalcDistance(vec, target), key);
    }
    Sort(distances.begin(), distances.end());

    THashSet<TString> result;
    for (size_t i = 0; i < Min(k, distances.size()); ++i) {
        result.insert(distances[i].second);
    }
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(THnswIndexTest) {

    Y_UNIT_TEST(BuildAndSearchFindsExactNeighborForTinyDataset) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            2);

        std::vector<std::pair<TString, TString>> data = {
            {"a", SerializeFloatVector({1.0f, 0.0f})},
            {"b", SerializeFloatVector({0.0f, 1.0f})},
            {"c", SerializeFloatVector({-1.0f, 0.0f})},
            {"d", SerializeFloatVector({0.9f, 0.1f})},
        };

        TString error;
        auto index = THnswIndex::Build(settings, data, /* maxMemoryBytes */ 0, error);
        UNIT_ASSERT_C(index, error);
        UNIT_ASSERT_VALUES_EQUAL(index->Size(), data.size());

        auto result = index->Search(SerializeFloatVector({1.0f, 0.0f}), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.Results.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result.Results[0].first, "a");
    }

    Y_UNIT_TEST(MatchesBruteForceTopKAcrossMetrics) {
        const Ydb::Table::VectorIndexSettings::Metric metrics[] = {
            Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
            Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN,
            Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT,
        };

        constexpr ui32 dimensions = 8;
        constexpr size_t numRows = 200;
        constexpr size_t k = 5;

        TFastRng<ui64> rng(42);
        std::vector<std::pair<TString, TString>> data;
        data.reserve(numRows);
        for (size_t i = 0; i < numRows; ++i) {
            data.emplace_back(KeyFor(i), RandomFloatVector(rng, dimensions));
        }
        const TString target = RandomFloatVector(rng, dimensions);

        for (auto metric : metrics) {
            auto settings = MakeSettings(metric, Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, dimensions);

            TString error;
            auto index = THnswIndex::Build(settings, data, /* maxMemoryBytes */ 0, error);
            UNIT_ASSERT_C(index, error);

            auto result = index->Search(target, k);
            UNIT_ASSERT_VALUES_EQUAL(result.Results.size(), k);

            auto expected = BruteForceTopK(settings, data, target, k);

            // HNSW is approximate; on a small random dataset with default
            // parameters recall should be at (or extremely close to) 100%.
            size_t hits = 0;
            for (const auto& [key, _] : result.Results) {
                if (expected.contains(key)) {
                    ++hits;
                }
            }
            UNIT_ASSERT_C(hits >= k - 1, "metric=" << static_cast<int>(metric) << " hits=" << hits << " of " << k);
        }
    }

    Y_UNIT_TEST(RejectsNonFloatVectorType) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8,
            4);

        std::vector<std::pair<TString, TString>> data = {
            {"a", SerializeFloatVector({1.0f, 2.0f, 3.0f, 4.0f})},
        };

        TString error;
        auto index = THnswIndex::Build(settings, data, /* maxMemoryBytes */ 0, error);
        UNIT_ASSERT(!index);
        UNIT_ASSERT(!error.empty());
    }

    Y_UNIT_TEST(RejectsWhenOverMemoryBudget) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            4);

        std::vector<std::pair<TString, TString>> data = {
            {"a", SerializeFloatVector({1.0f, 0.0f, 0.0f, 0.0f})},
            {"b", SerializeFloatVector({0.0f, 1.0f, 0.0f, 0.0f})},
        };

        // A tiny budget that no non-empty index could possibly fit into.
        TString error;
        auto index = THnswIndex::Build(settings, data, /* maxMemoryBytes */ 1, error);
        UNIT_ASSERT(!index);
        UNIT_ASSERT(!error.empty());
    }

    Y_UNIT_TEST(RejectsEmptyInput) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            4);

        std::vector<std::pair<TString, TString>> data;

        TString error;
        auto index = THnswIndex::Build(settings, data, /* maxMemoryBytes */ 0, error);
        UNIT_ASSERT(!index);
        UNIT_ASSERT(!error.empty());
    }

    Y_UNIT_TEST(SkipsMalformedVectorsButBuildsFromTheRest) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            2);

        std::vector<std::pair<TString, TString>> data = {
            {"a", SerializeFloatVector({1.0f, 0.0f})},
            {"bad", "\x01"}, // Too short to be a valid FloatVector.
            {"b", SerializeFloatVector({0.0f, 1.0f})},
        };

        TString error;
        auto index = THnswIndex::Build(settings, data, /* maxMemoryBytes */ 0, error);
        UNIT_ASSERT_C(index, error);
        UNIT_ASSERT_VALUES_EQUAL(index->Size(), 2u);
    }
}

} // namespace NKikimr::NDataShard
