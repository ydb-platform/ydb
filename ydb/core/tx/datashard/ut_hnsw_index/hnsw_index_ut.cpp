#include <ydb/core/tx/datashard/hnsw_index.h>

#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/random/fast.h>

#include <thread>

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
    Y_UNIT_TEST(OmittedMinRowsUsesDocumentedDefault) {
        Ydb::Table::VectorIndexSettings settings;
        UNIT_ASSERT_VALUES_EQUAL(GetHnswMinRows(settings), 10000u);
        settings.set_hnsw_min_rows(0);
        UNIT_ASSERT_VALUES_EQUAL(GetHnswMinRows(settings), 0u);
    }

    Y_UNIT_TEST(CacheSettingsIdentityIsNormalizedAndComplete) {
        auto cached = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            2);
        auto requested = cached;
        requested.set_hnsw_connectivity(16);
        requested.set_hnsw_construction_candidates(200);
        requested.set_hnsw_search_candidates(15);
        UNIT_ASSERT(AreHnswIndexSettingsCompatible(cached, requested));

        requested.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN);
        UNIT_ASSERT(!AreHnswIndexSettingsCompatible(cached, requested));
        requested = cached;
        requested.set_vector_dimension(3);
        UNIT_ASSERT(!AreHnswIndexSettingsCompatible(cached, requested));
        requested = cached;
        requested.set_hnsw_search_candidates(16);
        UNIT_ASSERT(!AreHnswIndexSettingsCompatible(cached, requested));
    }

    Y_UNIT_TEST(CacheMemoryTrackerBoundariesAndUnderflow) {
        THnswCacheMemoryTracker tracker;
        UNIT_ASSERT(!tracker.TryAcquire(1));

        tracker.SetLimit(100);
        UNIT_ASSERT(tracker.TryAcquire(100));
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetUsed(), 100);
        UNIT_ASSERT(!tracker.TryAcquire(1));
        UNIT_ASSERT(!tracker.TryAcquire(Max<ui64>()));

        tracker.Release(40);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetUsed(), 60);
        tracker.Release(100);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetUsed(), 0);
    }

    Y_UNIT_TEST(CacheMemoryTrackerConcurrentAcquireRelease) {
        THnswCacheMemoryTracker tracker;
        tracker.SetLimit(8);
        std::atomic<ui64> acquired = 0;
        std::vector<std::thread> threads;
        for (size_t i = 0; i < 8; ++i) {
            threads.emplace_back([&] {
                for (size_t attempt = 0; attempt < 10000; ++attempt) {
                    if (tracker.TryAcquire(1)) {
                        ++acquired;
                        tracker.Release(1);
                    }
                }
            });
        }
        tracker.SetLimit(4);
        for (auto& thread : threads) {
            thread.join();
        }
        UNIT_ASSERT(acquired.load() > 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetUsed(), 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetLimit(), 4);
    }


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

        TString stored;
        UNIT_ASSERT(index->GetVector("a", stored));
        UNIT_ASSERT_VALUES_EQUAL(stored, data[0].second);
        UNIT_ASSERT(!index->GetVector("missing", stored));
    }

    Y_UNIT_TEST(BuildAndSearchWithCustomParameters) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            2);
        settings.set_hnsw_connectivity(24);
        settings.set_hnsw_construction_candidates(100);
        settings.set_hnsw_search_candidates(10);

        std::vector<std::pair<TString, TString>> data = {
            {"a", SerializeFloatVector({1.0f, 0.0f})},
            {"b", SerializeFloatVector({0.0f, 1.0f})},
            {"c", SerializeFloatVector({-1.0f, 0.0f})},
        };

        TString error;
        auto index = THnswIndex::Build(settings, data, /* maxMemoryBytes */ 0, error);
        UNIT_ASSERT_C(index, error);
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

    Y_UNIT_TEST(MemoryEstimateAccountsForConnectivity) {
        const auto defaultEstimate = THnswIndex::EstimateMemoryBytes(100, 4, 16);
        const auto denseEstimate = THnswIndex::EstimateMemoryBytes(100, 4, 100);
        UNIT_ASSERT_GT(denseEstimate, defaultEstimate);
        UNIT_ASSERT_VALUES_EQUAL(denseEstimate - defaultEstimate,
            100 * 2 * (100 - 16) * sizeof(ui32));
        UNIT_ASSERT_VALUES_EQUAL(
            THnswIndex::EstimateMemoryBytes(100, 4, 16, 1234),
            defaultEstimate + 1234);
    }

    Y_UNIT_TEST(RejectsUnsafeHnswSettingsBeforeNmslibBuild) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            2);
        settings.set_hnsw_connectivity(NKMeans::MaxHnswConnectivity + 1);
        std::vector<std::pair<TString, TString>> data = {
            {"a", SerializeFloatVector({1.0f, 0.0f})},
        };

        TString error;
        auto index = THnswIndex::Build(settings, data, 0, error);
        UNIT_ASSERT(!index);
        UNIT_ASSERT_STRING_CONTAINS(error, "hnsw_connectivity");
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

    Y_UNIT_TEST(InsertAfterBuildParticipatesInSearch) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            2);
        std::vector<std::pair<TString, TString>> data = {
            {"a", SerializeFloatVector({0.0f, 0.0f})},
            {"b", SerializeFloatVector({10.0f, 10.0f})},
        };

        TString error;
        auto index = THnswIndex::Build(settings, data, /* maxMemoryBytes */ 0, error);
        UNIT_ASSERT_C(index, error);

        const TString inserted = SerializeFloatVector({0.1f, 0.1f});
        UNIT_ASSERT(index->Upsert("c", inserted));
        UNIT_ASSERT(index->HasChanges());
        UNIT_ASSERT(index->HasDelta("c"));

        TString vector;
        UNIT_ASSERT(index->GetVector("c", vector));
        UNIT_ASSERT_VALUES_EQUAL(vector, inserted);

        const auto result = index->Search(inserted, 1);
        UNIT_ASSERT(!result.Results.empty());
        UNIT_ASSERT_VALUES_EQUAL(result.Results.front().first, "c");
        UNIT_ASSERT_DOUBLES_EQUAL(result.Results.front().second, 0.0, 1e-6);
    }

    Y_UNIT_TEST(UpdateAfterBuildUsesNewVector) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            2);
        std::vector<std::pair<TString, TString>> data = {
            {"a", SerializeFloatVector({0.0f, 0.0f})},
            {"b", SerializeFloatVector({10.0f, 10.0f})},
        };
        TString error;
        auto index = THnswIndex::Build(settings, data, 0, error);
        UNIT_ASSERT_C(index, error);

        const TString updated = SerializeFloatVector({0.1f, 0.1f});
        UNIT_ASSERT(index->Upsert("b", updated));
        UNIT_ASSERT(index->HasDelta("b"));

        TString vector;
        UNIT_ASSERT(index->GetVector("b", vector));
        UNIT_ASSERT_VALUES_EQUAL(vector, updated);
        const auto result = index->Search(updated, 1);
        UNIT_ASSERT(!result.Results.empty());
        UNIT_ASSERT_VALUES_EQUAL(result.Results.front().first, "b");
        UNIT_ASSERT_DOUBLES_EQUAL(result.Results.front().second, 0.0, 1e-6);
    }

    Y_UNIT_TEST(DeleteExistingRowAfterBuildHidesVector) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            2);
        std::vector<std::pair<TString, TString>> data = {
            {"a", SerializeFloatVector({0.0f, 0.0f})},
            {"b", SerializeFloatVector({10.0f, 10.0f})},
        };
        TString error;
        auto index = THnswIndex::Build(settings, data, 0, error);
        UNIT_ASSERT_C(index, error);

        index->Erase("a");
        UNIT_ASSERT(index->HasChanges());
        UNIT_ASSERT(index->HasDelta("a"));
        TString vector;
        UNIT_ASSERT(!index->GetVector("a", vector));

        // The immutable graph retains erased keys. DataShard subsequently
        // validates every candidate against the MVCC-visible posting table.
        const auto result = index->Search(SerializeFloatVector({0.0f, 0.0f}), 2);
        THashSet<TString> keys;
        for (const auto& [key, _] : result.Results) {
            keys.insert(key);
        }
        UNIT_ASSERT(keys.contains("a"));
        UNIT_ASSERT(keys.contains("b"));
    }

    Y_UNIT_TEST(DeleteInsertedRowAfterBuildRemovesDeltaCandidate) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            2);
        std::vector<std::pair<TString, TString>> data = {
            {"a", SerializeFloatVector({0.0f, 0.0f})},
            {"b", SerializeFloatVector({10.0f, 10.0f})},
        };
        TString error;
        auto index = THnswIndex::Build(settings, data, 0, error);
        UNIT_ASSERT_C(index, error);

        UNIT_ASSERT(index->Upsert("c", SerializeFloatVector({0.1f, 0.1f})));
        index->Erase("c");
        TString vector;
        UNIT_ASSERT(!index->GetVector("c", vector));
        const auto result = index->Search(SerializeFloatVector({0.1f, 0.1f}), 1);
        for (const auto& [key, _] : result.Results) {
            UNIT_ASSERT_VALUES_UNEQUAL(key, "c");
        }
    }

    Y_UNIT_TEST(ReinsertDeletedRowAfterBuildUsesLatestVector) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            2);
        std::vector<std::pair<TString, TString>> data = {
            {"a", SerializeFloatVector({0.0f, 0.0f})},
            {"b", SerializeFloatVector({10.0f, 10.0f})},
        };
        TString error;
        auto index = THnswIndex::Build(settings, data, 0, error);
        UNIT_ASSERT_C(index, error);

        index->Erase("a");
        const TString reinserted = SerializeFloatVector({5.0f, 5.0f});
        UNIT_ASSERT(index->Upsert("a", reinserted));
        TString vector;
        UNIT_ASSERT(index->GetVector("a", vector));
        UNIT_ASSERT_VALUES_EQUAL(vector, reinserted);

        const auto result = index->Search(reinserted, 1);
        UNIT_ASSERT(!result.Results.empty());
        UNIT_ASSERT_VALUES_EQUAL(result.Results.front().first, "a");
        UNIT_ASSERT_DOUBLES_EQUAL(result.Results.front().second, 0.0, 1e-6);
    }
}

} // namespace NKikimr::NDataShard
