#include <ydb/core/tx/datashard/hnsw_index/hnsw_index.h>
#include <ydb/core/tx/datashard/hnsw_index/hnsw_memory_tracker.h>

#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/random/fast.h>

#include <ydb/library/nmslib/include/logging.h>

#include <atomic>
#include <barrier>
#include <limits>
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
        Y_UNIT_TEST(OmittedRebuildThresholdUsesDocumentedDefault) {
            Ydb::Table::VectorIndexSettings settings;
            UNIT_ASSERT_VALUES_EQUAL(GetHnswDeltaRows(settings), 10000u);
            settings.set_delta_rows(5);
            UNIT_ASSERT_VALUES_EQUAL(GetHnswDeltaRows(settings), 5u);
        }

        Y_UNIT_TEST(LegacyPercentageIsNotAnAbsoluteRowCount) {
            Ydb::Table::VectorIndexSettings settings;
            // Former field 8 held 5 percent, not a five-row limit.
            UNIT_ASSERT(settings.ParseFromString(TString("\x40\x05", 2)));
            UNIT_ASSERT(!settings.has_delta_rows());
            UNIT_ASSERT_VALUES_EQUAL(GetHnswDeltaRows(settings), 10000u);
            settings.set_delta_rows(0);
            UNIT_ASSERT_VALUES_EQUAL(GetHnswDeltaRows(settings), 0u);
            settings.set_delta_rows(1ULL << 40);
            UNIT_ASSERT_VALUES_EQUAL(GetHnswDeltaRows(settings), 1ULL << 40);
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
            settings.set_m(24);
            settings.set_ef_construction(100);

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

        Y_UNIT_TEST(EuclideanGraphAndJournalDistancesUseTheSameScale) {
            const auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                               Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            const auto target = SerializeFloatVector({0, 0});
            // Squaring changes which source wins both above and below distance 1.
            for (const auto [graphDistance, journalDistance] : {std::pair{2.0f, 3.0f}, std::pair{0.3f, 0.2f}}) {
                TString error;
                auto index = THnswIndex::Build(settings, {
                    {"base", SerializeFloatVector({graphDistance, 0})},
                    {"changed", SerializeFloatVector({10, 0})},
                }, 0, error);
                UNIT_ASSERT_C(index, error);
                index->SetSnapshot({100, 0}, std::make_shared<THnswIndexChanges>());
                UNIT_ASSERT(index->Upsert("changed", SerializeFloatVector({journalDistance, 0}), {120, 0}));

                const auto before = index->Search(target, 1, {119, 0});
                UNIT_ASSERT_VALUES_EQUAL(before.Results.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(before.Results[0].first, "base");
                UNIT_ASSERT_DOUBLES_EQUAL(before.Results[0].second, graphDistance, 1e-5);

                const auto merged = index->Search(target, 2, {120, 0});
                UNIT_ASSERT_VALUES_EQUAL(merged.Results.size(), 2);
                const auto expectedKey = graphDistance < journalDistance ? "base" : "changed";
                UNIT_ASSERT_VALUES_EQUAL(merged.Results[0].first, expectedKey);
                UNIT_ASSERT_DOUBLES_EQUAL(merged.Results[0].second, Min(graphDistance, journalDistance), 1e-5);
                UNIT_ASSERT_DOUBLES_EQUAL(merged.Results[1].second, Max(graphDistance, journalDistance), 1e-5);
                const auto nearest = index->Search(target, 1, {120, 0});
                UNIT_ASSERT_VALUES_EQUAL(nearest.Results.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(nearest.Results[0].first, expectedKey);
            }
        }

        Y_UNIT_TEST(RequestedCandidatesExpandSearchBreadth) {
            constexpr ui32 dimensions = 8;
            constexpr size_t count = 256;
            constexpr size_t requested = 64;
            TFastRng<ui64> rng(42);
            std::vector<std::pair<TString, TString>> data;
            for (size_t i = 0; i < count; ++i) {
                data.emplace_back(KeyFor(i), RandomFloatVector(rng, dimensions));
            }
            const auto target = RandomFloatVector(rng, dimensions);
            for (const auto metric : {
                     Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                     Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN,
                     Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT}) {
                auto settings = MakeSettings(metric,
                                             Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, dimensions);
                settings.set_m(4);
                TString error;
                auto index = THnswIndex::Build(settings, data, 0, error);
                UNIT_ASSERT_C(index, error);
                const auto result = index->Search(target, requested, TRowVersion::Max(), 1);
                UNIT_ASSERT_C(result.Covered, error);
                UNIT_ASSERT_VALUES_EQUAL_C(result.Results.size(), requested, static_cast<int>(metric));
                THashSet<TString> keys;
                for (const auto& [key, distance] : result.Results) {
                    Y_UNUSED(distance);
                    keys.insert(key);
                }
                UNIT_ASSERT_VALUES_EQUAL(keys.size(), requested);
            }
        }

        Y_UNIT_TEST(QueryLocalEfSearchIsConcurrentAndDoesNotMutateTheGraph) {
            TFastRng<ui64> rng(42);
            std::vector<std::pair<TString, TString>> data;
            for (size_t i = 0; i < 256; ++i) {
                data.emplace_back(KeyFor(i), RandomFloatVector(rng, 8));
            }
            const auto target = RandomFloatVector(rng, 8);
            for (const auto metric : {Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
                                      Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                      Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN,
                                      Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT}) {
                auto settings = MakeSettings(metric, Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 8);
                settings.set_m(4);
                TString error;
                auto index = THnswIndex::Build(settings, data, 0, error);
                UNIT_ASSERT_C(index, error);
                const std::vector<ui32> widths{1, 15, 50, 1000};
                std::vector<THnswSearchResult> coldResults(widths.size());
                std::barrier start(static_cast<std::ptrdiff_t>(widths.size()));
                std::vector<std::thread> firstReaders;
                for (size_t i = 0; i < widths.size(); ++i) {
                    firstReaders.emplace_back([&, i] {
                        start.arrive_and_wait();
                        coldResults[i] = index->Search(target, 64, TRowVersion::Max(), widths[i]);
                    });
                }
                for (auto& reader : firstReaders) {
                    reader.join();
                }
                std::vector<THnswSearchResult> expected;
                for (ui32 width : widths) {
                    expected.push_back(index->Search(target, 64, TRowVersion::Max(), width));
                    UNIT_ASSERT_VALUES_EQUAL(expected.back().Results.size(), 64u);
                }
                for (size_t i = 0; i < widths.size(); ++i) {
                    UNIT_ASSERT(coldResults[i].Covered);
                    UNIT_ASSERT(coldResults[i].Results == expected[i].Results);
                }
                std::atomic<bool> matches{true};
                std::vector<std::thread> readers;
                for (size_t i = 0; i < widths.size(); ++i) {
                    readers.emplace_back([&, i] {
                        for (size_t attempt = 0; attempt < 20; ++attempt) {
                            const auto result = index->Search(target, 64, TRowVersion::Max(), widths[i]);
                            if (!result.Covered || result.Results != expected[i].Results) {
                                matches.store(false);
                            }
                        }
                    });
                }
                for (auto& reader : readers) {
                    reader.join();
                }
                UNIT_ASSERT(matches.load());
                UNIT_ASSERT(index->Search(target, 64).Results == expected[1].Results);
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
            // Both the retained pointer graph and optimized ids grow with M.
            UNIT_ASSERT_GE(denseEstimate - defaultEstimate,
                           100 * 2 * (100 - 16) * (sizeof(void*) + sizeof(ui32)));
            UNIT_ASSERT_VALUES_EQUAL(
                THnswIndex::EstimateMemoryBytes(100, 4, 16, 1234),
                defaultEstimate + 1234);
        }

        Y_UNIT_TEST(DenseGraphBudgetIncludesBothNeighborRepresentations) {
            auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                         Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            settings.set_m(100);
            const std::vector<std::pair<TString, TString>> data = {
                {"a", SerializeFloatVector({0, 0})},
                {"b", SerializeFloatVector({1, 0})},
            };
            // ElList_ reserves 2*M+1 pointers per row even after optimization.
            // The optimized graph additionally stores 2*M+1 neighbor ids.
            const size_t neighborBytes = data.size() * (2 * settings.m() + 1)
                * (sizeof(void*) + sizeof(ui32));
            UNIT_ASSERT_GE(THnswIndex::EstimateMemoryBytes(data.size(), 2, settings.m(), 2), neighborBytes);
            TString error;
            UNIT_ASSERT(!THnswIndex::Build(settings, data, neighborBytes - 1, error));
            UNIT_ASSERT_STRING_CONTAINS(error, "exceeds budget");
        }

        Y_UNIT_TEST(BuildHonorsExactMemoryBudget) {
            const auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                               Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            const std::vector<std::pair<TString, TString>> data = {
                {"a", SerializeFloatVector({0, 0})},
                {"b", SerializeFloatVector({1, 0})},
            };
            const auto budget = THnswIndex::EstimateMemoryBytes(2, 2, 16, 2);
            TString error;
            auto index = THnswIndex::Build(settings, data, budget, error);
            UNIT_ASSERT_C(index, error);
            UNIT_ASSERT_VALUES_EQUAL(index->EstimatedMemoryBytes(), budget);
            UNIT_ASSERT(!THnswIndex::Build(settings, data, budget - 1, error));
            UNIT_ASSERT_STRING_CONTAINS(error, "exceeds budget");
        }

        Y_UNIT_TEST(AcceptsHnswConstructionSettings) {
            auto settings = MakeSettings(
                Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
                Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
                2);
            settings.set_ef_construction(64);
            settings.set_min_rows(1);
            settings.set_delta_rows(1000);
            std::vector<std::pair<TString, TString>> data = {
                {"a", SerializeFloatVector({1.0f, 0.0f})},
                {"b", SerializeFloatVector({0.0f, 1.0f})},
            };
            TString error;
            for (const auto m : {NKMeans::MinHnswM, 8u}) {
                settings.set_m(m);
                auto index = THnswIndex::Build(settings, data, 0, error);
                UNIT_ASSERT_C(index, error);
                UNIT_ASSERT_VALUES_EQUAL(index->Size(), data.size());
            }
        }

        Y_UNIT_TEST(RejectsUnsafeHnswSettingsBeforeNmslibBuild) {
            auto settings = MakeSettings(
                Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
                Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
                2);
            std::vector<std::pair<TString, TString>> data = {
                {"a", SerializeFloatVector({1.0f, 0.0f})},
            };

            TString error;
            for (const auto m : {0u, 1u, NKMeans::MaxHnswM + 1}) {
                settings.set_m(m);
                auto index = THnswIndex::Build(settings, data, 0, error);
                UNIT_ASSERT(!index);
                UNIT_ASSERT_STRING_CONTAINS(error, "M");
            }
        }

        Y_UNIT_TEST(MemoryEstimatesSaturateOnOverflow) {
            const size_t maximum = Max<size_t>();
            UNIT_ASSERT_VALUES_EQUAL(THnswIndex::EstimateMemoryBytes(maximum, 4), maximum);
            UNIT_ASSERT_VALUES_EQUAL(THnswIndex::EstimateMemoryBytes(1, maximum), maximum);
            UNIT_ASSERT_VALUES_EQUAL(THnswIndex::EstimateMemoryBytes(1, maximum / (2 * sizeof(float))), maximum);
            UNIT_ASSERT_VALUES_EQUAL(THnswIndex::EstimateMemoryBytes(1, 4, 16, maximum), maximum);
            UNIT_ASSERT_VALUES_EQUAL(THnswIndex::EstimateMemoryBytes(0, 4, 16, 123), 123u);
            UNIT_ASSERT_VALUES_EQUAL(THnswIndexChanges::EstimateBytes("key", maximum), maximum);
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

        Y_UNIT_TEST(InferredDimensionSkipsInvalidRowsAndRejectsInvalidUpdates) {
            const auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                               Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 0);
            const TString valid = SerializeFloatVector({0, 0});
            const TString wrongDimension = SerializeFloatVector({1, 2, 3});
            const std::vector<std::pair<TString, TString>> data = {
                {"malformed", "bad"},
                {"valid", valid},
                {"mismatched", wrongDimension},
            };
            TString error;
            auto index = THnswIndex::Build(settings, data, 0, error);
            UNIT_ASSERT_C(index, error);
            UNIT_ASSERT_VALUES_EQUAL(index->Dimension(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(index->Size(), 1u);
            UNIT_ASSERT(!THnswIndex::IsValidVector(wrongDimension, 2));
            UNIT_ASSERT(!index->Upsert("valid", wrongDimension));
            UNIT_ASSERT(!index->Upsert("valid", "bad"));
            TString stored;
            UNIT_ASSERT(index->GetVector("valid", stored));
            UNIT_ASSERT_VALUES_EQUAL(stored, valid);
            UNIT_ASSERT(!index->HasChanges());
            const auto result = index->Search(valid, 1);
            UNIT_ASSERT_VALUES_EQUAL(result.Results.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.Results.front().first, "valid");
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

            // Graph navigation retains the node, but a visible tombstone removes
            // it from the result set before top-K selection.
            const auto result = index->Search(SerializeFloatVector({0.0f, 0.0f}), 2);
            THashSet<TString> keys;
            for (const auto& [key, _] : result.Results) {
                keys.insert(key);
            }
            UNIT_ASSERT(!keys.contains("a"));
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
        Y_UNIT_TEST(MvccIntermediateInsertSurvivesLaterDelete) {
            auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                         Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            TString error;
            auto index = THnswIndex::Build(settings, {{"base", SerializeFloatVector({10, 10})}}, 0, error);
            UNIT_ASSERT_C(index, error);
            auto changes = std::make_shared<THnswIndexChanges>();
            index->SetSnapshot({100, 0}, changes);
            const auto near = SerializeFloatVector({0, 0});
            changes->Set("inserted", {120, 0}, near);
            changes->Set("inserted", {160, 0}, std::nullopt);
            UNIT_ASSERT(!index->Search(near, 1, {99, 0}).Covered);
            UNIT_ASSERT_VALUES_EQUAL(index->Search(near, 1, {119, 0}).Results.front().first, "base");
            UNIT_ASSERT_VALUES_EQUAL(index->Search(near, 1, {120, 0}).Results.front().first, "inserted");
            UNIT_ASSERT_VALUES_EQUAL(index->Search(near, 1, {159, 0}).Results.front().first, "inserted");
            UNIT_ASSERT_VALUES_EQUAL(index->Search(near, 1, {160, 0}).Results.front().first, "base");
            TString vector;
            UNIT_ASSERT(index->GetVector("inserted", vector, {150, 0}));
            UNIT_ASSERT_VALUES_EQUAL(vector, near);
            UNIT_ASSERT(!index->GetVector("inserted", vector, {170, 0}));
        }

        Y_UNIT_TEST(MvccRanksOnlyVisibleVector) {
            auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                         Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            TString error;
            const auto target = SerializeFloatVector({0, 0});
            auto index = THnswIndex::Build(settings,
                                           {{"a", target}, {"b", SerializeFloatVector({1, 0})}}, 0, error);
            UNIT_ASSERT_C(index, error);
            index->SetSnapshot({100, 0}, std::make_shared<THnswIndexChanges>());
            UNIT_ASSERT(index->Upsert("a", SerializeFloatVector({20, 0}), {120, 1}));
            UNIT_ASSERT_VALUES_EQUAL(index->Search(target, 1, {120, 0}).Results.front().first, "a");
            UNIT_ASSERT_VALUES_EQUAL(index->Search(target, 1, {120, 1}).Results.front().first, "b");
            index->Erase("b", {140, 0});
            const auto results = index->Search(target, 1, {150, 0});
            UNIT_ASSERT_VALUES_EQUAL(results.Results.front().first, "a");
            UNIT_ASSERT_DOUBLES_EQUAL(results.Results.front().second, 20, 1e-5);
        }

        Y_UNIT_TEST(MvccOutOfOrderAndSameVersionWrites) {
            THnswIndexChanges changes;
            changes.Set("key", {120, 2}, TString("second"));
            changes.Set("key", {120, 1}, TString("first"));
            changes.Set("key", {120, 2}, TString("replacement"));
            UNIT_ASSERT_VALUES_EQUAL(changes.Rows.at("key").size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(changes.GetVersionCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(changes.GetEstimatedBytes(),
                                     THnswIndexChanges::EstimateBytes("key", 5) + THnswIndexChanges::EstimateBytes("key", 11));
            UNIT_ASSERT_VALUES_EQUAL(*changes.Find("key", {120, 1}, {100, 0})->Vector, "first");
            UNIT_ASSERT_VALUES_EQUAL(*changes.Find("key", {120, 2}, {100, 0})->Vector, "replacement");
            UNIT_ASSERT(!changes.Find("key", {120, 0}, {100, 0}));
            UNIT_ASSERT(!changes.Find("key", {120, 2}, {120, 2}));
        }

        Y_UNIT_TEST(MvccGenerationsShareHistoryAndRespectCoverage) {
            auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                         Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            const auto oldVector = SerializeFloatVector({10, 0});
            const auto newVector = SerializeFloatVector({0, 0});
            TString error;
            auto old = THnswIndex::Build(settings, {{"a", oldVector}}, 0, error);
            auto current = THnswIndex::Build(settings, {{"a", newVector}}, 0, error);
            UNIT_ASSERT(old && current);
            auto changes = std::make_shared<THnswIndexChanges>();
            old->SetSnapshot({100, 0}, changes, {149, 0});
            changes->Set("a", {120, 0}, newVector);
            current->SetSnapshot({150, 0}, changes);
            changes->Set("a", {170, 0}, std::nullopt);
            TString vector;
            UNIT_ASSERT(old->GetVector("a", vector, {110, 0}));
            UNIT_ASSERT_VALUES_EQUAL(vector, oldVector);
            UNIT_ASSERT(old->GetVector("a", vector, {140, 0}));
            UNIT_ASSERT_VALUES_EQUAL(vector, newVector);
            UNIT_ASSERT(!current->CanRead({140, 0}));
            UNIT_ASSERT(current->GetVector("a", vector, {160, 0}));
            UNIT_ASSERT_VALUES_EQUAL(vector, newVector);
            UNIT_ASSERT(old->CanRead({149, 0}));
            UNIT_ASSERT(!old->CanRead({150, 0}));
            UNIT_ASSERT(!old->Search(newVector, 1, {150, 0}).Covered);
            UNIT_ASSERT(!old->GetVector("a", vector, {150, 0}));
            old.reset();
            changes->PruneThrough({150, 0});
            UNIT_ASSERT_VALUES_EQUAL(changes->Rows.at("a").size(), 1);
            UNIT_ASSERT(current->GetVector("a", vector, {160, 0}));
            UNIT_ASSERT(!current->GetVector("a", vector, {170, 0}));
            changes->Valid = false;
            UNIT_ASSERT(!current->CanRead({160, 0}));
        }

        Y_UNIT_TEST(MvccRebuildThresholdCountsDistinctKeys) {
            auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                         Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            TString error;
            const auto vector = SerializeFloatVector({0, 0});
            auto index = THnswIndex::Build(settings, {{"a", vector}, {"b", vector}}, 0, error);
            UNIT_ASSERT_C(index, error);
            index->SetSnapshot({100, 0}, std::make_shared<THnswIndexChanges>());
            UNIT_ASSERT(!index->NeedsRebuild(0));
            index->Upsert("new", vector, {120, 0});
            UNIT_ASSERT(!index->NeedsRebuild(1));
            UNIT_ASSERT(index->NeedsRebuild(0));
            index->Upsert("new", vector, {130, 0});
            UNIT_ASSERT_VALUES_EQUAL(index->ChangeCount(), 1);
            UNIT_ASSERT(!index->NeedsRebuild(1));
            index->Erase("a", {140, 0});
            UNIT_ASSERT(index->NeedsRebuild(1));
            UNIT_ASSERT(!index->NeedsRebuild(2));
            UNIT_ASSERT(!index->NeedsRebuild(1ULL << 40));
        }

        Y_UNIT_TEST(MvccPruneReleasesVersionReservations) {
            THnswIndexChanges changes;
            auto reservation = std::make_shared<int>(0);
            std::weak_ptr<int> weak = reservation;
            changes.Set("key", {120, 0}, TString("vector"), std::move(reservation));
            changes.Set("key", {140, 0}, std::nullopt);
            changes.PruneThrough({119, 0});
            UNIT_ASSERT(!weak.expired());
            changes.PruneThrough({120, 0});
            UNIT_ASSERT(weak.expired());
            UNIT_ASSERT(!changes.Find("key", {140, 0}, {120, 0})->Vector);
            changes.PruneThrough({140, 0});
            UNIT_ASSERT(changes.Rows.empty());
            UNIT_ASSERT_VALUES_EQUAL(changes.GetVersionCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(changes.GetEstimatedBytes(), 0);
        }

        Y_UNIT_TEST(RejectsNonFiniteVectorsBeforeGraphAndJournalScoring) {
            auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                         Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            const auto valid = SerializeFloatVector({1, 0});
            for (float value : {std::numeric_limits<float>::quiet_NaN(),
                                std::numeric_limits<float>::infinity(),
                                -std::numeric_limits<float>::infinity()}) {
                const auto invalid = SerializeFloatVector({value, 0});
                TString unaligned = "x" + invalid;
                UNIT_ASSERT(!THnswIndex::IsValidVector(TStringBuf(unaligned).SubStr(1), 2));
                TString error;
                auto index = THnswIndex::Build(settings, {{"good", valid}, {"bad", invalid}}, 0, error);
                UNIT_ASSERT_C(index, error);
                UNIT_ASSERT_VALUES_EQUAL(index->Size(), 1);
                UNIT_ASSERT(!index->Upsert("good", invalid));
                UNIT_ASSERT(index->Search(invalid, 1).Results.empty());
                UNIT_ASSERT_VALUES_EQUAL(index->Search(valid, 1).Results.front().first, "good");
            }
        }

        Y_UNIT_TEST(ZeroNormCosineUsesExactFallbackForGraphJournalAndTarget) {
            const auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
                                               Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            const auto zero = SerializeFloatVector({0, 0});
            const auto vector = SerializeFloatVector({1, 0});
            TString error;
            auto index = THnswIndex::Build(settings, {{"a", vector}}, 0, error);
            UNIT_ASSERT_C(index, error);
            UNIT_ASSERT(!index->Search(zero, 1).Covered);
            UNIT_ASSERT(index->Upsert("zero", zero));
            UNIT_ASSERT(!index->Search(vector, 1).Covered);
            index = THnswIndex::Build(settings, {{"zero", zero}, {"a", vector}}, 0, error);
            UNIT_ASSERT_C(index, error);
            UNIT_ASSERT(!index->Search(vector, 1).Covered);
        }

        Y_UNIT_TEST(BuildLoggingSuppressionIsThreadLocalAndRestoresPreviousState) {
            auto* logger = getGlobalLogger();
            {
                ScopedLoggingDisabled quiet;
                UNIT_ASSERT(!getGlobalLogger());
                { ScopedLoggingDisabled nested; UNIT_ASSERT(!getGlobalLogger()); }
                UNIT_ASSERT(!getGlobalLogger());
                std::atomic<bool> unaffected = false;
                std::thread other([&] { unaffected.store(getGlobalLogger() == logger); });
                other.join();
                UNIT_ASSERT(unaffected.load());
            }
            UNIT_ASSERT(getGlobalLogger() == logger);
        }

        Y_UNIT_TEST(CancelledBuildDoesNotConstructGraph) {
            const auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                               Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            const std::vector<std::pair<TString, TString>> rows = {{"a", SerializeFloatVector({1, 0})}};
            std::atomic<bool> cancelled{true};
            TString error;
            UNIT_ASSERT(!THnswIndex::Build(settings, rows, 0, error, false, &cancelled));
            UNIT_ASSERT_STRING_CONTAINS(error, "cancelled");
            cancelled.store(false);
            UNIT_ASSERT(THnswIndex::Build(settings, rows, 0, error, false, &cancelled));
        }

        Y_UNIT_TEST(MvccCachedCountsFollowOutOfOrderReplacementAndPruning) {
            THnswIndexChanges changes;
            const std::vector<TRowVersion> bases{{100, 0}, {120, 0}, {140, 0}};
            auto check = [&](std::initializer_list<size_t> expected) {
                size_t i = 0;
                for (auto count : expected) {
                    UNIT_ASSERT_VALUES_EQUAL(changes.CountAfter(bases[i++]), count);
                }
            };
            check({0, 0, 0});
            changes.Set("a", {130, 0}, TString("new"));
            check({1, 1, 0});
            changes.Set("a", {110, 0}, TString("old"));
            changes.Set("a", {130, 0}, std::nullopt);
            check({1, 1, 0});
            changes.Set("b", {150, 0}, TString("b"));
            check({2, 2, 1});
            changes.PruneThrough({120, 0});
            check({2, 2, 1});
            changes.PruneThrough({130, 0});
            check({1, 1, 1});
            changes.Clear();
            check({0, 0, 0});
        }

        Y_UNIT_TEST(MvccHotKeysRetainPinnedSnapshotsWithoutUnboundedHistory) {
            auto changes = std::make_shared<THnswIndexChanges>();
            const auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                               Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            TString error;
            auto index = THnswIndex::Build(settings, {{"hot", SerializeFloatVector({0, 0})}}, 0, error);
            UNIT_ASSERT_C(index, error);
            index->SetSnapshot({10, 0}, changes);
            std::weak_ptr<int> obsolete;
            for (ui64 step = 11; step <= 1000; ++step) {
                auto reservation = std::make_shared<int>(0);
                if (step == 12) { obsolete = reservation; }
                changes->AdvanceRetention({step, 0}, {{11, 0}, {20, 0}});
                changes->Set("hot", {step, 0}, SerializeFloatVector({float(step), 0}), std::move(reservation));
            }
            UNIT_ASSERT(obsolete.expired());
            UNIT_ASSERT_VALUES_EQUAL(changes->GetVersionCount(), 3);
            UNIT_ASSERT_VALUES_EQUAL(changes->CountAfter({10, 0}), 1);
            UNIT_ASSERT_VALUES_EQUAL(changes->GetEstimatedBytes(), 3 * THnswIndexChanges::EstimateBytes("hot", 9));
            for (ui64 step : {11, 20, 1000}) {
                const auto result = index->Search(SerializeFloatVector({0, 0}), 1, {step, 0});
                UNIT_ASSERT(result.Covered);
                UNIT_ASSERT_DOUBLES_EQUAL(result.Results.front().second, step, 1e-5);
            }
            UNIT_ASSERT(!index->Search(SerializeFloatVector({0, 0}), 1, {500, 0}).Covered);
            changes->Compact({1000, 0});
            UNIT_ASSERT_VALUES_EQUAL(changes->GetVersionCount(), 1);
            // Once history is released, a newly requested old pin cannot resurrect coverage.
            changes->Compact({1000, 0}, {{20, 0}});
            UNIT_ASSERT(!index->CanRead({20, 0}));
        }

        Y_UNIT_TEST(MvccHotKeyReservationsStayBoundedBetweenPrunes) {
            auto tracker = std::make_shared<THnswCacheMemoryTracker>();
            auto changes = std::make_shared<THnswIndexChanges>();
            TString vector(768 * sizeof(float) + 1, '\0');
            vector.back() = 1;
            const ui64 bytes = THnswIndexChanges::EstimateBytes("hot", vector.size());
            tracker->SetLimit(4 * bytes);
            auto reserve = [&] {
                UNIT_ASSERT_C(tracker->TryAcquire(bytes), "Hot-key journal exhausted its four-version budget");
                return std::shared_ptr<void>(new int(0), [tracker, bytes](void* value) {
                    delete static_cast<int*>(value);
                    tracker->Release(bytes);
                });
            };
            std::shared_ptr<void> firstPin, duplicatePin, secondPin;
            std::weak_ptr<void> obsolete;
            for (ui64 step = 11; step <= 5000; ++step) {
                auto reservation = reserve();
                if (step == 12) { obsolete = reservation; }
                changes->AdvanceRetentionFloor({step, 0});
                changes->Set("hot", {step, 0}, vector, std::move(reservation));
                if (step == 11) {
                    firstPin = changes->PinReadVersion({11, 0});
                    duplicatePin = changes->PinReadVersion({11, 0});
                }
                if (step == 20) { secondPin = changes->PinReadVersion({20, 0}); }
                if (step == 100) { duplicatePin.reset(); }
                UNIT_ASSERT_LE(changes->GetVersionCount(), 3u);
            }
            UNIT_ASSERT(obsolete.expired());
            UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), 3 * bytes);
            for (ui64 step : {11, 20, 5000}) {
                UNIT_ASSERT(changes->Covers({step, 0}));
                const auto* visible = changes->Find("hot", {step, 0}, {10, 0});
                UNIT_ASSERT(visible);
                UNIT_ASSERT_VALUES_EQUAL(visible->Version, TRowVersion(step, 0));
            }
            firstPin.reset();
            secondPin.reset();
            changes->AdvanceRetentionFloor({5001, 0});
            changes->Set("hot", {5001, 0}, vector, reserve());
            UNIT_ASSERT_VALUES_EQUAL(changes->GetVersionCount(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), bytes);
            UNIT_ASSERT(!changes->Covers({11, 0}));
            UNIT_ASSERT(!changes->PinReadVersion({11, 0}));
        }

        Y_UNIT_TEST(MvccBatchedRetentionPreservesWritesAndPinnedSnapshots) {
            auto changes = std::make_shared<THnswIndexChanges>();
            const auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                               Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            TString error;
            auto index = THnswIndex::Build(settings, {{"hot", SerializeFloatVector({0, 0})}}, 0, error);
            UNIT_ASSERT_C(index, error);
            index->SetSnapshot({10, 0}, changes);
            changes->Set("hot", {11, 0}, SerializeFloatVector({11, 0}));
            changes->Compact({20, 0}, {{11, 0}});
            for (ui64 step = 21; step <= 1000; ++step) {
                changes->Set("hot", {step, 0}, SerializeFloatVector({float(step), 0}));
            }
            UNIT_ASSERT_VALUES_EQUAL(changes->GetVersionCount(), 981u);
            for (ui64 step : {11, 21, 400, 1000}) {
                const auto result = index->Search(SerializeFloatVector({0, 0}), 1, {step, 0});
                UNIT_ASSERT(result.Covered);
                UNIT_ASSERT_DOUBLES_EQUAL(result.Results.front().second, step, 1e-5);
            }
            changes->Compact({1000, 0}, {{11, 0}, {400, 0}});
            UNIT_ASSERT_VALUES_EQUAL(changes->GetVersionCount(), 3u);
            changes->Set("hot", {900, 0}, SerializeFloatVector({900, 0}));
            UNIT_ASSERT_VALUES_EQUAL(changes->GetVersionCount(), 3u);
            for (ui64 step : {11, 400, 1000}) {
                const auto result = index->Search(SerializeFloatVector({0, 0}), 1, {step, 0});
                UNIT_ASSERT(result.Covered);
                UNIT_ASSERT_DOUBLES_EQUAL(result.Results.front().second, step, 1e-5);
            }
            UNIT_ASSERT(!index->CanRead({900, 0}));
        }

        Y_UNIT_TEST(BoundedOverfetchDeclaresUncoveredWhenShadowsHideLiveRows) {
            const auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                               Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            std::vector<std::pair<TString, TString>> rows;
            for (ui32 i = 0; i < 100; ++i) {
                rows.emplace_back(KeyFor(i), SerializeFloatVector({float(i), 0}));
            }
            TString error;
            auto index = THnswIndex::Build(settings, rows, 0, error);
            UNIT_ASSERT_C(index, error);
            index->SetSnapshot({100, 0}, std::make_shared<THnswIndexChanges>());
            for (ui32 i = 0; i < 50; ++i) { index->Erase(KeyFor(i), {120, 0}); }
            const auto target = SerializeFloatVector({0, 0});
            UNIT_ASSERT(!index->Search(target, 1, {120, 0}, 1).Covered);
            const auto complete = index->Search(target, 1, {120, 0}, 100);
            UNIT_ASSERT(complete.Covered);
            UNIT_ASSERT_VALUES_EQUAL(complete.Results.front().first, KeyFor(50));
        }

        Y_UNIT_TEST(MvccEmptyGenerationAcceptsNewVectors) {
            auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                                         Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
            TString error;
            auto index = THnswIndex::Build(settings, {}, 0, error, true);
            UNIT_ASSERT_C(index, error);
            index->SetSnapshot({100, 0}, std::make_shared<THnswIndexChanges>());
            const auto target = SerializeFloatVector({1, 0});
            UNIT_ASSERT(index->Search(target, 1, {100, 0}).Results.empty());
            UNIT_ASSERT(index->Upsert("new", target, {120, 0}));
            UNIT_ASSERT(index->Search(target, 1, {119, 0}).Results.empty());
            const auto results = index->Search(target, 1, {120, 0});
            UNIT_ASSERT_VALUES_EQUAL(results.Results.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(results.Results[0].first, "new");
            UNIT_ASSERT(index->NeedsRebuild(0));
            UNIT_ASSERT(!index->NeedsRebuild(1));
        }

    } // Y_UNIT_TEST_SUITE(THnswIndexTest)

} // namespace NKikimr::NDataShard
