#include <ydb/core/tx/datashard/hnsw_index.h>
#include <ydb/core/tx/datashard/hnsw/cache.h>

#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <library/cpp/dot_product/dot_product.h>
#include <library/cpp/dot_product/dot_product_avx2.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/random/fast.h>
#include <util/system/cpu_id.h>

#include <method/visited_list_pool.h>
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-but-set-variable"
#include <method/hnsw.h>
#include <space/space_lp.h>
#pragma clang diagnostic pop

#include <atomic>
#include <barrier>
#include <cstring>
#include <thread>
#include <sstream>

namespace NKikimr::NDataShard {

namespace {

class TTestMemoryConsumer : public NMemory::IMemoryConsumer {
public:
    void SetReport(NMemory::TConsumerReport report) override {
        Report = report;
    }

    NMemory::TConsumerReport Report;
};

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
    Y_UNIT_TEST(GraphBuildIsIndependentOfWorkerHistory) {
        const auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 200);
        TFastRng<ui64> rng(42);
        std::vector<std::pair<TString, TString>> rows;
        std::vector<std::pair<TString, TString>> history;
        for (ui32 i = 0; i < 64; ++i) {
            rows.emplace_back(KeyFor(i), RandomFloatVector(rng, 200));
        }
        for (ui32 i = 0; i < 37; ++i) {
            history.emplace_back(KeyFor(i), RandomFloatVector(rng, 200));
        }
        const auto snapshot = [&](const auto& vectors) {
            TString error;
            auto graph = THnswIndex::Build(settings, vectors, 4_MB, error);
            UNIT_ASSERT_C(graph, error);
            std::ostringstream output(std::ios::binary);
            graph->SaveGraph(output);
            return output.str();
        };
        const auto first = snapshot(rows);
        snapshot(history);
        UNIT_ASSERT_C(first == snapshot(rows), "Graph topology depends on previous builds on this worker");
    }

    Y_UNIT_TEST(InnerProductSimdTailsSurviveSnapshotRoundTrip) {
        // The test runner normally disables AVX dispatch. Exercise both kernels
        // in this forked subtest and restore the process-local dispatcher.
        const auto saved = NDotProductImpl::DotProductFloatImpl;
        Y_DEFER { NDotProductImpl::DotProductFloatImpl = saved; };
        std::vector<decltype(NDotProductImpl::DotProductFloatImpl)> kernels{saved};
        if (NX86::HaveAVX2() && NX86::HaveFMA()) {
            kernels.push_back(&DotProductAvx2);
        }
        for (auto kernel : kernels) {
            NDotProductImpl::DotProductFloatImpl = kernel;
            for (const ui32 dimension : {1, 3, 4, 7, 8, 15, 16, 31, 32, 199, 200, 201, 257}) {
                TFastRng<ui64> rng(42);
                auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT,
                    Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, dimension);
                settings.set_m(4);
                std::vector<std::pair<TString, TString>> rows;
                for (ui32 i = 0; i < 16; ++i) {
                    rows.emplace_back(KeyFor(i), RandomFloatVector(rng, dimension));
                }
                const auto target = RandomFloatVector(rng, dimension);
                THashMap<TString, double> expected;
                for (const auto& [key, vector] : rows) {
                    double distance = 0;
                    for (ui32 d = 0; d < dimension; ++d) {
                        float lhs, rhs;
                        std::memcpy(&lhs, vector.data() + d * sizeof(float), sizeof(float));
                        std::memcpy(&rhs, target.data() + d * sizeof(float), sizeof(float));
                        distance -= static_cast<double>(lhs) * rhs;
                    }
                    expected.emplace(key, distance);
                }
                TString error;
                auto original = THnswIndex::Build(settings, rows, 1_MB, error);
                UNIT_ASSERT_C(original, error);
                std::ostringstream out(std::ios::binary);
                original->SaveGraph(out);
                std::istringstream in(out.str(), std::ios::binary);
                auto restored = THnswIndex::LoadGraph(in, settings, 1_MB, error);
                UNIT_ASSERT_C(restored, error);
                for (const auto* index : {original.get(), restored.get()}) {
                    const auto result = index->Search(target, rows.size(), TRowVersion::Max(), 32);
                    UNIT_ASSERT(result.Covered);
                    UNIT_ASSERT_VALUES_EQUAL(result.Results.size(), rows.size());
                    for (const auto& [key, distance] : result.Results) {
                        UNIT_ASSERT_DOUBLES_EQUAL_C(distance, expected.at(key), 1e-4,
                            "dimension=" << dimension << " key=" << key);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(EuclideanSimdTailsSurviveSnapshotRoundTrip) {
        for (const ui32 dimension : {1, 3, 4, 7, 8, 15, 16, 31, 32, 191, 192, 193, 257}) {
            TFastRng<ui64> rng(42);
            auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, dimension);
            settings.set_m(4);
            std::vector<std::pair<TString, TString>> rows;
            for (ui32 i = 0; i < 16; ++i) {
                rows.emplace_back(KeyFor(i), RandomFloatVector(rng, dimension));
            }
            const auto target = RandomFloatVector(rng, dimension);
            THashMap<TString, double> expected;
            for (const auto& [key, vector] : rows) {
                double distance = 0;
                for (ui32 d = 0; d < dimension; ++d) {
                    float lhs, rhs;
                    std::memcpy(&lhs, vector.data() + d * sizeof(float), sizeof(float));
                    std::memcpy(&rhs, target.data() + d * sizeof(float), sizeof(float));
                    const double delta = static_cast<double>(lhs) - rhs;
                    distance += delta * delta;
                }
                expected.emplace(key, distance);
            }
            TString error;
            auto original = THnswIndex::Build(settings, rows, 1_MB, error);
            UNIT_ASSERT_C(original, error);
            std::ostringstream out(std::ios::binary);
            original->SaveGraph(out);
            std::istringstream in(out.str(), std::ios::binary);
            auto restored = THnswIndex::LoadGraph(in, settings, 1_MB, error);
            UNIT_ASSERT_C(restored, error);
            for (const auto* index : {original.get(), restored.get()}) {
                const auto result = index->Search(target, rows.size(), TRowVersion::Max(), 32);
                UNIT_ASSERT(result.Covered);
                UNIT_ASSERT_VALUES_EQUAL(result.Results.size(), rows.size());
                for (const auto& [key, distance] : result.Results) {
                    UNIT_ASSERT_DOUBLES_EQUAL_C(distance, expected.at(key), 1e-3,
                        "dimension=" << dimension << " key=" << key);
                }
            }
        }
    }

    Y_UNIT_TEST(LosslessByteL2Eligibility) {
        for (const float special : {0.0f, 0.5f, -1.0f, 256.0f}) {
            similarity::SpaceLp<float> space(2);
            std::vector<std::unique_ptr<similarity::Object>> owners;
            similarity::ObjectVector objects;
            for (int i = 0; i < 8; ++i) {
                const float values[] = {float(i), special};
                owners.push_back(std::make_unique<similarity::Object>(i, -1, sizeof(values), values));
                objects.push_back(owners.back().get());
            }
            similarity::Hnsw<float> index(false, space, objects);
            index.CreateIndex(similarity::AnyParams(std::vector<std::string>{
                "M=4", "efConstruction=16", "indexThreadQty=1"}));
            UNIT_ASSERT_VALUES_EQUAL(index.EnableLosslessL2Bytes(), special == 0 && NX86::HaveAVX2());
        }
    }

    Y_UNIT_TEST(LosslessByteL2TailsFallbackAndSnapshotRoundTrip) {
        const auto serialize = [](const std::vector<float>& values) {
            TString result;
            TStringOutput output(result);
            NKnnVectorSerialization::TSerializer<float> serializer(&output);
            for (float value : values) {
                serializer.HandleElement(value);
            }
            serializer.Finish();
            return result;
        };
        for (const ui32 dimension : {1, 3, 7, 8, 15, 16, 31, 32, 63, 64, 65, 127, 128, 191, 192, 193, 255, 256, 257}) {
            for (const bool fractionalGraph : {false, true}) {
                auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                    Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, dimension);
                settings.set_m(4);
                std::vector<std::vector<float>> vectors;
                std::vector<std::pair<TString, TString>> rows;
                for (ui32 i = 0; i < 16; ++i) {
                    auto& values = vectors.emplace_back(dimension);
                    for (ui32 d = 0; d < dimension; ++d) {
                        values[d] = i == 0 ? 0 : i == 1 ? 255 : (i * 13 + d * 17) % 256;
                    }
                    if (fractionalGraph && i == 15) {
                        values[dimension / 2] += 0.5f;
                    }
                    rows.emplace_back(KeyFor(i), serialize(values));
                }
                TString error;
                auto original = THnswIndex::Build(settings, rows, 1_MB, error);
                UNIT_ASSERT_C(original, error);
                std::ostringstream out(std::ios::binary);
                original->SaveGraph(out);
                std::istringstream in(out.str(), std::ios::binary);
                auto restored = THnswIndex::LoadGraph(in, settings, 1_MB, error);
                UNIT_ASSERT_C(restored, error);
                for (const float queryValue : {0.0f, 255.0f, 0.25f, -0.5f, 256.0f}) {
                    const auto target = serialize(std::vector<float>(dimension, queryValue));
                    THashMap<TString, double> expected;
                    for (ui32 i = 0; i < vectors.size(); ++i) {
                        double distance = 0;
                        for (float value : vectors[i]) {
                            const double delta = double(value) - queryValue;
                            distance += delta * delta;
                        }
                        expected.emplace(KeyFor(i), distance);
                    }
                    for (const auto* index : {original.get(), restored.get()}) {
                        for (const ui32 ef : {32, 1000}) {
                            const auto result = index->Search(target, rows.size(), TRowVersion::Max(), ef);
                            UNIT_ASSERT(result.Covered);
                            UNIT_ASSERT_VALUES_EQUAL(result.Results.size(), rows.size());
                            for (const auto& [key, distance] : result.Results) {
                                if (!fractionalGraph && queryValue >= 0 && queryValue <= 255 && queryValue != 0.25f) {
                                    UNIT_ASSERT_VALUES_EQUAL_C(distance, expected.at(key), "dimension=" << dimension);
                                } else {
                                    UNIT_ASSERT_DOUBLES_EQUAL_C(distance, expected.at(key),
                                        expected.at(key) * 1e-6 + 1e-3, "dimension=" << dimension << " key=" << key);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(LosslessByteL2ConcurrentQueries) {
        auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 4);
        std::vector<std::pair<TString, TString>> rows;
        for (ui32 i = 0; i < 32; ++i) {
            rows.emplace_back(KeyFor(i), SerializeFloatVector({float(i), 255, 0, 17}));
        }
        TString error;
        auto index = THnswIndex::Build(settings, rows, 1_MB, error);
        UNIT_ASSERT_C(index, error);
        std::atomic<bool> correct = true;
        std::barrier start(8);
        std::vector<std::thread> threads;
        for (ui32 i = 0; i < 8; ++i) {
            threads.emplace_back([&, i] {
                start.arrive_and_wait();
                for (ui32 j = 0; j < 32; ++j) {
                    const auto& target = rows[(i + j) % rows.size()];
                    const auto result = index->Search(target.second, 1, TRowVersion::Max(), 32 + i);
                    if (!result.Covered || result.Results.size() != 1
                            || result.Results.front().first != target.first || result.Results.front().second != 0) {
                        correct = false;
                    }
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        UNIT_ASSERT(correct);
    }

    Y_UNIT_TEST(LosslessByteL2MemoryBudget) {
        const size_t base = THnswIndex::EstimateMemoryBytes(2, 4, 16, 2);
        const size_t withBytes = THnswIndex::EstimateMemoryBytes(2, 4, 16, 2, true);
        UNIT_ASSERT_VALUES_EQUAL(withBytes, base + 2 * 4);
        UNIT_ASSERT_VALUES_EQUAL(THnswIndex::EstimateMemoryBytes(2, 257, 16, 2, true),
            THnswIndex::EstimateMemoryBytes(2, 257, 16, 2));
        auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 4);
        std::vector<std::pair<TString, TString>> rows{{"a", SerializeFloatVector({0, 0, 0, 0})},
            {"b", SerializeFloatVector({255, 255, 255, 255})}};
        TString error;
        UNIT_ASSERT(!THnswIndex::Build(settings, rows, base, error));
        auto index = THnswIndex::Build(settings, rows, withBytes, error);
        UNIT_ASSERT_C(index, error);
        UNIT_ASSERT_VALUES_EQUAL(index->EstimatedMemoryBytes(), withBytes);
        std::ostringstream out(std::ios::binary);
        index->SaveGraph(out);
        std::istringstream insufficient(out.str(), std::ios::binary);
        auto floatOnly = THnswIndex::LoadGraph(insufficient, settings, base, error);
        UNIT_ASSERT_C(floatOnly, error);
        UNIT_ASSERT_VALUES_EQUAL(floatOnly->EstimatedMemoryBytes(), base);
        UNIT_ASSERT_VALUES_EQUAL(floatOnly->Search(rows.front().second, 1).Results.front().second, 0);
        std::istringstream sufficient(out.str(), std::ios::binary);
        UNIT_ASSERT_C(THnswIndex::LoadGraph(sufficient, settings, withBytes, error), error);
    }

    Y_UNIT_TEST(BootstrapRejectsLateCommitsBeforeAndAfterManifestDelivery) {
        THnswBootstrapChanges before;
        UNIT_ASSERT(before.Observe(11, {50, 0}));
        UNIT_ASSERT(!before.SetSnapshot({100, 0}, 10));
        THnswBootstrapChanges after;
        UNIT_ASSERT(after.Observe(9, {50, 0}));
        UNIT_ASSERT(after.SetSnapshot({100, 0}, 10));
        UNIT_ASSERT(after.Covers(9));
        UNIT_ASSERT(after.Observe(9, {50, 0}));
        UNIT_ASSERT(!after.Covers(11));
        UNIT_ASSERT(after.Observe(11, {101, 0}));
        UNIT_ASSERT(!after.Observe(12, {100, 0}));
    }

    Y_UNIT_TEST(BootstrapHistoryIsBoundedAndReleasesReservationsAtCheckpoint) {
        THnswBootstrapChanges changes;
        auto reservation = std::make_shared<int>(0);
        std::weak_ptr<int> weak = reservation;
        UNIT_ASSERT(changes.Observe(1, {100, 0}, std::move(reservation)));
        UNIT_ASSERT(!weak.expired());
        UNIT_ASSERT(changes.SetSnapshot({50, 0}, 1));
        UNIT_ASSERT(weak.expired());
        THnswBootstrapChanges delayed;
        for (ui32 step = 0; step < THnswBootstrapChanges::MaxSteps; ++step)
            UNIT_ASSERT(delayed.Observe(step, {100, 0}));
        UNIT_ASSERT(!delayed.Observe(THnswBootstrapChanges::MaxSteps, {100, 0}));
    }

    Y_UNIT_TEST(JournalSnapshotPreservesVersionsAndDetectsCorruption) {
        THnswJournalSnapshot journal;
        journal.Changes.push_back({"a", {10, 1}, SerializeFloatVector({1, 2})});
        journal.Changes.push_back({"a", {20, 2}, std::nullopt});
        std::ostringstream output(std::ios::binary);
        journal.Save(output);
        const auto bytes = output.str();
        std::istringstream input(bytes, std::ios::binary);
        auto loaded = THnswJournalSnapshot::Load(input, 1_MB);
        UNIT_ASSERT_VALUES_EQUAL(loaded->Changes.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(loaded->Changes[0].Key, "a");
        UNIT_ASSERT_VALUES_EQUAL(loaded->Changes[0].Version, TRowVersion(10, 1));
        UNIT_ASSERT_VALUES_EQUAL(*loaded->Changes[0].Vector, *journal.Changes[0].Vector);
        UNIT_ASSERT(!loaded->Changes[1].Vector);
        for (size_t i = 0; i < bytes.size(); ++i) {
            auto damaged = bytes;
            damaged[i] ^= char(0x80);
            std::istringstream invalid(damaged, std::ios::binary);
            UNIT_ASSERT_EXCEPTION(THnswJournalSnapshot::Load(invalid, 1_MB), yexception);
        }
        std::istringstream tooSmall(bytes, std::ios::binary);
        UNIT_ASSERT_EXCEPTION(THnswJournalSnapshot::Load(tooSmall, 1), yexception);
    }

    Y_UNIT_TEST(SharedGraphViewsKeepIndependentSnapshotsAndOwnership) {
        auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
        TString error;
        auto leader = THnswIndex::Build(settings, {{"a", SerializeFloatVector({1, 0})},
            {"b", SerializeFloatVector({2, 0})}}, 1_MB, error);
        UNIT_ASSERT_C(leader, error);
        auto reservation = std::make_shared<int>(1);
        std::weak_ptr<int> lifetime = reservation;
        leader->SetGraphReservation(std::move(reservation));
        auto firstChanges = std::make_shared<THnswIndexChanges>();
        auto secondChanges = std::make_shared<THnswIndexChanges>();
        auto first = leader->CreateView({10, 0}, firstChanges, {20, 0});
        auto second = leader->CreateView({10, 0}, secondChanges, {30, 0});
        UNIT_ASSERT(first->SharesGraphWith(*second));
        UNIT_ASSERT(first->SharesGraphWith(*leader));
        first->Erase("a", {15, 0});
        const auto target = SerializeFloatVector({0, 0});
        UNIT_ASSERT_VALUES_EQUAL(first->Search(target, 1, {20, 0}).Results.front().first, "b");
        UNIT_ASSERT_VALUES_EQUAL(second->Search(target, 1, {20, 0}).Results.front().first, "a");
        UNIT_ASSERT(!first->CanRead({25, 0}));
        UNIT_ASSERT(second->CanRead({25, 0}));
        firstChanges->Valid = false;
        UNIT_ASSERT(!first->CanRead({20, 0}));
        UNIT_ASSERT(second->CanRead({20, 0}));
        leader.reset();
        first.reset();
        UNIT_ASSERT(!lifetime.expired());
        UNIT_ASSERT_VALUES_EQUAL(second->Search(target, 1, {20, 0}).Results.front().first, "a");
        second.reset();
        UNIT_ASSERT(lifetime.expired());
    }

    Y_UNIT_TEST(SharedGraphSearchWorkspaceIsAdmittedAndReleased) {
        auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
        TString error;
        const auto vector = SerializeFloatVector({1, 0});
        auto graph = THnswIndex::Build(settings, {{"a", vector}}, 1_MB, error);
        UNIT_ASSERT_C(graph, error);
        auto tracker = std::make_shared<THnswCacheMemoryTracker>();
        graph->SetGraphReservation(std::make_shared<int>(0), tracker);
        auto replica = graph->CreateView({10, 0}, std::make_shared<THnswIndexChanges>());
        const auto workspace = replica->EstimatedSearchMemoryBytes();
        tracker->SetLimit(workspace - 1);
        UNIT_ASSERT(!replica->Search(vector, 1, {10, 0}).Covered);
        UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), 0);
        tracker->SetLimit(workspace);
        const auto result = replica->Search(vector, 1, {10, 0});
        UNIT_ASSERT(result.Covered);
        UNIT_ASSERT_VALUES_EQUAL(result.Results.front().first, "a");
        UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), replica->GetSearchWorkspaceStats().RetainedBytes);
        UNIT_ASSERT_GT(tracker->GetUsed(), 0);
        replica->TrimSearchWorkspaces();
        UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), 0);
    }

    Y_UNIT_TEST(SearchWorkspacePoolSeparatesConstructionAndSearchRetention) {
        similarity::VisitedListPool pool(8, 128, 8);
        std::vector<similarity::VisitedListPool::Lease> leases;
        for (size_t i = 0; i < 8; ++i)
            leases.push_back(pool.acquire());
        leases.clear();
        UNIT_ASSERT_VALUES_EQUAL(pool.GetStats().Allocations, 8);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetStats().IdleBuffers, 8);

        pool.Configure({});
        UNIT_ASSERT_VALUES_EQUAL(pool.GetStats().IdleBuffers, 0);
        for (size_t i = 0; i < 8; ++i)
            leases.push_back(pool.acquire());
        leases.clear();
        UNIT_ASSERT_VALUES_EQUAL(pool.GetStats().IdleBuffers, 4);
        const auto allocations = pool.GetStats().Allocations;
        for (size_t i = 0; i < 4; ++i)
            leases.push_back(pool.acquire());
        leases.clear();
        UNIT_ASSERT_VALUES_EQUAL(pool.GetStats().Allocations, allocations);
    }

    Y_UNIT_TEST(SearchWorkspacePoolReusesConcurrentBorrowersAcrossMarkerWraparound) {
        constexpr size_t readers = 4;
        constexpr size_t rounds = 300;
        for (size_t retained : {1u, 4u}) {
            similarity::VisitedListPool pool(0, 65536);
            similarity::VisitedListPoolPolicy policy;
            policy.MaxIdleLists = retained;
            pool.Configure(std::move(policy));
            std::barrier barrier(readers);
            std::atomic<bool> clean{true};
            std::vector<std::thread> threads;
            for (size_t i = 0; i < readers; ++i) {
                threads.emplace_back([&] {
                    for (size_t round = 0; round < rounds; ++round) {
                        auto lease = pool.acquire();
                        barrier.arrive_and_wait(); // All buffers must coexist.
                        if (lease->mass[0] == lease->curV)
                            clean = false;
                        lease->mass[0] = lease->curV;
                        lease.reset();
                        barrier.arrive_and_wait(); // All returns precede the next round.
                    }
                });
            }
            for (auto& thread : threads)
                thread.join();
            UNIT_ASSERT(clean.load());
            const auto stats = pool.GetStats();
            const size_t expectedAllocations = readers + (rounds - 1) * (readers - retained);
            UNIT_ASSERT_VALUES_EQUAL(stats.Allocations, expectedAllocations);
            UNIT_ASSERT_VALUES_EQUAL(stats.Reuses, readers * rounds - expectedAllocations);
            UNIT_ASSERT_VALUES_EQUAL(stats.Frees, expectedAllocations - retained);
            UNIT_ASSERT_VALUES_EQUAL(stats.ActiveBuffers, 0);
            UNIT_ASSERT_VALUES_EQUAL(stats.IdleBuffers, retained);
            UNIT_ASSERT_VALUES_EQUAL(stats.RetainedBytes, retained * pool.BufferMemoryBytes());
        }
    }

    Y_UNIT_TEST(SearchWorkspacePoolAdmissionExceptionsAndPressure) {
        similarity::VisitedListPool pool(0, 128);
        const auto bytes = pool.BufferMemoryBytes();
        auto tracker = std::make_shared<THnswCacheMemoryTracker>();
        bool throwReservation = true;
        similarity::VisitedListPoolPolicy policy;
        policy.MaxIdleLists = 4;
        policy.Reserve = [&](size_t size) -> std::shared_ptr<void> {
            // Reentrant statistics verify callbacks run outside the pool lock.
            UNIT_ASSERT_GT(pool.GetStats().ActiveBuffers, 0);
            if (throwReservation)
                throw std::runtime_error("reservation failed");
            if (!tracker->TryAcquire(size))
                return {};
            return std::shared_ptr<void>(new int(0), [tracker, size](void* ptr) {
                delete static_cast<int*>(ptr);
                tracker->Release(size);
            });
        };
        policy.CanRetain = [&] {
            UNIT_ASSERT_GT(pool.GetStats().ActiveBuffers, 0);
            return tracker->GetUsed() <= tracker->GetLimit();
        };
        pool.Configure(std::move(policy));
        UNIT_ASSERT_EXCEPTION(pool.acquire(), std::runtime_error);
        throwReservation = false;
        tracker->SetLimit(bytes - 1);
        UNIT_ASSERT_EXCEPTION(pool.acquire(), similarity::VisitedListMemoryLimitExceeded);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetStats().ActiveBuffers, 0);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetStats().AllocationFailures, 2);
        UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), 0);

        tracker->SetLimit(bytes * 4);
        auto first = pool.acquire();
        auto second = pool.acquire();
        UNIT_ASSERT_EXCEPTION(pool.Configure({}), std::logic_error);
        first.reset();
        UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), bytes * 2);
        tracker->SetLimit(bytes);
        pool.TrimIdle();
        UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), bytes);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetStats().ActiveBuffers, 1);
        second->mass[0] = second->curV; // Trimming must preserve the borrower.
        tracker->SetLimit(0);
        second.reset();
        UNIT_ASSERT_VALUES_EQUAL(pool.GetStats().IdleBuffers, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), 0);

        tracker->SetLimit(bytes * 4);
        try {
            auto lease = pool.acquire();
            throw std::runtime_error("search failed");
        } catch (const std::runtime_error&) {
        }
        UNIT_ASSERT_VALUES_EQUAL(pool.GetStats().ActiveBuffers, 0);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetStats().IdleBuffers, 1);
        UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), bytes);
        pool.TrimIdle();
        UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), 0);
    }

    Y_UNIT_TEST(SearchWorkspacesAreSharedAndAccountedAfterBuildAndImport) {
        const auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
        const auto target = SerializeFloatVector({1, 0});
        TString error;
        auto built = THnswIndex::Build(settings, {{"a", target}, {"b", SerializeFloatVector({0, 1})}}, 1_MB, error);
        UNIT_ASSERT_C(built, error);
        std::stringstream snapshot;
        built->SaveGraph(snapshot);
        auto imported = THnswIndex::LoadGraph(snapshot, settings, 1_MB, error);
        UNIT_ASSERT_C(imported, error);
        for (auto* graph : {built.get(), imported.get()}) {
            auto tracker = std::make_shared<THnswCacheMemoryTracker>();
            tracker->SetLimit(1_MB);
            graph->SetGraphReservation(std::make_shared<int>(0), tracker);
            auto first = graph->CreateView({10, 0}, std::make_shared<THnswIndexChanges>());
            auto second = graph->CreateView({20, 0}, std::make_shared<THnswIndexChanges>());
            const auto before = graph->GetSearchWorkspaceStats();
            for (size_t i = 0; i < 300; ++i) {
                for (const auto* view : {graph, first.get(), second.get()}) {
                    const auto result = view->Search(target, 1, {20, 0});
                    UNIT_ASSERT(result.Covered);
                    UNIT_ASSERT_VALUES_EQUAL(result.Results.front().first, "a");
                }
            }
            const auto stats = graph->GetSearchWorkspaceStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.Allocations - before.Allocations, 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.Reuses - before.Reuses, 899);
            UNIT_ASSERT_VALUES_EQUAL(stats.IdleBuffers, 1);
            UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), stats.RetainedBytes);
            UNIT_ASSERT_VALUES_EQUAL(first->GetSearchWorkspaceStats().RetainedBytes, stats.RetainedBytes);
            first.reset();
            UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), stats.RetainedBytes);
            tracker->SetLimit(0);
            second->TrimSearchWorkspaces();
            UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), 0);
            UNIT_ASSERT(!graph->Search(target, 1).Covered);
            tracker->SetLimit(1_MB);
            UNIT_ASSERT(graph->Search(target, 1).Covered);
        }
    }

    Y_UNIT_TEST(SearchWorkspaceReservationsSurviveUntilLastGraphViewIsDestroyed) {
        const auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
        const auto target = SerializeFloatVector({1, 0});
        TString error;
        auto graph = THnswIndex::Build(settings, {{"a", target}}, 1_MB, error);
        UNIT_ASSERT_C(graph, error);
        auto tracker = std::make_shared<THnswCacheMemoryTracker>();
        tracker->SetLimit(1_MB);
        graph->SetGraphReservation(std::make_shared<int>(0), tracker);
        auto replica = graph->CreateView({10, 0}, std::make_shared<THnswIndexChanges>());
        UNIT_ASSERT(replica->Search(target, 1, {10, 0}).Covered);
        const auto retained = replica->GetSearchWorkspaceStats().RetainedBytes;
        UNIT_ASSERT_GT(retained, 0);
        graph.reset();
        UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), retained);
        replica.reset();
        UNIT_ASSERT_VALUES_EQUAL(tracker->GetUsed(), 0);
    }

    Y_UNIT_TEST(SnapshotRestoresTopologyVectorsAndCanBeExportedAgain) {
        for (const auto metric : {Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
                Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN,
                Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
                Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT}) {
            auto settings = MakeSettings(metric, Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 8);
            settings.set_m(4); // Exercise multiple higher levels in the optimized layout.
            TFastRng<ui64> rng(42);
            std::vector<std::pair<TString, TString>> rows;
            for (ui32 i = 0; i < 1000; ++i)
                rows.emplace_back(KeyFor(i), RandomFloatVector(rng, 8));
            TString error;
            auto original = THnswIndex::Build(settings, rows, 1_MB, error);
            UNIT_ASSERT_C(original, error);
            std::ostringstream out(std::ios::binary);
            original->SaveGraph(out);
            std::istringstream in(out.str(), std::ios::binary);
            auto restored = THnswIndex::LoadGraph(in, settings, original->EstimatedMemoryBytes() + 4096, error);
            UNIT_ASSERT_C(restored, error);
            UNIT_ASSERT(!original->SharesGraphWith(*restored));
            UNIT_ASSERT_VALUES_EQUAL(restored->Size(), original->Size());
            for (const auto& [key, vector] : rows) {
                TString actual;
                UNIT_ASSERT(restored->GetVector(key, actual));
                UNIT_ASSERT_VALUES_EQUAL(actual, vector);
                UNIT_ASSERT_VALUES_EQUAL(restored->Search(vector, 10).Results,
                    original->Search(vector, 10).Results);
            }
            std::ostringstream again(std::ios::binary);
            restored->SaveGraph(again);
            UNIT_ASSERT_VALUES_EQUAL(again.str(), out.str());
        }
    }

    Y_UNIT_TEST(SnapshotRejectsCorruptionTruncationIncompatibilityAndInsufficientMemory) {
        auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
        TString error;
        auto original = THnswIndex::Build(settings, {{"a", SerializeFloatVector({1, 0})}}, 1_MB, error);
        UNIT_ASSERT_C(original, error);
        std::ostringstream out(std::ios::binary);
        original->SaveGraph(out);
        const auto bytes = out.str();
        for (size_t length = 0; length < bytes.size(); ++length) {
            std::istringstream input(bytes.substr(0, length), std::ios::binary);
            UNIT_ASSERT(!THnswIndex::LoadGraph(input, settings, 1_MB, error));
        }
        for (size_t i = 0; i < bytes.size(); ++i) {
            auto damaged = bytes;
            damaged[i] ^= char(0x80);
            std::istringstream input(damaged, std::ios::binary);
            UNIT_ASSERT(!THnswIndex::LoadGraph(input, settings, 1_MB, error));
        }
        std::istringstream tooSmall(bytes, std::ios::binary);
        UNIT_ASSERT(!THnswIndex::LoadGraph(tooSmall, settings, 1, error));
        settings.set_m(24);
        std::istringstream incompatible(bytes, std::ios::binary);
        UNIT_ASSERT(!THnswIndex::LoadGraph(incompatible, settings, 1_MB, error));
    }

    Y_UNIT_TEST(EmptySnapshotRoundTrip) {
        auto settings = MakeSettings(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, 2);
        TString error;
        auto original = THnswIndex::Build(settings, {}, 1_MB, error, true);
        UNIT_ASSERT_C(original, error);
        std::ostringstream out(std::ios::binary);
        original->SaveGraph(out);
        std::istringstream input(out.str(), std::ios::binary);
        auto restored = THnswIndex::LoadGraph(input, settings, 1_MB, error);
        UNIT_ASSERT_C(restored, error);
        UNIT_ASSERT_VALUES_EQUAL(restored->Size(), 0);
        UNIT_ASSERT(restored->Search(SerializeFloatVector({1, 0}), 1).Covered);
    }

    Y_UNIT_TEST(OmittedMinRowsUsesDocumentedDefault) {
        Ydb::Table::VectorIndexSettings settings;
        UNIT_ASSERT_VALUES_EQUAL(GetHnswMinRows(settings), 10000u);
        settings.set_min_rows(0);
        UNIT_ASSERT_VALUES_EQUAL(GetHnswMinRows(settings), 0u);
    }

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

    Y_UNIT_TEST(CacheSettingsIdentityIsNormalizedAndComplete) {
        auto cached = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            2);
        auto requested = cached;
        requested.set_m(16);
        requested.set_ef_construction(200);
        UNIT_ASSERT(AreHnswIndexSettingsCompatible(cached, requested));

        requested.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN);
        UNIT_ASSERT(!AreHnswIndexSettingsCompatible(cached, requested));
        requested = cached;
        requested.set_vector_dimension(3);
        UNIT_ASSERT(!AreHnswIndexSettingsCompatible(cached, requested));
        requested = cached;
        requested.set_m(17);
        UNIT_ASSERT(!AreHnswIndexSettingsCompatible(cached, requested));
    }

    Y_UNIT_TEST(CacheMemoryTrackerBoundariesAndUnderflow) {
        THnswCacheMemoryTracker tracker;
        UNIT_ASSERT(!tracker.HasLimit());
        UNIT_ASSERT(!tracker.TryAcquire(1));
        tracker.SetLimit(0);
        UNIT_ASSERT(tracker.HasLimit());
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
        auto consumer = MakeIntrusive<TTestMemoryConsumer>();
        tracker.SetConsumer(consumer);
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
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Used, 0);
    }

    Y_UNIT_TEST(CacheMemoryTrackerReportsUsageAndDemand) {
        THnswCacheMemoryTracker tracker;
        tracker.SetLimit(100);
        UNIT_ASSERT(tracker.TryAcquire(60));

        auto consumer = MakeIntrusive<TTestMemoryConsumer>();
        tracker.SetConsumer(consumer);
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Used, 60);
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 60);

        tracker.SetReclaimable(100);
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Reclaimable, 60);

        UNIT_ASSERT(!tracker.TryAcquire(50));
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Used, 60);
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 110);

        // Abandoning a partial scan still requires the full graph on retry.
        tracker.Release(60);
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Used, 0);
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 110);
        tracker.SetLimit(110);
        UNIT_ASSERT(tracker.TryAcquire(110));
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Used, 110);
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 110);

        // Shrinking a share must keep reporting memory pinned by readers or builds.
        tracker.SetLimit(0);
        UNIT_ASSERT(!tracker.TryAcquire(1));
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Used, 110);
        tracker.ResetDemand();
        tracker.Release(60);
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Used, 50);
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 50);
        tracker.Release(50);
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Used, 0);
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 0);
        UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Reclaimable, 0);
    }

    Y_UNIT_TEST(CacheMemoryTrackersHaveIndependentLimits) {
        THnswCacheMemoryTracker first;
        THnswCacheMemoryTracker second;
        auto firstConsumer = MakeIntrusive<TTestMemoryConsumer>();
        auto secondConsumer = MakeIntrusive<TTestMemoryConsumer>();
        first.SetConsumer(firstConsumer);
        second.SetConsumer(secondConsumer);
        first.SetLimit(100);
        second.SetLimit(200);
        UNIT_ASSERT(first.TryAcquire(100));
        UNIT_ASSERT(second.TryAcquire(200));
        first.SetLimit(0);
        first.Release(100);
        UNIT_ASSERT_VALUES_EQUAL(firstConsumer->Report.Used, 0);
        UNIT_ASSERT_VALUES_EQUAL(secondConsumer->Report.Used, 200);
        UNIT_ASSERT_VALUES_EQUAL(second.GetLimit(), 200);
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
            std::vector<THnswSearchResult> expected;
            for (ui32 width : widths) {
                expected.push_back(index->Search(target, 64, TRowVersion::Max(), width));
                UNIT_ASSERT_VALUES_EQUAL(expected.back().Results.size(), 64u);
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
        // Both the native pointer links and optimized integer links remain
        // resident. Container overhead may change independently of this bound.
        UNIT_ASSERT_GE(defaultEstimate,
            100 * (2 * 4 * sizeof(float) + 2 * 16 * (sizeof(ui32) + sizeof(void*))));
        UNIT_ASSERT_GT(denseEstimate, defaultEstimate);
        UNIT_ASSERT_GE(denseEstimate - defaultEstimate,
            100 * 2 * (100 - 16) * (sizeof(ui32) + sizeof(void*)));
        UNIT_ASSERT_VALUES_EQUAL(
            THnswIndex::EstimateMemoryBytes(100, 4, 16, 1234),
            defaultEstimate + 1234);
    }

    Y_UNIT_TEST(RejectsUnsafeHnswSettingsBeforeNmslibBuild) {
        auto settings = MakeSettings(
            Ydb::Table::VectorIndexSettings::DISTANCE_COSINE,
            Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT,
            2);
        settings.set_m(NKMeans::MaxHnswM + 1);
        std::vector<std::pair<TString, TString>> data = {
            {"a", SerializeFloatVector({1.0f, 0.0f})},
        };

        TString error;
        auto index = THnswIndex::Build(settings, data, 0, error);
        UNIT_ASSERT(!index);
        UNIT_ASSERT_STRING_CONTAINS(error, "M");
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
        old->SetSnapshot({100, 0}, changes);
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

}

} // namespace NKikimr::NDataShard
