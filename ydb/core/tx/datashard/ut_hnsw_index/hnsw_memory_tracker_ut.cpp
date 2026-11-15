#include <ydb/core/tx/datashard/hnsw_index/hnsw_index.h>
#include <ydb/core/tx/datashard/hnsw_index/hnsw_memory_tracker.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <atomic>
#include <thread>

namespace NKikimr::NDataShard {
    namespace {
        class TTestMemoryConsumer: public NMemory::IMemoryConsumer {
        public:
            void SetReport(NMemory::TConsumerReport report) override {
                Report = report;
            }

            NMemory::TConsumerReport Report;
        };
    } // namespace

    Y_UNIT_TEST_SUITE(HnswMemoryTracker) {
        Y_UNIT_TEST(OmittedMinRowsUsesDocumentedDefault) {
            Ydb::Table::VectorIndexSettings settings;
            UNIT_ASSERT_VALUES_EQUAL(GetHnswMinRows(settings), 10000u);
            settings.set_min_rows(0);
            UNIT_ASSERT_VALUES_EQUAL(GetHnswMinRows(settings), 0u);
        }

        Y_UNIT_TEST(CacheSettingsIdentityIsNormalizedAndComplete) {
            Ydb::Table::VectorIndexSettings cached;
            cached.set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
            cached.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
            cached.set_vector_dimension(2);
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
        }

        Y_UNIT_TEST(IndexDemandSurvivesSiblingCompletionAndPartialRelease) {
            THnswCacheMemoryTracker tracker;
            auto consumer = MakeIntrusive<TTestMemoryConsumer>();
            tracker.SetConsumer(consumer);
            tracker.SetLimit(100);
            UNIT_ASSERT(tracker.TryAcquire(30, 1));
            UNIT_ASSERT(tracker.TryAcquire(40, 2));
            UNIT_ASSERT(!tracker.TryAcquire(70, 1)); // Owner 1 needs 100 total.
            UNIT_ASSERT(!tracker.TryAcquire(50, 2)); // Owner 2 needs 90 total.
            UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 190u);
            tracker.Release(40, 2);
            tracker.ResetDemand(2);
            UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 100u);
            tracker.Release(30, 1);
            UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 100u);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetUsed(1), 0u);
            tracker.ResetDemand(1);
            UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 0u);
            UNIT_ASSERT(!tracker.TryAcquire(Max<ui64>(), 1));
            UNIT_ASSERT(!tracker.TryAcquire(Max<ui64>(), 2));
            UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, Max<ui64>());
        }

        Y_UNIT_TEST(IdleDemandIsZeroAndAbandonedRequestsReleaseTheirShare) {
            THnswCacheMemoryTracker tracker(true);
            auto consumer = MakeIntrusive<TTestMemoryConsumer>();
            tracker.SetConsumer(consumer);
            UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 0);
            tracker.Request(4_MB, 1);
            UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 4_MB);
            tracker.ResetDemand(1);
            UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 0);
            tracker.SetLimit(1_MB);
            UNIT_ASSERT(tracker.TryAcquire(1_MB, 1));
            UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 1_MB + 128_KB);
            tracker.Release(1_MB, 1);
            UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Used, 0);
            UNIT_ASSERT_VALUES_EQUAL(consumer->Report.Demand, 0);
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
    } // Y_UNIT_TEST_SUITE(HnswMemoryTracker)

} // namespace NKikimr::NDataShard
