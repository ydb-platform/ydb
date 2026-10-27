#include <ydb/core/tx/datashard/hnsw_index.h>
#include <ydb/core/tx/datashard/hnsw_memory_tracker.h>
#include <library/cpp/testing/unittest/registar.h>

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
