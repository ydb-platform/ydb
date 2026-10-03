#include <ydb/core/blobstorage/ddisk/tablet_stats.h>
#include <ydb/core/blobstorage/ddisk/tablet_stats_actor.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <library/cpp/testing/unittest/registar.h>
#include <set>

namespace NKikimr::NDDisk {
namespace {
struct TTestTabletState {
    TTabletStatsEntry Stats;
    bool HasChunkState = false;
    bool CanRetire() const { return !HasChunkState; }
};
}

Y_UNIT_TEST_SUITE(TDDiskTabletStats) {
    Y_UNIT_TEST(BoundedCollectionAndCooldown) {
        THashMap<ui64, TTestTabletState> tablets;
        TTabletStatsTracker stats(&tablets);
        const auto start = TMonotonic::Seconds(10);
        for (ui64 id = 0; id < 250; ++id) {
            stats.AddIo(id, ETabletOperation::Read, 10, 40960, start);
        }
        UNIT_ASSERT(stats.Collect(start + TDuration::MilliSeconds(999)).empty());
        UNIT_ASSERT_VALUES_EQUAL(stats.Collect(start + TDuration::Seconds(1)).size(), 100);
        UNIT_ASSERT_VALUES_EQUAL(stats.Collect(start + TDuration::Seconds(1)).size(), 100);
        UNIT_ASSERT_VALUES_EQUAL(stats.Collect(start + TDuration::Seconds(1)).size(), 50);
        UNIT_ASSERT(stats.Collect(start + TDuration::Seconds(1)).empty());
    }

    Y_UNIT_TEST(SleepWakeAndActualInterval) {
        THashMap<ui64, TTestTabletState> tablets;
        TTabletStatsTracker stats(&tablets);
        const auto start = TMonotonic::Seconds(10);
        stats.AddChunks(42, 3, start);
        stats.AddIo(42, ETabletOperation::Sync, 8, 32768, start);
        auto batch = stats.Collect(start + TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(batch.size(), 1);
        auto rate = CalculateTabletIoRate(batch[0].Previous[2], batch[0].Current[2], batch[0].Elapsed);
        UNIT_ASSERT_VALUES_EQUAL(rate.Iops, 4);
        UNIT_ASSERT_VALUES_EQUAL(rate.BytesPerSecond, 16384);
        stats.Collect(start + TDuration::Seconds(3));
        UNIT_ASSERT(stats.NextDeadline());
        batch = stats.Collect(start + TDuration::Seconds(4));
        UNIT_ASSERT(!stats.NextDeadline());
        UNIT_ASSERT(!batch[0].Retired);
        UNIT_ASSERT_VALUES_EQUAL(batch[0].Chunks, 3);
        stats.AddIo(42, ETabletOperation::Read, 7, 700, start + TDuration::Hours(1));
        batch = stats.Collect(start + TDuration::Hours(1) + TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(batch[0].Elapsed, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(batch[0].Current[0].Requests - batch[0].Previous[0].Requests, 7);
    }

    Y_UNIT_TEST(IndependentSleepAndDeletion) {
        THashMap<ui64, TTestTabletState> tablets;
        TTabletStatsTracker stats(&tablets);
        const auto start = TMonotonic::Seconds(10);
        stats.AddChunks(1, 2, start);
        stats.AddIo(2, ETabletOperation::Write, 1, 100, start);
        for (int second = 1; second <= 3; ++second) {
            stats.AddIo(1, ETabletOperation::Read, 1, 100, start + TDuration::Seconds(second));
            auto batch = stats.Collect(start + TDuration::Seconds(second));
            if (second == 3) {
                UNIT_ASSERT_VALUES_EQUAL(batch.size(), 2);
                UNIT_ASSERT(batch[1].Retired);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(stats.Size(), 1);
        stats.AddChunks(1, -2, start + TDuration::Seconds(3));
        auto batch = stats.Collect(start + TDuration::Seconds(4));
        UNIT_ASSERT_VALUES_EQUAL(batch[0].Chunks, 0);
        stats.Collect(start + TDuration::Seconds(5));
        stats.Collect(start + TDuration::Seconds(6));
        UNIT_ASSERT_VALUES_EQUAL(stats.Size(), 0);
        UNIT_ASSERT(!stats.NextDeadline());
    }

    Y_UNIT_TEST(IdleAllocatedTabletPublishesZeroWhileAnotherStaysActive) {
        THashMap<ui64, TTestTabletState> tablets;
        TTabletStatsTracker stats(&tablets);
        const auto start = TMonotonic::Seconds(10);
        stats.AddChunks(1, 2, start);
        stats.AddChunks(2, 3, start);
        stats.AddIo(2, ETabletOperation::Read, 10, 1000, start);
        for (int second = 1; second <= 5; ++second) {
            stats.AddIo(1, ETabletOperation::Write, 1, 100, start + TDuration::Seconds(second));
            const auto batch = stats.Collect(start + TDuration::Seconds(second));
            UNIT_ASSERT_VALUES_EQUAL(batch.size(), second <= 3 ? 2 : 1);
            if (second == 2 || second == 3) {
                const auto& idle = batch[1];
                UNIT_ASSERT_VALUES_EQUAL(idle.TabletId, 2);
                UNIT_ASSERT_VALUES_EQUAL(idle.Chunks, 3);
                UNIT_ASSERT(!idle.Retired);
                const auto rate = CalculateTabletIoRate(idle.Previous[0], idle.Current[0], idle.Elapsed);
                UNIT_ASSERT_VALUES_EQUAL(rate.Iops, 0);
                UNIT_ASSERT_VALUES_EQUAL(rate.BytesPerSecond, 0);
            }
            UNIT_ASSERT(stats.NextDeadline());
        }
        UNIT_ASSERT_VALUES_EQUAL(stats.Size(), 2);
    }

    Y_UNIT_TEST(ConnectedIdleTabletsSleepWithoutRetirement) {
        THashMap<ui64, TTestTabletState> tablets;
        TTabletStatsTracker stats(&tablets);
        auto now = TMonotonic::Seconds(10);
        stats.AddSessions(42, 1, now);
        stats.AddSessions(42, 1, now);
        const auto sleep = [&] {
            std::vector<TTabletStatsSample> batch;
            for (int i = 0; i < 3; ++i) {
                now += TDuration::Seconds(1);
                batch = stats.Collect(now);
                UNIT_ASSERT_VALUES_EQUAL(batch.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(batch[0].Chunks, 0);
                UNIT_ASSERT_VALUES_EQUAL(batch[0].Current[0].Requests, 0);
            }
            UNIT_ASSERT(!stats.NextDeadline());
            return batch[0].Retired;
        };
        UNIT_ASSERT(!sleep());
        UNIT_ASSERT_VALUES_EQUAL(stats.Size(), 1);
        stats.AddSessions(42, -1, now);
        UNIT_ASSERT(!sleep());
        stats.AddSessions(42, -1, now);
        UNIT_ASSERT(sleep());
        UNIT_ASSERT_VALUES_EQUAL(stats.Size(), 0);
    }

    Y_UNIT_TEST(RateNormalizationAndCounterReset) {
        constexpr ui64 baseline = 1ull << 60;
        const auto rate = CalculateTabletIoRate({baseline, baseline}, {baseline + 3, baseline + 1000},
            TDuration::MilliSeconds(250));
        UNIT_ASSERT_VALUES_EQUAL(rate.Iops, 12);
        UNIT_ASSERT_VALUES_EQUAL(rate.BytesPerSecond, 4000);
        UNIT_ASSERT_VALUES_EQUAL(CalculateTabletIoRate({}, {1, 1}, {}).Iops, 0);
        UNIT_ASSERT_VALUES_EQUAL(CalculateTabletIoRate({2, 2}, {1, 3}, TDuration::Seconds(1)).BytesPerSecond, 0);
    }

    Y_UNIT_TEST(BacklogIsNotIdleAndChangesDuringCollectionSurvive) {
        THashMap<ui64, TTestTabletState> tablets;
        TTabletStatsTracker stats(&tablets);
        const auto start = TMonotonic::Seconds(10);
        for (ui64 id = 0; id < 251; ++id) {
            stats.AddChunks(id, 1, start);
        }
        for (int second = 1; second <= 3; ++second) {
            std::set<ui64> seen;
            for (int batch = 0; batch < 3; ++batch) {
                for (const auto& sample : stats.Collect(start + TDuration::Seconds(second))) {
                    UNIT_ASSERT(seen.insert(sample.TabletId).second);
                }
                if (second == 1 && batch == 0) {
                    stats.AddIo(0, ETabletOperation::Write, 9, 900, start + TDuration::Seconds(1));
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(seen.size(), 251);
        }
        UNIT_ASSERT(stats.NextDeadline()); // Tablet 0 changed after its first snapshot.
        auto batch = stats.Collect(start + TDuration::Seconds(4));
        UNIT_ASSERT_VALUES_EQUAL(batch.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(batch[0].TabletId, 0);
        UNIT_ASSERT(!stats.NextDeadline());
        stats.AddChunks(250, -1, start + TDuration::Seconds(4));
        batch = stats.Collect(start + TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(batch[0].Chunks, 0);
    }

    Y_UNIT_TEST(ChunkStateSurvivesSleepingStatsAndRetiresAfterDeletion) {
        THashMap<ui64, TTestTabletState> tablets;
        TTabletStatsTracker stats(&tablets);
        const auto start = TMonotonic::Seconds(10);
        tablets[42].HasChunkState = true;
        stats.AddIo(42, ETabletOperation::Read, 1, 4096, start);
        for (int second = 1; second <= 3; ++second) {
            const auto batch = stats.Collect(start + TDuration::Seconds(second));
            UNIT_ASSERT_VALUES_EQUAL(batch.size(), 1);
            UNIT_ASSERT(!batch[0].Retired);
        }
        UNIT_ASSERT_VALUES_EQUAL(tablets.size(), 1);
        UNIT_ASSERT(!stats.NextDeadline());
        tablets.at(42).HasChunkState = false;
        stats.AddChunks(42, 0, start + TDuration::Seconds(4));
        for (int second = 5; second <= 7; ++second) {
            const auto batch = stats.Collect(start + TDuration::Seconds(second));
            UNIT_ASSERT_VALUES_EQUAL(batch.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batch[0].Retired, second == 7);
        }
        UNIT_ASSERT(tablets.empty());
    }

    Y_UNIT_TEST(ActorPaginationCookiesNormalizationAndRetirement) {
        TTestActorSystem runtime(1);
        runtime.Start();
        const auto owner = runtime.AllocateEdgeActor(1);
        const auto reader = runtime.AllocateEdgeActor(1);
        const auto actor = runtime.Register(CreateTabletStatsActor(owner), 1);
        const auto publish = [&](std::unique_ptr<TEvTabletStatsBatch> batch) {
            runtime.Send(new IEventHandle(actor, owner, new TEvTabletStatsChanged()), 1);
            runtime.WaitForEdgeActorEvent<TEvCollectTabletStats>(owner, false);
            runtime.Send(new IEventHandle(actor, owner, batch.release()), 1);
            runtime.Send(new IEventHandle(actor, owner, new TEvGetTabletStats()), 1);
            runtime.WaitForEdgeActorEvent<TEvTabletStats>(owner, false);
        };
        for (ui64 first = 1; first <= 201; first += 100) {
            auto batch = std::make_unique<TEvTabletStatsBatch>();
            batch->SampledAt = TInstant::Seconds(12);
            for (ui64 id = first; id < first + 100 && id <= 251; ++id) {
                TTabletStatsSample sample;
                sample.TabletId = id;
                sample.Chunks = 2;
                sample.Current[2] = {3, 1000};
                sample.Elapsed = TDuration::MilliSeconds(250);
                batch->Samples.push_back(sample);
            }
            publish(std::move(batch));
        }
        const auto query = [&](std::optional<ui64> after, std::optional<ui64> id, ui32 limit) {
            auto request = std::make_unique<TEvGetTabletStats>();
            request->AfterTabletId = after;
            request->TabletId = id;
            request->Limit = limit;
            runtime.Send(new IEventHandle(actor, reader, request.release(), 0, 71), 1);
            auto reply = runtime.WaitForEdgeActorEvent<TEvTabletStats>(reader, false);
            UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, 71);
            return reply;
        };
        auto reply = query({}, {}, 1000);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Tablets.size(), 100);
        UNIT_ASSERT_VALUES_EQUAL(*reply->Get()->NextTabletId, 100);
        reply = query(100, {}, 100);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Tablets.front().TabletId, 101);
        UNIT_ASSERT_VALUES_EQUAL(*reply->Get()->NextTabletId, 200);
        reply = query(200, {}, 100);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Tablets.size(), 51);
        UNIT_ASSERT(!reply->Get()->NextTabletId);
        reply = query({}, 42, 100);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Tablets.size(), 1);
        const auto row = reply->Get()->Tablets.front();
        UNIT_ASSERT_VALUES_EQUAL(row.Rates[2].Iops, 12);
        UNIT_ASSERT_VALUES_EQUAL(row.Rates[2].BytesPerSecond, 4000);
        UNIT_ASSERT_VALUES_EQUAL(row.DataMappedChunks, 2);
        UNIT_ASSERT_VALUES_EQUAL(row.SampledAt, TInstant::Seconds(12));
        UNIT_ASSERT_VALUES_EQUAL(row.Interval, TDuration::MilliSeconds(250));
        auto batch = std::make_unique<TEvTabletStatsBatch>();
        TTabletStatsSample sample;
        sample.TabletId = 42;
        sample.Retired = true;
        batch->Samples.push_back(sample);
        publish(std::move(batch));
        UNIT_ASSERT(query({}, 42, 100)->Get()->Tablets.empty());
        UNIT_ASSERT_VALUES_EQUAL(query({}, {}, 0)->Get()->Tablets.size(), 1);
        runtime.Stop();
    }

}
} // namespace NKikimr::NDDisk
