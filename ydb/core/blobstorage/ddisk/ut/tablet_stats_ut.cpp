#include "mon_component_test.h"
#include <ydb/core/blobstorage/ddisk/ddisk_mon.h>
#include <ydb/library/actors/core/mon.h>
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
    Y_UNIT_TEST(TopTenUsesIndependentRankingsAndPreservesCookie) {
        TTestActorSystem runtime(1);
        runtime.Start();
        const auto owner = runtime.AllocateEdgeActor(1);
        const auto actor = runtime.Register(CreateTabletStatsActor(owner), 1);
        auto batch = std::make_unique<TEvTabletStatsBatch>();
        for (ui64 id = 1; id <= 20; ++id) {
            TTabletStatsSample sample;
            sample.TabletId = id;
            sample.Chunks = 21 - id;
            sample.Current[0] = {id, id == 7 ? 10000u : id};
            sample.Elapsed = TDuration::Seconds(1);
            batch->Samples.push_back(sample);
        }
        runtime.Send(new IEventHandle(actor, owner, new TEvTabletStatsChanged()), 1);
        runtime.WaitForEdgeActorEvent<TEvCollectTabletStats>(owner, false);
        runtime.Send(new IEventHandle(actor, owner, batch.release()), 1);
        for (const auto& [sort, first] : std::initializer_list<std::pair<TString, ui64>>{
                {"iops", 20}, {"throughput", 7}, {"chunks", 1}}) {
            auto request = std::make_unique<TEvGetTabletStats>();
            request->RankBy = sort;
            request->Limit = 100;
            runtime.Send(new IEventHandle(actor, owner, request.release(), 0, 71), 1);
            const auto reply = runtime.WaitForEdgeActorEvent<TEvTabletStats>(owner, false);
            UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, 71);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Tablets.size(), 10);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Tablets.front().TabletId, first);
            UNIT_ASSERT(!reply->Get()->NextTabletId);
        }
        runtime.Stop();
    }

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

    Y_UNIT_TEST(ActorSharesPaginationSearchAndOther) {
        TTestActorSystem runtime(1);
        runtime.Start();
        const auto owner = runtime.AllocateEdgeActor(1);
        const auto actor = runtime.Register(CreateTabletStatsActor(owner), 1);
        for (ui64 first = 1; first <= 201; first += 100) {
            auto batch = std::make_unique<TEvTabletStatsBatch>();
            for (ui64 id = first; id < first + 100; ++id) {
                TTabletStatsSample sample;
                sample.TabletId = id;
                sample.Chunks = 301 - id;
                sample.Current[0] = {id >= 121 && id <= 150 ? 1000 + id : id, id * 4096};
                sample.Elapsed = TDuration::Seconds(2);
                batch->Samples.push_back(sample);
            }
            runtime.Send(new IEventHandle(actor, owner, new TEvTabletStatsChanged()), 1);
            runtime.WaitForEdgeActorEvent<TEvCollectTabletStats>(owner, false);
            runtime.Send(new IEventHandle(actor, owner, batch.release()), 1);
            runtime.Send(new IEventHandle(actor, owner, new TEvGetTabletStats()), 1);
            runtime.WaitForEdgeActorEvent<TEvTabletStats>(owner, false);
        }
        const auto pageSender = runtime.AllocateEdgeActor(1);
        const auto page = [&](TDDiskMonQuery query) {
            auto request = std::make_unique<TEvGetTabletStatsSnapshot>();
            request->Query.SearchTabletId = query.SearchTabletId;
            request->Query.StatsSelectedTabletId = query.StatsSelectedTabletId;
            request->Query.AfterTabletId = query.AfterTabletId;
            request->Query.StatsOther = query.StatsOther;
            runtime.Send(new IEventHandle(actor, pageSender, request.release(), 0, 71), 1);
            auto reply = runtime.WaitForEdgeActorEvent<TEvTabletStatsSnapshot>(pageSender, false);
            UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, 71);
            TDDiskMonInfo info;
            static_cast<TTabletStatsSnapshot&>(info) = std::move(reply->Get()->Info);
            info.StatsColorSlots = info.ParticipantSlots;
            info.ChunkSize = 1ull << 20;
            TPersistentBufferMonInfo pb;
            pb.ChunkSize = info.ChunkSize;
            pb.PDiskSpace.emplace();
            pb.PDiskSpace->TotalChunks = pb.PDiskSpace->FreeChunks = 100000;
            query.Tab = "tablets";
            return RenderDDiskMonPage(info, &pb, query, {});
        };
        const auto count = [](const TString& html, TStringBuf needle) {
            size_t result = 0;
            for (size_t pos = 0; (pos = html.find(needle, pos)) != TString::npos; pos += needle.size()) {
                ++result;
            }
            return result;
        };
        const auto row = [](ui64 id) { return "data-tablet-row=\"" + ToString(id) + "\""; };
        const auto first = page({});
        UNIT_ASSERT_VALUES_EQUAL(TabletBar(first, "iops")["segments"].GetArraySafe().size() - 1, 90);
        UNIT_ASSERT_VALUES_EQUAL(TabletBar(first, "space")["segments"].GetArraySafe().size() - 5, 90);
        UNIT_ASSERT_VALUES_EQUAL(count(first, "data-tablet-row="), 100);
        const auto color = [](const TString& html, ui64 id, TStringBuf kind) {
            const auto segment = TabletSegment(html, kind, id);
            UNIT_ASSERT(!segment.IsNull());
            return segment["color"].GetString();
        };
        std::set<TString> colors;
        for (ui64 id : {1u, 30u, 271u, 300u}) {
            UNIT_ASSERT_VALUES_EQUAL(color(first, id, "iops"), color(first, id, "space"));
        }
        for (ui64 id = 1; id <= 300; ++id) {
            if (id <= 30 || (id >= 121 && id <= 150) || id >= 271) {
                colors.insert(color(first, id, "iops"));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(colors.size(), 90);

        // All 90 distinct chart leaders precede ordinary rows on the first page.
        for (ui64 id = 271; id <= 300; ++id) {
            UNIT_ASSERT(first.find(row(id)) < first.find(row(31)));
        }
        for (ui64 id = 1; id <= 30; ++id) {
            UNIT_ASSERT(first.find(row(id)) < first.find(row(31)));
        }
        for (ui64 id = 121; id <= 150; ++id) {
            UNIT_ASSERT(first.find(row(id)) < first.find(row(31)));
        }
        UNIT_ASSERT(first.Contains("afterTabletId=40"));
        TDDiskMonQuery query;
        query.AfterTabletId = 40;
        const auto second = page(query);
        UNIT_ASSERT_VALUES_EQUAL(count(second, "data-tablet-row="), 100);
        UNIT_ASSERT(second.Contains(row(41)));
        UNIT_ASSERT(second.Contains(row(170)));
        UNIT_ASSERT(!second.Contains(row(300)));
        // Global charts and denominators do not change with pagination.
        const auto bars = [](const TString& html) {
            const auto begin = html.find("<section class=\"ddisk-tablet-resources\"");
            return html.substr(begin, html.find("<form", begin) - begin);
        };
        UNIT_ASSERT_VALUES_EQUAL(bars(first), bars(second));
        query.AfterTabletId = 170;
        const auto third = page(query);
        UNIT_ASSERT_VALUES_EQUAL(count(third, "data-tablet-row="), 100);
        UNIT_ASSERT(third.Contains(row(171)) && third.Contains(row(270)));
        UNIT_ASSERT(!third.Contains("Next tablets"));
        query.SearchTabletId = 200;
        const auto found = page(query);
        UNIT_ASSERT_VALUES_EQUAL(count(found, "data-tablet-row="), 1);
        UNIT_ASSERT(found.Contains(row(200)));
        UNIT_ASSERT_VALUES_EQUAL(TabletBar(found, "iops")["segments"].GetArraySafe().size() - 1, 88);
        UNIT_ASSERT_VALUES_EQUAL(TabletBar(found, "space")["segments"].GetArraySafe().size() - 5, 88);
        UNIT_ASSERT(!TabletSegment(found, "iops", 200).IsNull());
        UNIT_ASSERT(found.Contains("highlightTabletId=200"));
        UNIT_ASSERT_VALUES_EQUAL(color(first, 300, "iops"), color(found, 300, "iops"));
        query.SearchTabletId = 9999999999999999ull;
        const auto missing = page(query);
        UNIT_ASSERT(missing.Contains("No data for this tablet on this DDisk"));
        UNIT_ASSERT_VALUES_EQUAL(count(missing, "data-tablet-row="), 0);
        query = {};
        query.StatsOther = "throughput";
        const auto other = page(query);
        UNIT_ASSERT(!other.Contains(row(300)));
        UNIT_ASSERT(!other.Contains(row(1)));
        UNIT_ASSERT(other.Contains(row(31)) && other.Contains(row(160)));
        UNIT_ASSERT(other.Contains("210 tablets / 100 per page"));
        UNIT_ASSERT_VALUES_EQUAL(bars(first), bars(other));
        query.StatsSelectedTabletId = 200;
        const auto selectedOther = page(query);
        UNIT_ASSERT(!selectedOther.Contains(row(200)));
        UNIT_ASSERT(selectedOther.Contains("highlightTabletId=200"));
        auto batch = std::make_unique<TEvTabletStatsBatch>();
        TTabletStatsSample retired;
        retired.TabletId = 300;
        retired.Retired = true;
        batch->Samples.push_back(retired);
        runtime.Send(new IEventHandle(actor, owner, new TEvTabletStatsChanged()), 1);
        runtime.WaitForEdgeActorEvent<TEvCollectTabletStats>(owner, false);
        runtime.Send(new IEventHandle(actor, owner, batch.release()), 1);
        runtime.Send(new IEventHandle(actor, owner, new TEvGetTabletStats()), 1);
        runtime.WaitForEdgeActorEvent<TEvTabletStats>(owner, false);
        query = {};
        query.SearchTabletId = 300;
        UNIT_ASSERT(page(query).Contains("No data for this tablet on this DDisk"));
        runtime.Stop();
    }

    Y_UNIT_TEST(ShareSelectionExceedsHalfAndHandlesIdle) {
        TTestActorSystem runtime(1);
        runtime.Start();
        const auto owner = runtime.AllocateEdgeActor(1);
        const auto actor = runtime.Register(CreateTabletStatsActor(owner), 1);
        const auto publish = [&](bool idle) {
            auto batch = std::make_unique<TEvTabletStatsBatch>();
            for (ui64 id = 0; id < 4; ++id) {
                TTabletStatsSample sample;
                sample.TabletId = id;
                sample.Chunks = id ? 1 : 7;
                sample.Current[0] = {id ? 10u : 30u, 0};
                sample.Previous = idle ? sample.Current : std::array<TTabletIoCounters, 3>{};
                sample.Elapsed = TDuration::Seconds(1);
                batch->Samples.push_back(sample);
            }
            runtime.Send(new IEventHandle(actor, owner, new TEvTabletStatsChanged()), 1);
            runtime.WaitForEdgeActorEvent<TEvCollectTabletStats>(owner, false);
            runtime.Send(new IEventHandle(actor, owner, batch.release()), 1);
            runtime.Send(new IEventHandle(actor, owner, new TEvGetTabletStats()), 1);
            runtime.WaitForEdgeActorEvent<TEvTabletStats>(owner, false);
        };
        const auto page = [&]() {
            runtime.Send(new IEventHandle(actor, owner, new TEvGetTabletStatsSnapshot()), 1);
            auto reply = runtime.WaitForEdgeActorEvent<TEvTabletStatsSnapshot>(owner, false);
            TDDiskMonInfo info;
            static_cast<TTabletStatsSnapshot&>(info) = std::move(reply->Get()->Info);
            info.StatsColorSlots = info.ParticipantSlots;
            info.ChunkSize = 1ull << 20;
            TPersistentBufferMonInfo pb;
            pb.ChunkSize = info.ChunkSize;
            pb.PDiskSpace.emplace();
            pb.PDiskSpace->TotalChunks = pb.PDiskSpace->FreeChunks = 100000;
            TDDiskMonQuery query;
            query.Tab = "tablets";
            return RenderDDiskMonPage(info, &pb, query, {});
        };
        publish(false);
        const auto html = page();
        // Exactly 50% is not enough: the second largest is selected too.
        UNIT_ASSERT(!TabletSegment(html, "iops", 0).IsNull());
        UNIT_ASSERT(!TabletSegment(html, "iops", 3).IsNull());
        UNIT_ASSERT(!!TabletSegment(html, "iops", 2).IsNull());
        // Space selects one leader, but must also show the I/O leader separately.
        UNIT_ASSERT(!TabletSegment(html, "space", 0).IsNull());
        UNIT_ASSERT(!TabletSegment(html, "space", 3).IsNull());
        publish(true);
        const auto idle = page();
        UNIT_ASSERT(idle.Contains("No I/O activity"));
        UNIT_ASSERT(TabletBar(idle, "iops")["segments"].GetArraySafe().empty());
        UNIT_ASSERT(!TabletBar(idle, "space")["segments"].GetArraySafe().empty());
        runtime.Stop();
    }

    Y_UNIT_TEST(AnalyticsRendersScopeRatesAndTabletLink) {
        TDDiskMonInfo info;
        info.StatsAvailable = true;
        info.ChunkSize = 1 << 20;
        info.StatsTablets = 1;
        info.StatsChunks = 2;
        info.StatsIops = 10;
        info.StatsBytesPerSecond = 40960;
        info.CollectedAt = TInstant::Seconds(12);
        TDDiskMonTabletStats row;
        row.TabletId = 42;
        row.Chunks = 2;
        row.Rates[0] = {10, 40960};
        row.SampledAt = TInstant::Seconds(11);
        info.TabletStats.push_back(row);
        TDDiskMonQuery query;
        query.Tab = "analytics";
        const auto html = RenderDDiskMonPage(info, nullptr, query, {});
        UNIT_ASSERT(html.Contains("tabletId=42"));
        UNIT_ASSERT(html.Contains("40.00 KiB/s"));
        UNIT_ASSERT(html.Contains("2 (2.00 MiB)"));
        UNIT_ASSERT(html.Contains("100.0%"));
        UNIT_ASSERT(!html.Contains("PersistentBuffer unavailable"));
    }
    Y_UNIT_TEST(ResourceSnapshotIsBoundedAndSearchable) {
        TTestActorSystem runtime(1);
        runtime.Start();
        const auto owner = runtime.AllocateEdgeActor(1);
        const auto reader = runtime.AllocateEdgeActor(1);
        const auto actor = runtime.Register(CreateTabletStatsActor(owner), 1);
        for (ui64 first = 1; first <= 201; first += 100) {
            auto batch = std::make_unique<TEvTabletStatsBatch>();
            for (ui64 id = first; id < first + 100; ++id) {
                TTabletStatsSample sample;
                sample.TabletId = id;
                sample.Chunks = 301 - id;
                sample.Current[0] = {id, id * 4096};
                sample.Elapsed = TDuration::Seconds(1);
                batch->Samples.push_back(sample);
            }
            runtime.Send(new IEventHandle(actor, owner, new TEvTabletStatsChanged()), 1);
            runtime.WaitForEdgeActorEvent<TEvCollectTabletStats>(owner, false);
            runtime.Send(new IEventHandle(actor, owner, batch.release()), 1);
            runtime.Send(new IEventHandle(actor, owner, new TEvGetTabletStats()), 1);
            runtime.WaitForEdgeActorEvent<TEvTabletStats>(owner, false);
        }
        const auto snapshot = [&](std::optional<ui64> search, std::optional<ui64> after = {}) {
            auto request = std::make_unique<TEvGetTabletStatsSnapshot>();
            request->Query.SearchTabletId = search;
            request->Query.AfterTabletId = after;
            runtime.Send(new IEventHandle(actor, reader, request.release(), 0, 71), 1);
            auto reply = runtime.WaitForEdgeActorEvent<TEvTabletStatsSnapshot>(reader, false);
            UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, 71);
            return reply->Get()->Info;
        };
        const auto all = snapshot({});
        UNIT_ASSERT_VALUES_EQUAL(all.StatsTablets, 300);
        UNIT_ASSERT_VALUES_EQUAL(all.StatsChunks, 45150);
        UNIT_ASSERT_VALUES_EQUAL(all.TabletStats.size(), 100);
        UNIT_ASSERT(all.StatsShares.size() <= 90);
        UNIT_ASSERT_VALUES_EQUAL(all.ParticipantSlots.size(), all.StatsShares.size());
        UNIT_ASSERT(all.StatsNextTabletId);
        std::set<ui64> visited;
        auto page = all;
        do {
            for (const auto& row : page.TabletStats) {
                UNIT_ASSERT(visited.insert(row.TabletId).second);
            }
            if (!page.StatsNextTabletId) {
                break;
            }
            page = snapshot({}, page.StatsNextTabletId);
        } while (true);
        UNIT_ASSERT_VALUES_EQUAL(visited.size(), 300);
        for (ui64 id = 1; id <= 300; ++id) {
            UNIT_ASSERT(visited.contains(id));
        }
        const auto found = snapshot(200);
        UNIT_ASSERT_VALUES_EQUAL(found.TabletStats.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(found.TabletStats.front().TabletId, 200);
        UNIT_ASSERT_VALUES_EQUAL(found.StatsChunks, all.StatsChunks);
        UNIT_ASSERT_VALUES_EQUAL(found.StatsIops, all.StatsIops);
        const auto missing = snapshot(999);
        UNIT_ASSERT(missing.TabletStats.empty());
        auto batch = std::make_unique<TEvTabletStatsBatch>();
        TTabletStatsSample updated;
        updated.TabletId = 1;
        updated.Chunks = 1000;
        updated.Current[0] = {10000, 10000 * 4096};
        updated.Elapsed = TDuration::Seconds(1);
        batch->Samples.push_back(updated);
        TTabletStatsSample retired;
        retired.TabletId = 300;
        retired.Retired = true;
        batch->Samples.push_back(retired);
        runtime.Send(new IEventHandle(actor, owner, new TEvTabletStatsChanged()), 1);
        runtime.WaitForEdgeActorEvent<TEvCollectTabletStats>(owner, false);
        runtime.Send(new IEventHandle(actor, owner, batch.release()), 1);
        runtime.Send(new IEventHandle(actor, owner, new TEvGetTabletStats()), 1);
        runtime.WaitForEdgeActorEvent<TEvTabletStats>(owner, false);
        const auto changed = snapshot({});
        UNIT_ASSERT_VALUES_EQUAL(changed.StatsTablets, 299);
        UNIT_ASSERT_VALUES_EQUAL(changed.StatsChunks, 45150 - 300 + 1000 - 1);
        UNIT_ASSERT_DOUBLES_EQUAL(changed.StatsIops, 45150 - 1 + 10000 - 300, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(changed.StatsBytesPerSecond, changed.StatsIops * 4096, 1e-6);
        UNIT_ASSERT_VALUES_EQUAL(changed.TabletStats.front().TabletId, 1);
        UNIT_ASSERT(snapshot(300).TabletStats.empty());
        runtime.Stop();
    }

}

} // namespace NKikimr::NDDisk
