#include "partition_stats.h"

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/sys_view/common/events.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NSysView {

Y_UNIT_TEST_SUITE(PartitionStats) {

    TTestActorRuntime::TEgg MakeEgg()
    {
        return { new TAppData(0, 0, 0, 0, { }, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {}, {} };
    }

    void WaitForBootstrap(TTestActorRuntime &runtime) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(runtime.DispatchEvents(options));
    }

    void TestCollector(size_t batchSize) {
        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());

        auto collector = CreatePartitionStatsCollector(batchSize);
        auto collectorId = runtime.Register(collector.Release());
        WaitForBootstrap(runtime);

        auto sender = runtime.AllocateEdgeActor();

        auto domainKey = TPathId(1, 1);

        for (ui64 ownerId = 0; ownerId <= 1; ++ownerId) {
            for (ui64 pathId = 0; pathId <= 1; ++pathId) {
                auto id = TPathId(ownerId, pathId);
                auto setPartitioning = MakeHolder<TEvSysView::TEvSetPartitioning>(domainKey, id, "");
                setPartitioning->ShardIndices.push_back(TShardIdx(ownerId * 3 + pathId, 0));
                setPartitioning->ShardIndices.push_back(TShardIdx(ownerId * 3 + pathId, 1));
                runtime.Send(new IEventHandle(collectorId, TActorId(), setPartitioning.Release()));
            }
        }

        auto test = [&] (
            TMaybe<ui64> fromOwnerId, TMaybe<ui64> fromPathId, TMaybe<ui64> fromPartIdx, bool fromInc,
            TMaybe<ui64> toOwnerId, TMaybe<ui64> toPathId, TMaybe<ui64> toPartIdx, bool toInc,
            std::initializer_list<std::tuple<ui64, ui64, ui64>> check)
        {
            auto get = MakeHolder<TEvSysView::TEvGetPartitionStats>();
            auto& record = get->Record;
            record.SetDomainKeyOwnerId(domainKey.OwnerId);
            record.SetDomainKeyPathId(domainKey.LocalPathId);

            if (fromOwnerId) {
                record.MutableFrom()->SetOwnerId(*fromOwnerId);
            }
            if (fromPathId) {
                record.MutableFrom()->SetPathId(*fromPathId);
            }
            if (fromPartIdx) {
                record.MutableFrom()->SetPartIdx(*fromPartIdx);
            }
            record.SetFromInclusive(fromInc);

            if (toOwnerId) {
                record.MutableTo()->SetOwnerId(*toOwnerId);
            }
            if (toPathId) {
                record.MutableTo()->SetPathId(*toPathId);
            }
            if (toPartIdx) {
                record.MutableTo()->SetPartIdx(*toPartIdx);
            }
            record.SetToInclusive(toInc);

            auto checkIt = check.begin();

            while (true) {
                runtime.Send(new IEventHandle(collectorId, sender, get.Release()));

                TAutoPtr<IEventHandle> handle;
                auto result = runtime.GrabEdgeEvent<TEvSysView::TEvGetPartitionStatsResult>(handle);

                for (size_t i = 0; i < result->Record.StatsSize(); ++i, ++checkIt) {
                    auto& stats = result->Record.GetStats(i);
                    UNIT_ASSERT(checkIt != check.end());
                    UNIT_ASSERT_VALUES_EQUAL(stats.GetKey().GetOwnerId(), std::get<0>(*checkIt));
                    UNIT_ASSERT_VALUES_EQUAL(stats.GetKey().GetPathId(), std::get<1>(*checkIt));
                    UNIT_ASSERT_VALUES_EQUAL(stats.GetKey().GetPartIdx(), std::get<2>(*checkIt));
                }

                if (result->Record.GetLastBatch()) {
                    break;
                }

                get = MakeHolder<TEvSysView::TEvGetPartitionStats>();
                auto& record = get->Record;
                record.SetDomainKeyOwnerId(domainKey.OwnerId);
                record.SetDomainKeyPathId(domainKey.LocalPathId);

                record.MutableFrom()->CopyFrom(result->Record.GetNext());
                record.SetFromInclusive(true);

                if (toOwnerId) {
                    record.MutableTo()->SetOwnerId(*toOwnerId);
                }
                if (toPathId) {
                    record.MutableTo()->SetPathId(*toPathId);
                }
                if (toPartIdx) {
                    record.MutableTo()->SetPartIdx(*toPartIdx);
                }
                record.SetToInclusive(toInc);
            }

            UNIT_ASSERT(checkIt == check.end());
        };

        test({}, {}, {}, false, {}, {}, {}, false, {
            {0, 0, 0},
            {0, 0, 1},
            {0, 1, 0},
            {0, 1, 1},
            {1, 0, 0},
            {1, 0, 1},
            {1, 1, 0},
            {1, 1, 1},
        });

        test(0, {}, {}, true, 1, {}, {}, false, {
            {0, 0, 0},
            {0, 0, 1},
            {0, 1, 0},
            {0, 1, 1},
        });

        test(0, {}, {}, false, 1, {}, {}, true, {
            {1, 0, 0},
            {1, 0, 1},
            {1, 1, 0},
            {1, 1, 1},
        });

        test(0, 1, {}, true, 1, 1, {}, false, {
            {0, 1, 0},
            {0, 1, 1},
            {1, 0, 0},
            {1, 0, 1},
        });

        test(0, 0, {}, false, 1, 0, {}, true, {
            {0, 1, 0},
            {0, 1, 1},
            {1, 0, 0},
            {1, 0, 1},
        });

        test(0, 0, 1, true, 1, 0, 1, false, {
            {0, 0, 1},
            {0, 1, 0},
            {0, 1, 1},
            {1, 0, 0},
        });

        test(0, 0, 1, false, 1, 0, 1, true, {
            {0, 1, 0},
            {0, 1, 1},
            {1, 0, 0},
            {1, 0, 1},
        });
    }

    Y_UNIT_TEST(Collector) {
        for (size_t batchSize = 1; batchSize < 9; ++batchSize) {
            TestCollector(batchSize);
        }
    }

    Y_UNIT_TEST(CollectorOverload) {
        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());

        auto collector = CreatePartitionStatsCollector(1, 0);
        auto collectorId = runtime.Register(collector.Release());
        WaitForBootstrap(runtime);

        auto sender = runtime.AllocateEdgeActor();

        auto domainKey = TPathId(1, 1);

        auto get = MakeHolder<TEvSysView::TEvGetPartitionStats>();
        auto& record = get->Record;
        record.SetDomainKeyOwnerId(domainKey.OwnerId);
        record.SetDomainKeyPathId(domainKey.LocalPathId);

        runtime.Send(new IEventHandle(collectorId, sender, get.Release()));

        TAutoPtr<IEventHandle> handle;
        auto result = runtime.GrabEdgeEvent<TEvSysView::TEvGetPartitionStatsResult>(handle);

        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetOverloaded(), true);
    }

    template <typename TPartition>
    const TPartition& FindPartition(const google::protobuf::RepeatedPtrField<TPartition>& partitions, ui64 tabletId, ui32 followerId) {
        for (const auto& partition : partitions) {
            if (partition.GetTabletId() == tabletId && partition.GetFollowerId() == followerId) {
                return partition;
            }
        }
        UNIT_FAIL("No partition for tablet " << tabletId << " follower " << followerId);
        Y_UNREACHABLE();
    }

    Y_UNIT_TEST(CollectorOverloadedFollowerLaggingLeader) {
        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());
        runtime.GetAppData().UsePartitionStatsCollectorForTests = true;

        auto pipeCache = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCache);

        auto collector = CreatePartitionStatsCollector();
        auto collectorId = runtime.Register(collector.Release());
        runtime.EnableScheduleForActor(collectorId);
        WaitForBootstrap(runtime);

        const ui64 schemeShardId = 1;
        auto domainKey = TPathId(schemeShardId, 1);
        auto pathId = TPathId(schemeShardId, 2);
        auto laggingShard = TShardIdx(schemeShardId, 0);
        auto upToDateShard = TShardIdx(schemeShardId, 1);

        runtime.Send(new IEventHandle(collectorId, TActorId(),
            new TEvSysView::TEvInitPartitionStatsCollector(domainKey, 1)));

        auto setPartitioning = MakeHolder<TEvSysView::TEvSetPartitioning>(domainKey, pathId, "/Root/Table");
        setPartitioning->ShardIndices.push_back(laggingShard);
        setPartitioning->ShardIndices.push_back(upToDateShard);
        runtime.Send(new IEventHandle(collectorId, TActorId(), setPartitioning.Release()));

        auto sendStats = [&](TShardIdx shardIdx, ui64 tabletId, ui32 followerId, double cpuCores, ui64 dataSize) {
            auto ev = MakeHolder<TEvSysView::TEvSendPartitionStats>(domainKey, pathId, shardIdx);
            ev->Stats.SetTabletId(tabletId);
            ev->Stats.SetFollowerId(followerId);
            ev->Stats.SetCPUCores(cpuCores);
            ev->Stats.SetLocksBroken(1);
            ev->Stats.SetDataSize(dataSize);
            ev->Stats.SetRowCount(dataSize);
            ev->Stats.SetIndexSize(dataSize);
            runtime.Send(new IEventHandle(collectorId, TActorId(), ev.Release()));
        };

        const ui64 laggingTabletId = 100;
        const ui64 upToDateTabletId = 200;
        const ui32 leader = 0;
        const ui32 follower = 1;
        const double leaderCpuCores = 0.5;
        const double followerCpuCores = 1.0;
        const ui64 leaderDataSize = 1000;
        const ui64 followerDataSize = 10;

        // Follower stats arrive before any stats from the leader
        sendStats(laggingShard, laggingTabletId, follower, followerCpuCores, followerDataSize);

        sendStats(upToDateShard, upToDateTabletId, leader, leaderCpuCores, leaderDataSize);
        sendStats(upToDateShard, upToDateTabletId, follower, followerCpuCores, followerDataSize);

        auto forward = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
        UNIT_ASSERT_VALUES_EQUAL(forward->Get()->Ev->Type(), (ui32)TEvSysView::EvSendTopPartitions);
        const auto& top = static_cast<TEvSysView::TEvSendTopPartitions*>(forward->Get()->Ev.Get())->Record;

        UNIT_ASSERT_VALUES_EQUAL(top.PartitionsByCpuSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(top.PartitionsByTliSize(), 3);

        {
            // Size fields are empty until the leader reports
            const auto& byCpu = FindPartition(top.GetPartitionsByCpu(), laggingTabletId, follower);
            UNIT_ASSERT_VALUES_EQUAL(byCpu.GetPath(), "/Root/Table");
            UNIT_ASSERT_VALUES_EQUAL(byCpu.GetCPUCores(), followerCpuCores);
            UNIT_ASSERT_VALUES_EQUAL(byCpu.GetDataSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(byCpu.GetRowCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(byCpu.GetIndexSize(), 0);

            const auto& byTli = FindPartition(top.GetPartitionsByTli(), laggingTabletId, follower);
            UNIT_ASSERT_VALUES_EQUAL(byTli.GetLocksBroken(), 1);
            UNIT_ASSERT_VALUES_EQUAL(byTli.GetDataSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(byTli.GetRowCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(byTli.GetIndexSize(), 0);
        }

        {
            // Size fields of a follower are taken from the leader
            const auto& byCpu = FindPartition(top.GetPartitionsByCpu(), upToDateTabletId, follower);
            UNIT_ASSERT_VALUES_EQUAL(byCpu.GetDataSize(), leaderDataSize);
            UNIT_ASSERT_VALUES_EQUAL(byCpu.GetRowCount(), leaderDataSize);
            UNIT_ASSERT_VALUES_EQUAL(byCpu.GetIndexSize(), leaderDataSize);

            const auto& byTli = FindPartition(top.GetPartitionsByTli(), upToDateTabletId, follower);
            UNIT_ASSERT_VALUES_EQUAL(byTli.GetDataSize(), leaderDataSize);
            UNIT_ASSERT_VALUES_EQUAL(byTli.GetRowCount(), leaderDataSize);
            UNIT_ASSERT_VALUES_EQUAL(byTli.GetIndexSize(), leaderDataSize);
        }
    }

}

} // NSysView
} // NKikimr
