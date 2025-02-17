#include "partition_stats.h"

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/sys_view/common/events.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NSysView {

Y_UNIT_TEST_SUITE(PartitionStats) {

    TTestActorRuntime::TEgg MakeEgg()
    {
        return { new TAppData(0, 0, 0, 0, { }, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {} };
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

}

} // NSysView
} // NKikimr
