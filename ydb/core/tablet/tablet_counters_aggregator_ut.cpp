#include "tablet_counters_aggregator.h"
#include "private/labeled_db_counters.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/interconnect.h>

namespace NKikimr {

using namespace NActors;

void TestHeavy(const ui32 v, ui32 numWorkers) {

    TInstant t(Now());

    TVector<TActorId> cc;
    TActorId aggregatorId;
    TTestBasicRuntime runtime(1);
    constexpr int NODES = 10;
    constexpr int GROUPS = 1000;
    constexpr int VALUES = 20;

    runtime.Initialize(TAppPrepare().Unwrap());
    TActorId edge = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TABLET_AGGREGATOR, NActors::NLog::PRI_DEBUG);

    IActor* aggregator = CreateClusterLabeledCountersAggregatorActor(edge, TTabletTypes::PersQueue, v, TString(), numWorkers);
    aggregatorId = runtime.Register(aggregator);

    if (numWorkers == 0) {
        cc.push_back(aggregatorId);
        ++numWorkers;
    }

    runtime.SetRegistrationObserverFunc([&cc, &aggregatorId](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                TTestActorRuntime::DefaultRegistrationObserver(runtime, parentId, actorId);
                if (parentId == aggregatorId) {
                    cc.push_back(actorId);
                }
            });

    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, numWorkers);
    runtime.DispatchEvents(options);
    for (const auto& a : cc) {
        auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
        for (auto i = 1; i <= NODES; ++i) {
            nodes->emplace_back(TEvInterconnect::TNodeInfo(i, "::", "localhost", "localhost", 1234, TNodeLocation()));
        }
        THolder<TEvInterconnect::TEvNodesInfo> nodesInfo = MakeHolder<TEvInterconnect::TEvNodesInfo>(nodes);
        runtime.Send(new NActors::IEventHandle(a, edge, nodesInfo.Release()), 0, true);
    }

    for (auto i = 1; i <= NODES; ++i) {
        THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = MakeHolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
        for (auto k = 0; k < GROUPS; ++k) {
            char delim = (k % 2 == 0) ? '/' : '|';
            auto& group1 = *response->Record.AddLabeledCountersByGroup();
            group1.SetGroup(Sprintf("group%d%c%d", i, delim, k));
            group1.SetGroupNames(Sprintf("A%cB", delim));
            if (k % 4 != 0)
                group1.SetDelimiter(TStringBuilder() << delim);
            for (auto j = 0; j < VALUES; ++j) {
                auto& counter1 = *group1.AddLabeledCounter();
                counter1.SetName(Sprintf("value%d", j));
                counter1.SetValue(13);
                counter1.SetType(TLabeledCounterOptions::CT_SIMPLE);
                counter1.SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);
            }
        }
        Cerr << "Sending message to " << cc[i % numWorkers] << " from " << aggregatorId <<  " id " << i << "\n";
        runtime.Send(new NActors::IEventHandle(cc[i % numWorkers], aggregatorId, response.Release(), 0, i), 0, true);
    }
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodesInfo, numWorkers);
        runtime.DispatchEvents(options, TDuration::Seconds(1));
    }

    THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = runtime.GrabEdgeEvent<TEvTabletCounters::TEvTabletLabeledCountersResponse>();

    UNIT_ASSERT(response != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.LabeledCountersByGroupSize(), NODES * GROUPS);

    Cerr << "TEST " << v << " " << numWorkers << " duration " << TInstant::Now() - t << "\n";
}

Y_UNIT_TEST_SUITE(TTabletCountersAggregator) {

    struct TTabletWithHist {
        TTabletWithHist(ui64 tabletId, const TTabletTypes::EType tabletType)
            : TabletId(tabletId)
            , TenantPathId(1113, 1001)
            , CounterEventsInFlight(new TEvTabletCounters::TInFlightCookie)
            , TabletType(tabletType)
            , ExecutorCounters(new TTabletCountersBase)
        {
            auto simpleCount = sizeof(SimpleCountersMetaInfo) / sizeof(SimpleCountersMetaInfo[0]);
            auto percentileCount = sizeof(PercentileCountersMetaInfo) / sizeof(PercentileCountersMetaInfo[0]);
            AppCounters.reset(new TTabletCountersBase(
                simpleCount,
                0, // cumulativeCnt
                percentileCount,
                SimpleCountersMetaInfo,
                nullptr, // cumulative meta
                PercentileCountersMetaInfo));

            for (auto i: xrange(percentileCount))
                AppCounters->Percentile()[i].Initialize(RangeDefs[i].first, RangeDefs[i].second, true);

            AppCountersBaseline.reset(new TTabletCountersBase());
            AppCounters->RememberCurrentStateAsBaseline(*AppCountersBaseline);

            ExecutorCountersBaseline.reset(new TTabletCountersBase());
            ExecutorCounters->RememberCurrentStateAsBaseline(*ExecutorCountersBaseline);
        }

        void SendUpdate(TTestBasicRuntime& runtime, const TActorId& aggregatorId, const TActorId& sender) {
            auto executorCounters = ExecutorCounters->MakeDiffForAggr(*ExecutorCountersBaseline);
            ExecutorCounters->RememberCurrentStateAsBaseline(*ExecutorCountersBaseline);

            auto appCounters = AppCounters->MakeDiffForAggr(*AppCountersBaseline);
            AppCounters->RememberCurrentStateAsBaseline(*AppCountersBaseline);

            runtime.Send(new IEventHandle(aggregatorId, sender, new TEvTabletCounters::TEvTabletAddCounters(
                CounterEventsInFlight, TabletId, TabletType, TenantPathId, executorCounters, appCounters)));

            // force recalc
            runtime.Send(new IEventHandle(aggregatorId, sender, new NActors::TEvents::TEvWakeup()));
        }

        void ForgetTablet(TTestBasicRuntime& runtime, const TActorId& aggregatorId, const TActorId& sender) {
            runtime.Send(new IEventHandle(
                aggregatorId,
                sender,
                new TEvTabletCounters::TEvTabletCountersForgetTablet(TabletId, TabletType, TenantPathId)));

            // force recalc
            runtime.Send(new IEventHandle(aggregatorId, sender, new NActors::TEvents::TEvWakeup()));
        }

        void SetSimpleCount(const char* name, ui64 count) {
            size_t index = SimpleNameToIndex(name);
            AppCounters->Simple()[index].Set(count);
        }

        void UpdatePercentile(const char* name, ui64 what) {
            size_t index = PercentileNameToIndex(name);
            AppCounters->Percentile()[index].IncrementFor(what);
        }

        void UpdatePercentile(const char* name, ui64 what, ui64 value) {
            size_t index = PercentileNameToIndex(name);
            AppCounters->Percentile()[index].AddFor(what, value);
        }

    public:
        static ::NMonitoring::TDynamicCounterPtr GetAppCounters(TTestBasicRuntime& runtime, const TTabletTypes::EType tabletType) {
            ::NMonitoring::TDynamicCounterPtr counters = runtime.GetAppData(0).Counters;
            UNIT_ASSERT(counters);

            TString tabletTypeStr = TTabletTypes::TypeToStr(tabletType);
            auto dsCounters = counters->GetSubgroup("counters", "tablets")->GetSubgroup("type", tabletTypeStr);
            return dsCounters->GetSubgroup("category", "app");
        }

        template <typename TArray>
        static size_t StringToIndex(const char* name, const TArray& array) {
            size_t i = 0;
            for (const auto& s: array) {
                if (TStringBuf(name) == TStringBuf(s))
                    return i;
                ++i;
            }
            return i;
        }

        static size_t SimpleNameToIndex(const char* name) {
            return StringToIndex(name, SimpleCountersMetaInfo);
        }

        static size_t PercentileNameToIndex(const char* name) {
            return StringToIndex(name, PercentileCountersMetaInfo);
        }

        static NMonitoring::THistogramPtr GetHistogram(TTestBasicRuntime& runtime, const char* name, const TTabletTypes::EType tabletType) {
            size_t index = PercentileNameToIndex(name);
           return GetAppCounters(runtime, tabletType)->FindHistogram(PercentileCountersMetaInfo[index]);
        }

        static std::vector<ui64> GetOldHistogram(TTestBasicRuntime& runtime, const char* name, const TTabletTypes::EType tabletType) {
            size_t index = PercentileNameToIndex(name);
            auto rangesArray = RangeDefs[index].first;
            auto rangeCount = RangeDefs[index].second;

            std::vector<TTabletPercentileCounter::TRangeDef> ranges(rangesArray, rangesArray + rangeCount);
            ranges.push_back({});
            ranges.back().RangeName = "inf";
            ranges.back().RangeVal = Max<ui64>();

            auto appCounters = GetAppCounters(runtime, tabletType);
            std::vector<ui64> buckets;
            for (auto i: xrange(ranges.size())) {
                auto subGroup = appCounters->GetSubgroup("range", ranges[i].RangeName);
                auto sensor = subGroup->FindCounter(PercentileCountersMetaInfo[index]);
                if (sensor) {
                    buckets.push_back(sensor->Val());
                }
            }

            return buckets;
        }

        static void CheckHistogram(
            TTestBasicRuntime& runtime,
            const char* name,
            const std::vector<ui64>& goldValuesNew,
            const std::vector<ui64>& goldValuesOld,
            const TTabletTypes::EType tabletType
        )
        {
            // new stype histogram
            auto histogram = TTabletWithHist::GetHistogram(runtime, name, tabletType);
            UNIT_ASSERT(histogram);
            auto snapshot = histogram->Snapshot();
            UNIT_ASSERT(snapshot);

            UNIT_ASSERT_VALUES_EQUAL(snapshot->Count(), goldValuesNew.size());
            {
                // for pretty printing the diff
                std::vector<ui64> values;
                values.reserve(goldValuesNew.size());
                for (auto i: xrange(goldValuesNew.size()))
                    values.push_back(snapshot->Value(i));
                UNIT_ASSERT_VALUES_EQUAL(values, goldValuesNew);
            }

            // old histogram
            auto values = TTabletWithHist::GetOldHistogram(runtime, name, tabletType);
            UNIT_ASSERT_VALUES_EQUAL(values.size(), goldValuesOld.size());
            UNIT_ASSERT_VALUES_EQUAL(values, goldValuesOld);
        }

    public:
        ui64 TabletId;
        TPathId TenantPathId;
        TIntrusivePtr<TEvTabletCounters::TInFlightCookie> CounterEventsInFlight;
        const TTabletTypes::EType TabletType;

        std::unique_ptr<TTabletCountersBase> ExecutorCounters;
        std::unique_ptr<TTabletCountersBase> ExecutorCountersBaseline;

        std::unique_ptr<TTabletCountersBase> AppCounters;
        std::unique_ptr<TTabletCountersBase> AppCountersBaseline;

    public:
        static constexpr TTabletPercentileCounter::TRangeDef RangeDefs1[] = {
            {0,   "0"}
        };

        static constexpr TTabletPercentileCounter::TRangeDef RangeDefs4[] = {
            {0,   "0"},
            {1,   "1"},
            {13,  "13"},
            {29,  "29"}
        };

        static constexpr std::pair<const TTabletPercentileCounter::TRangeDef*, size_t> RangeDefs[] = {
            {RangeDefs1, 1},
            {RangeDefs4, 4},
            {RangeDefs1, 1},
            {RangeDefs4, 4},
        };

        static constexpr const char* PercentileCountersMetaInfo[] = {
            "MyHistSingleBucket",
            "HIST(Count)",
            "HIST(CountSingleBucket)",
            "MyHist",
        };

        static constexpr const char* SimpleCountersMetaInfo[] = {
            "JustCount1",
            "Count",
            "CountSingleBucket",
            "JustCount2",
        };
    };

    Y_UNIT_TEST(IntegralPercentileAggregationHistNamedSingleBucket) {
        // test case when only 1 range in hist
        // histogram with name "HIST(CountSingleBucket)" and
        // associated corresponding simple counter "CountSingleBucket"
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::DataShard);

        tablet1.SetSimpleCount("CountSingleBucket", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet2(2, TTabletTypes::DataShard);
        tablet2.SetSimpleCount("CountSingleBucket", 13);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(CountSingleBucket)",
            {0, 2},
            {0, 2},
            TTabletTypes::DataShard
        );

        // sanity check we didn't mess other histograms

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 0, 0, 0, 0},
            {0, 0, 0, 0, 0},
            TTabletTypes::DataShard
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {2, 0, 0, 0, 0},
            {2, 0, 0, 0, 0},
            TTabletTypes::DataShard
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHistSingleBucket",
            {0, 0},
            {0, 0},
            TTabletTypes::DataShard
        );
    }

    Y_UNIT_TEST(IntegralPercentileAggregationHistNamed) {
        // test special histogram with name "HIST(Count)" and
        // associated corresponding simple counter "Count"
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::DataShard);

        tablet1.SetSimpleCount("Count", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 0, 0, 0},
            {0, 1, 0, 0, 0},
            TTabletTypes::DataShard
        );

        TTabletWithHist tablet2(2, TTabletTypes::DataShard);
        tablet2.SetSimpleCount("Count", 13);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 1, 0, 0},
            {0, 1, 1, 0, 0},
            TTabletTypes::DataShard
        );

        TTabletWithHist tablet3(3, TTabletTypes::DataShard);
        tablet3.SetSimpleCount("Count", 1);
        tablet3.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 2, 1, 0, 0},
            {0, 2, 1, 0, 0},
            TTabletTypes::DataShard
        );

        tablet3.SetSimpleCount("Count", 13);
        tablet3.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 2, 0, 0},
            {0, 1, 2, 0, 0},
            TTabletTypes::DataShard
        );

        tablet3.ForgetTablet(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 1, 0, 0},
            {0, 1, 1, 0, 0},
            TTabletTypes::DataShard
        );

        // sanity check we didn't mess other histograms

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 0, 0, 0, 0},
            {0, 0, 0, 0, 0},
            TTabletTypes::DataShard
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(CountSingleBucket)",
            {2, 0},
            {2, 0},
            TTabletTypes::DataShard
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHistSingleBucket",
            {0, 0},
            {0, 0},
            TTabletTypes::DataShard
        );
    }

    Y_UNIT_TEST(IntegralPercentileAggregationHistNamedNoOverflowCheck) {
        // test special histogram with name "HIST(Count)" and
        // associated corresponding simple counter "Count"
        //
        // test just for extra sanity, because for Max<ui32> in bucket we
        // will need Max<ui32> tablets. So just check simple count behaviour
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::DataShard);

        tablet1.SetSimpleCount("Count", Max<i64>() - 100UL);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 0, 0, 0, 1},
            {0, 0, 0, 0, 1},
            TTabletTypes::DataShard
        );

        TTabletWithHist tablet2(2, TTabletTypes::DataShard);
        tablet2.SetSimpleCount("Count", 100);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 0, 0, 0, 2},
            {0, 0, 0, 0, 2},
            TTabletTypes::DataShard
        );
    }

    Y_UNIT_TEST(IntegralPercentileAggregationRegularCheckSingleTablet) {
        // test regular histogram, i.e. not named "HIST"
        // check that when single tablet sends multiple count updates,
        // the aggregated value is correct
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::DataShard);
        tablet1.UpdatePercentile("MyHist", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 1, 0, 0, 0},
            {0, 1, 0, 0, 0},
            TTabletTypes::DataShard
        );

        tablet1.UpdatePercentile("MyHist", 13);
        tablet1.SendUpdate(runtime, aggregatorId, edge);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 1, 1, 0, 0},
            {0, 1, 1, 0, 0},
            TTabletTypes::DataShard
        );

        tablet1.UpdatePercentile("MyHist", 1);
        tablet1.UpdatePercentile("MyHist", 1);
        tablet1.UpdatePercentile("MyHist", 100);
        tablet1.SendUpdate(runtime, aggregatorId, edge);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 3, 1, 0, 1},
            {0, 3, 1, 0, 1},
            TTabletTypes::DataShard
        );
    }

    // Regression test for KIKIMR-13457
    Y_UNIT_TEST(IntegralPercentileAggregationRegular) {
        // test regular histogram, i.e. not named "HIST"
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::DataShard);
        tablet1.UpdatePercentile("MyHist", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet2(2, TTabletTypes::DataShard);
        tablet2.UpdatePercentile("MyHist", 1);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet3(3, TTabletTypes::DataShard);
        tablet3.UpdatePercentile("MyHist", 1);
        tablet3.UpdatePercentile("MyHist", 13);
        tablet3.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 3, 1, 0, 0},
            {0, 3, 1, 0, 0},
            TTabletTypes::DataShard
        );

        tablet3.ForgetTablet(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 2, 0, 0, 0},
            {0, 2, 0, 0, 0},
            TTabletTypes::DataShard
        );

        // sanity check we didn't mess other histograms

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {2, 0, 0, 0, 0},
            {2, 0, 0, 0, 0},
            TTabletTypes::DataShard
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHistSingleBucket",
            {0, 0},
            {0, 0},
            TTabletTypes::DataShard
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(CountSingleBucket)",
            {2, 0},
            {2, 0},
            TTabletTypes::DataShard
        );
    }

    Y_UNIT_TEST(IntegralPercentileAggregationRegularNoOverflowCheck) {
        // test regular histogram, i.e. not named "HIST"
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::DataShard);
        tablet1.UpdatePercentile("MyHist", 10, Max<i64>() - 100);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet2(2, TTabletTypes::DataShard);
        tablet2.UpdatePercentile("MyHist", 10, 25);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet3(3, TTabletTypes::DataShard);
        tablet3.UpdatePercentile("MyHist", 10, 5);
        tablet3.SendUpdate(runtime, aggregatorId, edge);

        ui64 v = Max<i64>() - 70;
        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 0, v, 0, 0},
            {0, 0, v, 0, 0},
            TTabletTypes::DataShard
        );

        tablet1.ForgetTablet(runtime, aggregatorId, edge);
        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 0, 30, 0, 0},
            {0, 0, 30, 0, 0},
            TTabletTypes::DataShard
        );
    }

    Y_UNIT_TEST(ColumnShardCounters) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist tablet1(1, TTabletTypes::ColumnShard);

        tablet1.SetSimpleCount("Count", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 0, 0, 0},
            {0, 1, 0, 0, 0},
            tablet1.TabletType
        );
    }
}

Y_UNIT_TEST_SUITE(TTabletLabeledCountersAggregator) {
    Y_UNIT_TEST(SimpleAggregation) {
        TVector<TActorId> cc;
        TActorId aggregatorId;

        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        IActor* aggregator = CreateClusterLabeledCountersAggregatorActor(edge, TTabletTypes::PersQueue, 2, TString(), 3);
        aggregatorId = runtime.Register(aggregator);

        runtime.SetRegistrationObserverFunc([&cc, &aggregatorId](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                TTestActorRuntime::DefaultRegistrationObserver(runtime, parentId, actorId);
                    if (parentId == aggregatorId) {
                        cc.push_back(actorId);
                    }
                });

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);
        for (const auto& a : cc) {
            auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
            nodes->emplace_back(TEvInterconnect::TNodeInfo(1, "::", "localhost", "localhost", 1234, TNodeLocation()));
            nodes->emplace_back(TEvInterconnect::TNodeInfo(2, "::", "localhost", "localhost", 1234, TNodeLocation()));
            nodes->emplace_back(TEvInterconnect::TNodeInfo(3, "::", "localhost", "localhost", 1234, TNodeLocation()));
            THolder<TEvInterconnect::TEvNodesInfo> nodesInfo = MakeHolder<TEvInterconnect::TEvNodesInfo>(nodes);
            runtime.Send(new NActors::IEventHandle(a, edge, nodesInfo.Release()), 0, true);
        }

        {
            THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = MakeHolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
            auto& group1 = *response->Record.AddLabeledCountersByGroup();
            group1.SetGroup("group1|group2");
            group1.SetGroupNames("AAA|BBB");
            group1.SetDelimiter("|");
            auto& counter1 = *group1.AddLabeledCounter();
            counter1.SetName("value1");
            counter1.SetValue(13);
            counter1.SetType(TLabeledCounterOptions::CT_SIMPLE);
            counter1.SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);
            runtime.Send(new NActors::IEventHandle(cc[0], edge, response.Release(), 0, 1), 0, true);
        }

        {
            THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = MakeHolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
            response->Record.AddCounterNames("value1");
            auto& group1 = *response->Record.AddLabeledCountersByGroup();
            group1.SetGroup("group1|group2");
            group1.SetGroupNames("AAA|BBB");
            group1.SetDelimiter("|");
            auto& counter1 = *group1.AddLabeledCounter();
            counter1.SetNameId(0);
            counter1.SetValue(13);
            counter1.SetType(TLabeledCounterOptions::CT_SIMPLE);
            counter1.SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);
            runtime.Send(new NActors::IEventHandle(cc[1], edge, response.Release(), 0, 2), 0, true);
        }

        {
            THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = MakeHolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
            response->Record.AddCounterNames("value1");
            auto& group1 = *response->Record.AddLabeledCountersByGroup();
            group1.SetGroup("group1|group2");
            group1.SetGroupNames("AAA|BBB");
            group1.SetDelimiter("|");
            auto& counter1 = *group1.AddLabeledCounter();
            counter1.SetNameId(0);
            counter1.SetValue(13);
            counter1.SetType(TLabeledCounterOptions::CT_SIMPLE);
            counter1.SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);
            runtime.Send(new NActors::IEventHandle(cc[2], edge, response.Release(), 0, 3), 0, true);
        }

        runtime.DispatchEvents();
        THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = runtime.GrabEdgeEvent<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
#ifndef NDEBUG
        Cerr << response->Record.DebugString() << Endl;
#endif
        UNIT_ASSERT(response != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.LabeledCountersByGroupSize(), 1);
        const auto& group1 = response->Record.GetLabeledCountersByGroup(0);
        UNIT_ASSERT_VALUES_EQUAL(group1.GetGroup(), "group1/group2");
        UNIT_ASSERT_VALUES_EQUAL(group1.LabeledCounterSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(group1.LabeledCounterSize(), 1);
        const auto& counter1 = group1.GetLabeledCounter(0);
        UNIT_ASSERT_VALUES_EQUAL(counter1.GetNameId(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counter1.GetValue(), 39);
    }

    Y_UNIT_TEST(HeavyAggregation) {
        TestHeavy(2, 10);
        TestHeavy(2, 20);
        TestHeavy(2, 1);
        TestHeavy(2, 0);
    }

    Y_UNIT_TEST(Version3Aggregation) {
        TVector<TActorId> cc;
        TActorId aggregatorId;

        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        // NOTE(shmel1k@): KIKIMR-14221
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);

        TActorId edge = runtime.AllocateEdgeActor();

        IActor* aggregator = CreateClusterLabeledCountersAggregatorActor(edge, TTabletTypes::PersQueue, 3, "rt3.*--*,cons*/*/rt.*--*", 3);
        aggregatorId = runtime.Register(aggregator);

        runtime.SetRegistrationObserverFunc([&cc, &aggregatorId](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                TTestActorRuntime::DefaultRegistrationObserver(runtime, parentId, actorId);
                    if (parentId == aggregatorId) {
                        cc.push_back(actorId);
                    }
                });

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);
        for (const auto& a : cc) {
            auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
            nodes->emplace_back(TEvInterconnect::TNodeInfo(1, "::", "localhost", "localhost", 1234, TNodeLocation()));
            nodes->emplace_back(TEvInterconnect::TNodeInfo(2, "::", "localhost", "localhost", 1234, TNodeLocation()));
            nodes->emplace_back(TEvInterconnect::TNodeInfo(3, "::", "localhost", "localhost", 1234, TNodeLocation()));
            THolder<TEvInterconnect::TEvNodesInfo> nodesInfo = MakeHolder<TEvInterconnect::TEvNodesInfo>(nodes);
            runtime.Send(new NActors::IEventHandle(a, edge, nodesInfo.Release()), 0, true);
        }

        {
            THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = MakeHolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
            auto& group1 = *response->Record.AddLabeledCountersByGroup();
            group1.SetGroup("rt3.man--aba@caba--daba");
            group1.SetGroupNames("topic");
            group1.SetDelimiter("/");
            auto& counter1 = *group1.AddLabeledCounter();
            counter1.SetName("value1");
            counter1.SetValue(13);
            counter1.SetType(TLabeledCounterOptions::CT_SIMPLE);
            counter1.SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);
            runtime.Send(new NActors::IEventHandle(cc[0], edge, response.Release(), 0, 1), 0, true);
        }

        {
            THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = MakeHolder<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
            response->Record.AddCounterNames("value1");
            auto& group1 = *response->Record.AddLabeledCountersByGroup();
            group1.SetGroup("cons@aaa/1/rt3.man--aba@caba--daba");
            group1.SetGroupNames("consumer/important/topic");
            group1.SetDelimiter("/");
            auto& counter1 = *group1.AddLabeledCounter();
            counter1.SetNameId(0);
            counter1.SetValue(13);
            counter1.SetType(TLabeledCounterOptions::CT_SIMPLE);
            counter1.SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);
            runtime.Send(new NActors::IEventHandle(cc[1], edge, response.Release(), 0, 2), 0, true);
        }

        runtime.DispatchEvents();
        THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response = runtime.GrabEdgeEvent<TEvTabletCounters::TEvTabletLabeledCountersResponse>();
#ifndef NDEBUG
        Cerr << response->Record.DebugString() << Endl;
#endif
        UNIT_ASSERT(response != nullptr);
        Cerr << response->Record;
        UNIT_ASSERT_VALUES_EQUAL(response->Record.LabeledCountersByGroupSize(), 2);
        const auto& group1 = response->Record.GetLabeledCountersByGroup(1);
        const auto& group2 = response->Record.GetLabeledCountersByGroup(0);
        TVector<TString> res = {group1.GetGroup(), group2.GetGroup()};
        std::sort(res.begin(), res.end());

        UNIT_ASSERT_VALUES_EQUAL(res[0], "aba/caba/daba|man");
        UNIT_ASSERT_VALUES_EQUAL(res[1], "cons/aaa|1|aba/caba/daba|man");
    }

    Y_UNIT_TEST(DbAggregation) {
        TVector<TActorId> cc;
        TActorId aggregatorId;

        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

        TActorId edge = runtime.AllocateEdgeActor();

        runtime.SetRegistrationObserverFunc([&cc, &aggregatorId]
            (TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                TTestActorRuntime::DefaultRegistrationObserver(runtime, parentId, actorId);
                    if (parentId == aggregatorId) {
                        cc.push_back(actorId);
                    }
                });

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);
        for (const auto& a : cc) {
            auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
            nodes->emplace_back(TEvInterconnect::TNodeInfo(1, "::", "localhost", "localhost", 1234, TNodeLocation()));
            nodes->emplace_back(TEvInterconnect::TNodeInfo(2, "::", "localhost", "localhost", 1234, TNodeLocation()));
            nodes->emplace_back(TEvInterconnect::TNodeInfo(3, "::", "localhost", "localhost", 1234, TNodeLocation()));
            THolder<TEvInterconnect::TEvNodesInfo> nodesInfo = MakeHolder<TEvInterconnect::TEvNodesInfo>(nodes);
            runtime.Send(new NActors::IEventHandle(a, edge, nodesInfo.Release()), 0, true);
        }

        NPrivate::TDbLabeledCounters PQCounters;

        const size_t namesN{5};
        std::array<const char *, namesN> names;
        names.fill("");
        names[0] = "whatever";
        names[1] = "whenever";
        std::array<const char *, namesN> groupNames;
        groupNames.fill("topic");
        groupNames[1] = "user||topic";
        std::array<ui8, namesN> types;
        types.fill(static_cast<ui8>(TLabeledCounterOptions::CT_SIMPLE));

        std::array<ui8, namesN> functions;
        functions.fill(static_cast<ui8>(TLabeledCounterOptions::EAF_SUM));
        functions[1] = static_cast<ui8>(TLabeledCounterOptions::EAF_MAX);

        {
            NKikimr::TTabletLabeledCountersBase labeledCounters(namesN, &names[0], &types[0], &functions[0],
                                                                "some_stream", &groupNames[0], 1, "/Root/PQ1");
            labeledCounters.GetCounters()[0].Set(10);
            labeledCounters.GetCounters()[1].Set(10);
            PQCounters.Apply(0, &labeledCounters);
            labeledCounters.GetCounters()[0].Set(11);
            labeledCounters.GetCounters()[1].Set(100);
            PQCounters.Apply(1, &labeledCounters);
            labeledCounters.GetCounters()[0].Set(12);
            labeledCounters.GetCounters()[1].Set(10);
            PQCounters.Apply(2, &labeledCounters);
            // SUM 33
            // MAX 100
        }

        {
            NKikimr::TTabletLabeledCountersBase labeledCounters(namesN, &names[0], &types[0], &functions[0],
                                                                "some_stream", &groupNames[0], 1, "/Root/PQ2");
            labeledCounters.GetCounters()[0].Set(20);
            labeledCounters.GetCounters()[1].Set(1);
            PQCounters.Apply(0, &labeledCounters);
            labeledCounters.GetCounters()[0].Set(21);
            labeledCounters.GetCounters()[1].Set(11);
            PQCounters.Apply(1, &labeledCounters);
            labeledCounters.GetCounters()[0].Set(22);
            labeledCounters.GetCounters()[1].Set(10);
            PQCounters.Apply(2, &labeledCounters);
            // SUM 63
            // MAX 11
        }

        NKikimr::NSysView::TDbServiceCounters counters;

        // Here we check that consequent calls do not interfere
        for (int i = 10; i >= 0; --i) {
            PQCounters.ToProto(counters);

            auto pqCounters = counters.FindOrAddLabeledCounters("some_stream");
            UNIT_ASSERT_VALUES_EQUAL(pqCounters->GetAggregatedPerTablets().group(), "some_stream");
            UNIT_ASSERT_VALUES_EQUAL(pqCounters->GetAggregatedPerTablets().delimiter(), "|");
            UNIT_ASSERT_VALUES_EQUAL(pqCounters->GetAggregatedPerTablets().GetLabeledCounter().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(pqCounters->GetAggregatedPerTablets().GetLabeledCounter(0).value(), 63);
            UNIT_ASSERT_VALUES_EQUAL(pqCounters->GetAggregatedPerTablets().GetLabeledCounter(1).value(), 11);

            auto additional = pqCounters->MutableAggregatedPerTablets()->AddLabeledCounter();
            additional->SetNameId(1000);
            additional->SetValue(13);
            additional->SetType(TLabeledCounterOptions::CT_SIMPLE);
            additional->SetAggregateFunc(TLabeledCounterOptions::EAF_SUM);

            PQCounters.FromProto(counters);
        }
    }
}

}
