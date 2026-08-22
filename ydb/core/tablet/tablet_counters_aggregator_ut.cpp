#include "tablet_counters_aggregator.h"
#include "private/labeled_db_counters.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/table_metrics_settings.pb.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>

#include <util/generic/array_size.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>

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

    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev){
        if (ev->GetTypeRewrite() == TEvInterconnect::EvNodesInfo && ev->Sender != edge) {
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });

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

        static void CheckHistogram(
            TTestBasicRuntime& runtime,
            const char* name,
            const std::vector<ui64>& goldValues,
            const TTabletTypes::EType tabletType
        )
        {
            auto histogram = TTabletWithHist::GetHistogram(runtime, name, tabletType);
            UNIT_ASSERT(histogram);
            auto snapshot = histogram->Snapshot();
            UNIT_ASSERT(snapshot);

            UNIT_ASSERT_VALUES_EQUAL(snapshot->Count(), goldValues.size());
            {
                // for pretty printing the diff
                std::vector<ui64> values;
                values.reserve(goldValues.size());
                for (auto i: xrange(goldValues.size()))
                    values.push_back(snapshot->Value(i));
                UNIT_ASSERT_VALUES_EQUAL(values, goldValues);
            }
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

        TTabletWithHist tablet1(1, TTabletTypes::Dummy);

        tablet1.SetSimpleCount("CountSingleBucket", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet2(2, TTabletTypes::Dummy);
        tablet2.SetSimpleCount("CountSingleBucket", 13);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(CountSingleBucket)",
            {0, 2},
            TTabletTypes::Dummy
        );

        // sanity check we didn't mess other histograms

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 0, 0, 0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {2, 0, 0, 0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHistSingleBucket",
            {0, 0},
            TTabletTypes::Dummy
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

        TTabletWithHist tablet1(1, TTabletTypes::Dummy);

        tablet1.SetSimpleCount("Count", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 0, 0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist tablet2(2, TTabletTypes::Dummy);
        tablet2.SetSimpleCount("Count", 13);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 1, 0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist tablet3(3, TTabletTypes::Dummy);
        tablet3.SetSimpleCount("Count", 1);
        tablet3.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 2, 1, 0, 0},
            TTabletTypes::Dummy
        );

        tablet3.SetSimpleCount("Count", 13);
        tablet3.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 2, 0, 0},
            TTabletTypes::Dummy
        );

        tablet3.ForgetTablet(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 1, 1, 0, 0},
            TTabletTypes::Dummy
        );

        // sanity check we didn't mess other histograms

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 0, 0, 0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(CountSingleBucket)",
            {2, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHistSingleBucket",
            {0, 0},
            TTabletTypes::Dummy
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

        TTabletWithHist tablet1(1, TTabletTypes::Dummy);

        tablet1.SetSimpleCount("Count", Max<i64>() - 100UL);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 0, 0, 0, 1},
            TTabletTypes::Dummy
        );

        TTabletWithHist tablet2(2, TTabletTypes::Dummy);
        tablet2.SetSimpleCount("Count", 100);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {0, 0, 0, 0, 2},
            TTabletTypes::Dummy
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

        TTabletWithHist tablet1(1, TTabletTypes::Dummy);
        tablet1.UpdatePercentile("MyHist", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 1, 0, 0, 0},
            TTabletTypes::Dummy
        );

        tablet1.UpdatePercentile("MyHist", 13);
        tablet1.SendUpdate(runtime, aggregatorId, edge);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 1, 1, 0, 0},
            TTabletTypes::Dummy
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
            TTabletTypes::Dummy
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

        TTabletWithHist tablet1(1, TTabletTypes::Dummy);
        tablet1.UpdatePercentile("MyHist", 1);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet2(2, TTabletTypes::Dummy);
        tablet2.UpdatePercentile("MyHist", 1);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet3(3, TTabletTypes::Dummy);
        tablet3.UpdatePercentile("MyHist", 1);
        tablet3.UpdatePercentile("MyHist", 13);
        tablet3.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 3, 1, 0, 0},
            TTabletTypes::Dummy
        );

        tablet3.ForgetTablet(runtime, aggregatorId, edge);

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 2, 0, 0, 0},
            TTabletTypes::Dummy
        );

        // sanity check we didn't mess other histograms

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(Count)",
            {2, 0, 0, 0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHistSingleBucket",
            {0, 0},
            TTabletTypes::Dummy
        );

        TTabletWithHist::CheckHistogram(
            runtime,
            "HIST(CountSingleBucket)",
            {2, 0},
            TTabletTypes::Dummy
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

        TTabletWithHist tablet1(1, TTabletTypes::Dummy);
        tablet1.UpdatePercentile("MyHist", 10, Max<i64>() - 100);
        tablet1.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet2(2, TTabletTypes::Dummy);
        tablet2.UpdatePercentile("MyHist", 10, 25);
        tablet2.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist tablet3(3, TTabletTypes::Dummy);
        tablet3.UpdatePercentile("MyHist", 10, 5);
        tablet3.SendUpdate(runtime, aggregatorId, edge);

        ui64 v = Max<i64>() - 70;
        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 0, v, 0, 0},
            TTabletTypes::Dummy
        );

        tablet1.ForgetTablet(runtime, aggregatorId, edge);
        TTabletWithHist::CheckHistogram(
            runtime,
            "MyHist",
            {0, 0, 30, 0, 0},
            TTabletTypes::Dummy
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
            tablet1.TabletType
        );
    }

    Y_UNIT_TEST(SearchCounters) {
        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        auto aggregator = CreateTabletCountersAggregator(false);
        auto aggregatorId = runtime.Register(aggregator);
        runtime.EnableScheduleForActor(aggregatorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        TTabletWithHist dummyTablet(1, TTabletTypes::Dummy);
        dummyTablet.SetSimpleCount("JustCount1", 11);
        dummyTablet.SendUpdate(runtime, aggregatorId, edge);

        TTabletWithHist columnShardTablet(2, TTabletTypes::ColumnShard);
        columnShardTablet.SetSimpleCount("JustCount1", 22);
        columnShardTablet.SendUpdate(runtime, aggregatorId, edge);

        struct TTestHttpRequest : NMonitoring::IHttpRequest {
            HTTP_METHOD Method;
            TCgiParameters CgiParameters;
            THttpHeaders HttpHeaders;
            TString Path;

            TTestHttpRequest(HTTP_METHOD method, TString path)
                : Method(method)
                , Path(std::move(path))
            {
            }

            const char* GetURI() const override {
                return "";
            }

            const char* GetPath() const override {
                return Path.c_str();
            }

            const TCgiParameters& GetParams() const override {
                return CgiParameters;
            }

            const TCgiParameters& GetPostParams() const override {
                return CgiParameters;
            }

            TStringBuf GetPostContent() const override {
                return {};
            }

            HTTP_METHOD GetMethod() const override {
                return Method;
            }

            const THttpHeaders& GetHeaders() const override {
                return HttpHeaders;
            }

            TString GetRemoteAddr() const override {
                return {};
            }
        };

        TTestHttpRequest httpReq(HTTP_METHOD_GET, "/actors/tablet_counters_aggregator/search");
        httpReq.CgiParameters.emplace("name", "JustCount1");
        NMonitoring::TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, nullptr, "/search", nullptr);
        runtime.Send(new IEventHandle(aggregatorId, edge, new NMon::TEvHttpInfo(monReq)));

        TAutoPtr<IEventHandle> handle;
        auto* resp = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);
        UNIT_ASSERT(resp);

        const TString& answer = resp->Answer;
        UNIT_ASSERT_STRING_CONTAINS(answer, "JustCount1");
        UNIT_ASSERT_STRING_CONTAINS(answer, "TabletID=1");
        UNIT_ASSERT_STRING_CONTAINS(answer, "TabletID=2");
        UNIT_ASSERT_STRING_CONTAINS(answer, "<td>11</td>");
        UNIT_ASSERT_STRING_CONTAINS(answer, "<td>22</td>");
    }
}

Y_UNIT_TEST_SUITE(TTabletLabeledCountersAggregator) {
    Y_UNIT_TEST(SimpleAggregation) {
        TVector<TActorId> cc;
        TActorId aggregatorId;

        TTestBasicRuntime runtime(1);

        runtime.Initialize(TAppPrepare().Unwrap());
        TActorId edge = runtime.AllocateEdgeActor();

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev){
            if (ev->GetTypeRewrite() == TEvInterconnect::EvNodesInfo && ev->Sender != edge) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

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

        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev){
            if (ev->GetTypeRewrite() == TEvInterconnect::EvNodesInfo && ev->Sender != edge) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

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

Y_UNIT_TEST_SUITE(TEvTabletAddCountersDetailedMetricsFields) {
    Y_UNIT_TEST(DefaultsToLeader) {
        TEvTabletCounters::TEvTabletAddCounters ev(
            new TEvTabletCounters::TInFlightCookie, 1, TTabletTypes::DataShard, TPathId(1113, 1001),
            new TTabletCountersBase, new TTabletCountersBase);

        UNIT_ASSERT_VALUES_EQUAL(ev.FollowerId, 0u);
    }

    Y_UNIT_TEST(StampsFollowerIdWhenProvided) {
        TEvTabletCounters::TEvTabletAddCounters ev(
            new TEvTabletCounters::TInFlightCookie, 1, TTabletTypes::DataShard, TPathId(1113, 1001),
            new TTabletCountersBase, new TTabletCountersBase,
            7);

        UNIT_ASSERT_VALUES_EQUAL(ev.FollowerId, 7u);
    }
}

/**
 * Tests for the detailed metrics, which the two aggregator actors of a node build
 * within the private "ydb_detailed_raw" counter group.
 */
Y_UNIT_TEST_SUITE(TTabletCountersAggregatorDetailedMetrics) {

    const TString DETAILED_RAW_GROUP = "ydb_detailed_raw";

    const TString DATABASE_PATH = "/Root/db";

    const TString TABLE_PATH = "/Root/db/dir/table";
    const TString RELATIVE_TABLE_PATH = "dir/table";

    const TPathId TENANT_PATH_ID(1113, 1001);
    const TPathId TABLE_ID(1113, 42);

    constexpr TTabletTypes::EType TABLET_TYPE = TTabletTypes::DataShard;

    constexpr ui32 LEVEL_TABLE = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable;
    constexpr ui32 LEVEL_PARTITION = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition;
    constexpr ui32 LEVEL_DISABLED = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelDisabled;

    // The only tablet type with a detailed metrics counter set is DataShard, and
    // GetDetailedMetricsCounterNames() allow-lists this Executor counter name (it is
    // the source of the public table.datashard.row_count metric, see
    // counters_detailed_datashard.proto)
    const TString ALLOWED_EXECUTOR_COUNTER = "DbUniqueRowsTotal";

    // An Executor simple counter, which is NOT in that allow-list (see
    // flat_executor_counters.h / counters_detailed_datashard.proto)
    const TString UNLISTED_EXECUTOR_COUNTER = "LogRedoItems";

    constexpr const char* EXECUTOR_SIMPLE_COUNTER_NAMES[] = {
        "DbUniqueRowsTotal",
        "LogRedoItems",
    };

    enum EExecutorSimpleCounter : ui32 {
        DB_UNIQUE_ROWS_TOTAL = 0,
        LOG_REDO_ITEMS = 1,
    };

    ////////////////////////////////////////////

    /**
     * A stand-in for the scheme cache: it resolves the path id of the database to
     * its path and nothing else.
     */
    class TFakeSchemeCache : public TActor<TFakeSchemeCache> {
    public:
        TFakeSchemeCache(ui32* requestCounter, THashSet<TPathId>* watchedPathIds)
            : TActor(&TThis::StateWork)
            , RequestCounter(requestCounter)
            , WatchedPathIds(watchedPathIds)
        {}

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
                hFunc(TEvTxProxySchemeCache::TEvWatchPathId, Handle);
                default:
                    break;
            }
        }

    private:
        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
            ++*RequestCounter;

            TAutoPtr<NSchemeCache::TSchemeCacheNavigate> navigate = ev->Get()->Request.Release();

            for (auto& entry : navigate->ResultSet) {
                entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                entry.Path = SplitPath(DATABASE_PATH);
            }

            Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(navigate));
        }

        void Handle(TEvTxProxySchemeCache::TEvWatchPathId::TPtr& ev) {
            WatchedPathIds->insert(ev->Get()->PathId);
        }

    private:
        ui32* const RequestCounter;
        THashSet<TPathId>* const WatchedPathIds;
    };

    ////////////////////////////////////////////

    /**
     * The two aggregator actors of a single node and a scheme cache, which resolves
     * the path of the database.
     */
    struct TEnv {
        explicit TEnv(bool detailedMetricsEnabled)
            : Runtime(1)
        {
            TAppPrepare app;
            app.SetEnableDataShardDetailedMetrics(detailedMetricsEnabled);
            Runtime.Initialize(app.Unwrap());

            Edge = Runtime.AllocateEdgeActor();

            Runtime.RegisterService(
                MakeSchemeCacheID(),
                Runtime.Register(new TFakeSchemeCache(&NavigateRequests, &WatchedPathIds))
            );

            LeaderAggregatorId = Runtime.Register(CreateTabletCountersAggregator(false));
            FollowerAggregatorId = Runtime.Register(CreateTabletCountersAggregator(true));

            Runtime.EnableScheduleForActor(LeaderAggregatorId);
            Runtime.EnableScheduleForActor(FollowerAggregatorId);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 2);
            Runtime.DispatchEvents(options);
        }

        TActorId GetAggregatorId(ui32 followerId) const {
            return followerId == 0 ? LeaderAggregatorId : FollowerAggregatorId;
        }

        ::NMonitoring::TDynamicCounterPtr GetCountersRoot() {
            ::NMonitoring::TDynamicCounterPtr counters = Runtime.GetAppData(0).Counters;
            UNIT_ASSERT(counters);
            return counters;
        }

        TTestBasicRuntime Runtime;
        TActorId Edge;
        TActorId LeaderAggregatorId;
        TActorId FollowerAggregatorId;

        ui32 NavigateRequests = 0;
        THashSet<TPathId> WatchedPathIds;
    };

    ////////////////////////////////////////////

    /**
     * A single tablet of the table, which reports its low level counters to
     * the aggregator actor of its role. The table identity (TEvTabletSetTableInfo)
     * and the counters (TEvTabletAddCounters) are two separate events.
     */
    struct TFakeTablet {
        TFakeTablet(ui64 tabletId, ui32 followerId, ui32 metricsLevel)
            : TabletId(tabletId)
            , FollowerId(followerId)
            , MetricsLevel(metricsLevel)
            , CounterEventsInFlight(new TEvTabletCounters::TInFlightCookie)
            , ExecutorCounters(new TTabletCountersBase(
                Y_ARRAY_SIZE(EXECUTOR_SIMPLE_COUNTER_NAMES),
                0, // cumulativeCnt
                0, // percentileCnt
                EXECUTOR_SIMPLE_COUNTER_NAMES,
                nullptr,
                nullptr))
            , ExecutorCountersBaseline(new TTabletCountersBase)
            , AppCounters(new TTabletCountersBase)
            , AppCountersBaseline(new TTabletCountersBase)
        {
            ExecutorCounters->RememberCurrentStateAsBaseline(*ExecutorCountersBaseline);
            AppCounters->RememberCurrentStateAsBaseline(*AppCountersBaseline);
        }

        TFakeTablet& SetSimple(EExecutorSimpleCounter counter, ui64 value) {
            ExecutorCounters->Simple()[counter].Set(value);
            return *this;
        }

        void SetMetricsLevel(ui32 metricsLevel) {
            MetricsLevel = metricsLevel;
        }

        void SendTableInfo(TEnv& env) {
            const TActorId aggregatorId = env.GetAggregatorId(FollowerId);

            env.Runtime.Send(new IEventHandle(aggregatorId, env.Edge,
                new TEvTabletCounters::TEvTabletSetTableInfo(
                    TabletId, TENANT_PATH_ID, FollowerId, TABLE_ID, TABLE_PATH,
                    1 /* schemaVersion */, MetricsLevel)));
        }

        void SendCounters(TEnv& env) {
            auto executorCounters = ExecutorCounters->MakeDiffForAggr(*ExecutorCountersBaseline);
            ExecutorCounters->RememberCurrentStateAsBaseline(*ExecutorCountersBaseline);

            auto appCounters = AppCounters->MakeDiffForAggr(*AppCountersBaseline);
            AppCounters->RememberCurrentStateAsBaseline(*AppCountersBaseline);

            const TActorId aggregatorId = env.GetAggregatorId(FollowerId);

            env.Runtime.Send(new IEventHandle(aggregatorId, env.Edge,
                new TEvTabletCounters::TEvTabletAddCounters(
                    CounterEventsInFlight, TabletId, TABLET_TYPE, TENANT_PATH_ID,
                    executorCounters, appCounters, FollowerId)));

            // force recalc
            env.Runtime.Send(new IEventHandle(aggregatorId, env.Edge, new TEvents::TEvWakeup()));
        }

        /**
         * Send the table identity and one round of the counters, the way a real
         * tablet does it after boot.
         */
        void SendUpdate(TEnv& env) {
            SendTableInfo(env);
            SendCounters(env);
        }

        void SendForget(TEnv& env) {
            const TActorId aggregatorId = env.GetAggregatorId(FollowerId);

            env.Runtime.Send(new IEventHandle(aggregatorId, env.Edge,
                new TEvTabletCounters::TEvTabletCountersForgetTablet(
                    TabletId, TABLET_TYPE, TENANT_PATH_ID, FollowerId)));

            // force recalc
            env.Runtime.Send(new IEventHandle(aggregatorId, env.Edge, new TEvents::TEvWakeup()));
        }

        const ui64 TabletId;
        const ui32 FollowerId;
        ui32 MetricsLevel;

        TIntrusivePtr<TEvTabletCounters::TInFlightCookie> CounterEventsInFlight;

        std::unique_ptr<TTabletCountersBase> ExecutorCounters;
        std::unique_ptr<TTabletCountersBase> ExecutorCountersBaseline;

        std::unique_ptr<TTabletCountersBase> AppCounters;
        std::unique_ptr<TTabletCountersBase> AppCountersBaseline;
    };

    /**
     * Report the counters of the tablets, giving the aggregator actors the round, which
     * they spend on resolving the path of the database.
     */
    void ReportCounters(TEnv& env, const TVector<TFakeTablet*>& tablets) {
        for (ui32 round = 0; round < 2; ++round) {
            for (auto* tablet : tablets) {
                tablet->SendCounters(env);
            }
            env.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        }
    }

    ////////////////////////////////////////////

    ::NMonitoring::TDynamicCounterPtr FindRawGroup(TEnv& env) {
        return env.GetCountersRoot()->FindSubgroup("counters", DETAILED_RAW_GROUP);
    }

    /**
     * @note There is no role= node: both actors of the node build ONE shared tree,
     *       exactly the shape the specification defines. At the partition level the
     *       role is carried by follower_id (follower_id=0 IS the leader) and at the
     *       table level the bucket belongs to the actor of the leaders alone.
     */
    ::NMonitoring::TDynamicCounterPtr FindTableGroup(TEnv& env) {
        auto rawGroup = FindRawGroup(env);
        if (!rawGroup) {
            return nullptr;
        }

        auto databaseGroup = rawGroup->FindSubgroup("database", DATABASE_PATH);
        if (!databaseGroup) {
            return nullptr;
        }

        return databaseGroup->FindSubgroup("table", RELATIVE_TABLE_PATH);
    }

    ::NMonitoring::TDynamicCounterPtr FindExecutorCounters(::NMonitoring::TDynamicCounterPtr bucketGroup) {
        if (!bucketGroup) {
            return nullptr;
        }

        auto typeGroup = bucketGroup->FindSubgroup("type", TString(TTabletTypes::TypeToStr(TABLET_TYPE)));
        if (!typeGroup) {
            return nullptr;
        }

        return typeGroup->FindSubgroup("category", "executor");
    }

    /**
     * @return The bucket, which collapses ALL the partitions of a TABLE level table
     *         (this bucket lives directly on the table= node)
     */
    ::NMonitoring::TDynamicCounterPtr FindTableBucketCounters(TEnv& env) {
        return FindExecutorCounters(FindTableGroup(env));
    }

    /**
     * @return The leaf of a single tablet of a PARTITION level table
     */
    ::NMonitoring::TDynamicCounterPtr FindLeafCounters(TEnv& env, ui64 tabletId, ui32 followerId) {
        auto tableGroup = FindTableGroup(env);
        if (!tableGroup) {
            return nullptr;
        }

        auto perPartitionGroup = tableGroup->FindSubgroup("detailed_metrics", "per_partition");
        if (!perPartitionGroup) {
            return nullptr;
        }

        auto tabletGroup = perPartitionGroup->FindSubgroup("tablet_id", ToString(tabletId));
        if (!tabletGroup) {
            return nullptr;
        }

        return FindExecutorCounters(tabletGroup->FindSubgroup("follower_id", ToString(followerId)));
    }

    ui64 GetCounterValue(::NMonitoring::TDynamicCounterPtr countersGroup, const TString& aggregate, const TString& name) {
        UNIT_ASSERT_C(countersGroup, "no counter group for " << aggregate << "(" << name << ")");

        auto counter = countersGroup->FindNamedCounter("sensor", aggregate + "(" + name + ")");
        UNIT_ASSERT_C(counter, "no counter " << aggregate << "(" << name << ")");

        return counter->Val();
    }

    ////////////////////////////////////////////

    /**
     * Verify that nothing at all is created while the feature flag is off.
     */
    Y_UNIT_TEST(NoCountersWhenDisabled) {
        TEnv env(false /* detailedMetricsEnabled */);

        TFakeTablet leader(1000, 0, LEVEL_PARTITION);
        TFakeTablet follower(1000, 1, LEVEL_PARTITION);

        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);

        leader.SendUpdate(env);
        follower.SendUpdate(env);
        ReportCounters(env, {&leader, &follower});

        UNIT_ASSERT(!FindRawGroup(env));
    }

    /**
     * Verify that at the partition level both aggregator actors of the node fill their
     * own leaves of one and the same private counter tree, told apart by follower_id
     * alone and sharing the tablet_id= node above them.
     */
    Y_UNIT_TEST(PartitionLevelLeavesOfBothRoles) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet leader(1000, 0, LEVEL_PARTITION);
        TFakeTablet follower(1000, 1, LEVEL_PARTITION);

        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);

        leader.SendUpdate(env);
        follower.SendUpdate(env);
        ReportCounters(env, {&leader, &follower});

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindLeafCounters(env, 1000, 0), "SUM", ALLOWED_EXECUTOR_COUNTER), 1u);
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindLeafCounters(env, 1000, 1), "SUM", ALLOWED_EXECUTOR_COUNTER), 2u);

        // The two leaves hang off ONE shared tablet_id= node, written by the two
        // different actors, and no invented label appears anywhere
        auto tabletGroup = FindTableGroup(env)
            ->FindSubgroup("detailed_metrics", "per_partition")
            ->FindSubgroup("tablet_id", "1000");
        UNIT_ASSERT(tabletGroup);
        UNIT_ASSERT(tabletGroup->FindSubgroup("follower_id", "0"));
        UNIT_ASSERT(tabletGroup->FindSubgroup("follower_id", "1"));

        UNIT_ASSERT(!FindRawGroup(env)->FindSubgroup("role", "leader"));
        UNIT_ASSERT(!FindRawGroup(env)->FindSubgroup("role", "follower"));
    }

    /**
     * Verify that at the table level the leader partitions of the table are collapsed
     * into a single bucket and no per-partition group is created.
     */
    Y_UNIT_TEST(TableLevelCollapsesPartitions) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet leader1(1000, 0, LEVEL_TABLE);
        TFakeTablet leader2(2000, 0, LEVEL_TABLE);

        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);

        leader1.SendUpdate(env);
        leader2.SendUpdate(env);
        ReportCounters(env, {&leader1, &leader2});

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(env), "SUM", ALLOWED_EXECUTOR_COUNTER), 1u + 2u);
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindTableBucketCounters(env), "MAX", ALLOWED_EXECUTOR_COUNTER), 2u);

        auto tableGroup = FindTableGroup(env);
        UNIT_ASSERT(tableGroup);
        UNIT_ASSERT(!tableGroup->FindSubgroup("detailed_metrics", "per_partition"));
    }

    /**
     * Verify the ordering guarantee of the identity join: counters, which arrive
     * before the identity of their table is known, are skipped, not queued.
     */
    Y_UNIT_TEST(CountersBeforeTableInfoAreSkipped) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet leader(1000, 0, LEVEL_PARTITION);
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);

        // Counters arrive first: nothing is published yet
        ReportCounters(env, {&leader});
        UNIT_ASSERT(!FindTableGroup(env));

        // The identity arrives, followed by another round of counters
        leader.SendTableInfo(env);
        ReportCounters(env, {&leader});

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindLeafCounters(env, 1000, 0), "SUM", ALLOWED_EXECUTOR_COUNTER), 1u);
    }

    /**
     * Verify that forgetting a tablet drops its own leaf and ONLY its own leaf while the
     * other role of the very same tablet is still on the node, and that the shared nodes
     * above it are reclaimed once that role goes too.
     *
     * The leader and its follower share the tablet_id= node and are reported by two
     * different actors, so a cleanup that reached above the leaf too eagerly would detach
     * the other actor's live counters for good.
     */
    Y_UNIT_TEST(ForgetTabletKeepsTheOtherRole) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet leader(1000, 0, LEVEL_PARTITION);
        TFakeTablet follower(1000, 1, LEVEL_PARTITION);

        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);

        leader.SendUpdate(env);
        follower.SendUpdate(env);
        ReportCounters(env, {&leader, &follower});

        UNIT_ASSERT(FindLeafCounters(env, 1000, 0));
        UNIT_ASSERT(FindLeafCounters(env, 1000, 1));

        leader.SendForget(env);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(1));

        // The leader's own leaf is gone ...
        UNIT_ASSERT(!FindLeafCounters(env, 1000, 0));

        // ... the shared spine above it stays, because the follower is still there ...
        UNIT_ASSERT(FindTableGroup(env));

        // ... and the follower's leaf is still reachable from the root
        UNIT_ASSERT(FindLeafCounters(env, 1000, 1));
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindLeafCounters(env, 1000, 1), "SUM", ALLOWED_EXECUTOR_COUNTER), 2u);

        // The follower goes too, so nothing of this tablet is left on the node: the
        // tablet_id=, detailed_metrics=, table= and database= nodes are all reclaimed,
        // which is what a rebalanced away tablet must leave behind — nothing
        follower.SendForget(env);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT(!FindLeafCounters(env, 1000, 1));
        UNIT_ASSERT(!FindTableGroup(env));

        // The private root of the node itself stays: it is created once at boot, not
        // per database
        auto rawGroup = FindRawGroup(env);
        UNIT_ASSERT(rawGroup);
        UNIT_ASSERT(!rawGroup->FindSubgroup("database", DATABASE_PATH));
    }

    /**
     * Verify the same for two followers of one tablet, which a node holds while it is
     * drained or rolled: unlike the leader and its follower, the two are remembered by
     * ONE actor under one and the same tablet id, so forgetting one of them must not
     * take the counters of the other with it.
     */
    Y_UNIT_TEST(ForgetFollowerKeepsTheOtherFollower) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet follower1(1000, 1, LEVEL_PARTITION);
        TFakeTablet follower2(1000, 2, LEVEL_PARTITION);

        follower1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        follower2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);

        follower1.SendUpdate(env);
        follower2.SendUpdate(env);
        ReportCounters(env, {&follower1, &follower2});

        UNIT_ASSERT(FindLeafCounters(env, 1000, 1));
        UNIT_ASSERT(FindLeafCounters(env, 1000, 2));

        follower1.SendForget(env);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT(!FindLeafCounters(env, 1000, 1));
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindLeafCounters(env, 1000, 2), "SUM", ALLOWED_EXECUTOR_COUNTER), 2u);

        // The one, which is left, keeps reporting into its own leaf
        follower2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 3);
        ReportCounters(env, {&follower2});

        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindLeafCounters(env, 1000, 2), "SUM", ALLOWED_EXECUTOR_COUNTER), 3u);

        follower2.SendForget(env);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT(!FindTableGroup(env));
    }

    /**
     * Verify that only the allow-listed counter set of the tablet type is published:
     * an Executor counter, which is not in counters_detailed_datashard.proto, must
     * not appear anywhere in the tree.
     */
    Y_UNIT_TEST(PublishesOnlyTheDetailedMetricsCounterSet) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet leader(1000, 0, LEVEL_PARTITION);
        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        leader.SetSimple(LOG_REDO_ITEMS, 5);

        leader.SendUpdate(env);
        ReportCounters(env, {&leader});

        auto executorCounters = FindLeafCounters(env, 1000, 0);
        UNIT_ASSERT(executorCounters);

        UNIT_ASSERT(executorCounters->FindNamedCounter("sensor", "SUM(" + ALLOWED_EXECUTOR_COUNTER + ")"));
        UNIT_ASSERT(!executorCounters->FindNamedCounter("sensor", "SUM(" + UNLISTED_EXECUTOR_COUNTER + ")"));
        UNIT_ASSERT(!executorCounters->FindNamedCounter("sensor", "MAX(" + UNLISTED_EXECUTOR_COUNTER + ")"));
        UNIT_ASSERT(!executorCounters->FindCounter(UNLISTED_EXECUTOR_COUNTER));
    }

    /**
     * Verify that when a Table level table's level drops to Disabled, the bucket it
     * published is withdrawn, not just frozen in place.
     */
    Y_UNIT_TEST(TableLevelDisabledDropsTheBucket) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet leader1(1000, 0, LEVEL_TABLE);
        TFakeTablet leader2(2000, 0, LEVEL_TABLE);

        leader1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        leader2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);

        leader1.SendUpdate(env);
        leader2.SendUpdate(env);
        ReportCounters(env, {&leader1, &leader2});

        UNIT_ASSERT(FindTableBucketCounters(env));

        leader1.SetMetricsLevel(LEVEL_DISABLED);
        leader1.SendTableInfo(env);
        leader2.SetMetricsLevel(LEVEL_DISABLED);
        leader2.SendTableInfo(env);
        ReportCounters(env, {&leader1, &leader2});

        UNIT_ASSERT(!FindTableBucketCounters(env));
        UNIT_ASSERT(!FindRawGroup(env)->FindSubgroup("database", DATABASE_PATH));
    }

    /**
     * Same as above for a Partition level table: both leaves must be withdrawn.
     */
    Y_UNIT_TEST(PartitionLevelDisabledDropsTheLeaves) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet leader(1000, 0, LEVEL_PARTITION);
        TFakeTablet follower(1000, 1, LEVEL_PARTITION);

        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);

        leader.SendUpdate(env);
        follower.SendUpdate(env);
        ReportCounters(env, {&leader, &follower});

        UNIT_ASSERT(FindLeafCounters(env, 1000, 0));
        UNIT_ASSERT(FindLeafCounters(env, 1000, 1));

        leader.SetMetricsLevel(LEVEL_DISABLED);
        leader.SendTableInfo(env);
        follower.SetMetricsLevel(LEVEL_DISABLED);
        follower.SendTableInfo(env);
        ReportCounters(env, {&leader, &follower});

        UNIT_ASSERT(!FindLeafCounters(env, 1000, 0));
        UNIT_ASSERT(!FindLeafCounters(env, 1000, 1));
    }

    /**
     * Verify that disabling one follower's level withdraws only its own leaf, not the
     * sibling follower's -- the withdraw is per tablet+follower, not per table.
     */
    Y_UNIT_TEST(PartitionLevelDisabledDropsOnlyTheDisabledFollower) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet follower1(1000, 1, LEVEL_PARTITION);
        TFakeTablet follower2(1000, 2, LEVEL_PARTITION);

        follower1.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        follower2.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);

        follower1.SendUpdate(env);
        follower2.SendUpdate(env);
        ReportCounters(env, {&follower1, &follower2});

        follower1.SetMetricsLevel(LEVEL_DISABLED);
        follower1.SendTableInfo(env);
        ReportCounters(env, {&follower1, &follower2});

        UNIT_ASSERT(!FindLeafCounters(env, 1000, 1));
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindLeafCounters(env, 1000, 2), "SUM", ALLOWED_EXECUTOR_COUNTER), 2u);
    }

    /**
     * Verify that the follower actor gets its OWN db watcher and registers its own
     * watch, even though EnableDbCounters (the leader-only db counters feature) is off
     * in TEnv -- detailed metrics alone must be enough to create the watcher.
     */
    Y_UNIT_TEST(FollowerActorWatchesItsDetailedMetricsDatabase) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet follower(1000, 1, LEVEL_PARTITION);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        follower.SendUpdate(env);
        ReportCounters(env, {&follower});

        UNIT_ASSERT(env.WatchedPathIds.contains(TENANT_PATH_ID));
    }

    /**
     * Verify that the follower actor's own TEvRemoveDatabase drops its own leaves (and
     * the leader's TEvRemoveDatabase drops the leader's).
     */
    Y_UNIT_TEST(RemoveDatabaseDropsFollowerLeaves) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet leader(1000, 0, LEVEL_PARTITION);
        TFakeTablet follower(1000, 1, LEVEL_PARTITION);

        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);

        leader.SendUpdate(env);
        follower.SendUpdate(env);
        ReportCounters(env, {&leader, &follower});

        UNIT_ASSERT(FindLeafCounters(env, 1000, 0));
        UNIT_ASSERT(FindLeafCounters(env, 1000, 1));

        env.Runtime.Send(new IEventHandle(env.LeaderAggregatorId, env.Edge,
            new TEvTabletCounters::TEvRemoveDatabase(DATABASE_PATH, TENANT_PATH_ID)));
        env.Runtime.Send(new IEventHandle(env.FollowerAggregatorId, env.Edge,
            new TEvTabletCounters::TEvRemoveDatabase(DATABASE_PATH, TENANT_PATH_ID)));
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT(!FindLeafCounters(env, 1000, 0));
        UNIT_ASSERT(!FindLeafCounters(env, 1000, 1));

        auto rawGroup = FindRawGroup(env);
        UNIT_ASSERT(rawGroup);
        UNIT_ASSERT(!rawGroup->FindSubgroup("database", DATABASE_PATH));
    }

    /**
     * Pin that the two actors are independently notified rather than accidentally
     * coupled: TEvRemoveDatabase sent to the leader alone must not touch the follower.
     */
    Y_UNIT_TEST(RemoveDatabaseOnlyReachesItsOwnRole) {
        TEnv env(true /* detailedMetricsEnabled */);

        TFakeTablet leader(1000, 0, LEVEL_PARTITION);
        TFakeTablet follower(1000, 1, LEVEL_PARTITION);

        leader.SetSimple(DB_UNIQUE_ROWS_TOTAL, 1);
        follower.SetSimple(DB_UNIQUE_ROWS_TOTAL, 2);

        leader.SendUpdate(env);
        follower.SendUpdate(env);
        ReportCounters(env, {&leader, &follower});

        env.Runtime.Send(new IEventHandle(env.LeaderAggregatorId, env.Edge,
            new TEvTabletCounters::TEvRemoveDatabase(DATABASE_PATH, TENANT_PATH_ID)));
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT(!FindLeafCounters(env, 1000, 0));
        UNIT_ASSERT_VALUES_EQUAL(
            GetCounterValue(FindLeafCounters(env, 1000, 1), "SUM", ALLOWED_EXECUTOR_COUNTER), 2u);
    }
}

}
