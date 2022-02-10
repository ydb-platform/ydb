#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/actors/core/interconnect.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include "tablet_counters_aggregator.h"

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
        THolder<TEvInterconnect::TEvNodesInfo> nodesInfo = MakeHolder<TEvInterconnect::TEvNodesInfo>();
        for (auto i = 1; i <= NODES; ++i) {
            nodesInfo->Nodes.emplace_back(TEvInterconnect::TNodeInfo(i, "::", "localhost", "localhost", 1234, TNodeLocation())); 
        }
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
            THolder<TEvInterconnect::TEvNodesInfo> nodesInfo = MakeHolder<TEvInterconnect::TEvNodesInfo>();
            nodesInfo->Nodes.emplace_back(TEvInterconnect::TNodeInfo(1, "::", "localhost", "localhost", 1234, TNodeLocation())); 
            nodesInfo->Nodes.emplace_back(TEvInterconnect::TNodeInfo(2, "::", "localhost", "localhost", 1234, TNodeLocation())); 
            nodesInfo->Nodes.emplace_back(TEvInterconnect::TNodeInfo(3, "::", "localhost", "localhost", 1234, TNodeLocation())); 
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
            THolder<TEvInterconnect::TEvNodesInfo> nodesInfo = MakeHolder<TEvInterconnect::TEvNodesInfo>();
            nodesInfo->Nodes.emplace_back(TEvInterconnect::TNodeInfo(1, "::", "localhost", "localhost", 1234, TNodeLocation())); 
            nodesInfo->Nodes.emplace_back(TEvInterconnect::TNodeInfo(2, "::", "localhost", "localhost", 1234, TNodeLocation())); 
            nodesInfo->Nodes.emplace_back(TEvInterconnect::TNodeInfo(3, "::", "localhost", "localhost", 1234, TNodeLocation())); 
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

}

}
