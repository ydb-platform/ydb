#include <ydb/core/actor_tracing/tree_broadcast.h>
#include <ydb/core/actor_tracing/tracing_service.h>
#include <ydb/core/actor_tracing/tracing_events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NKikimr::NActorTracing;

Y_UNIT_TEST_SUITE(GetDirectChildren) {

    Y_UNIT_TEST(Empty) {
        UNIT_ASSERT(GetDirectChildren({}).empty());
    }

    Y_UNIT_TEST(SingleNode) {
        auto children = GetDirectChildren({42u});
        UNIT_ASSERT_VALUES_EQUAL(children.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(children[0].first, 42u);
        UNIT_ASSERT(children[0].second.empty());
    }

    Y_UNIT_TEST(ThreeNodesDefaultFanOut) {
        // K=8 > 3 nodes, so 3 groups of 1: each node is a leaf
        auto children = GetDirectChildren({1u, 2u, 3u});
        UNIT_ASSERT_VALUES_EQUAL(children.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(children[0].first, 1u);
        UNIT_ASSERT(children[0].second.empty());
        UNIT_ASSERT_VALUES_EQUAL(children[1].first, 2u);
        UNIT_ASSERT(children[1].second.empty());
        UNIT_ASSERT_VALUES_EQUAL(children[2].first, 3u);
        UNIT_ASSERT(children[2].second.empty());
    }

    Y_UNIT_TEST(NineNodesK3) {
        // K=3, 9 nodes -> 3 groups of 3: {1,[2,3]}, {4,[5,6]}, {7,[8,9]}
        TVector<ui32> nodes = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        auto children = GetDirectChildren(nodes, 3);
        UNIT_ASSERT_VALUES_EQUAL(children.size(), 3u);

        UNIT_ASSERT_VALUES_EQUAL(children[0].first, 1u);
        UNIT_ASSERT_VALUES_EQUAL(children[0].second, (TVector<ui32>{2u, 3u}));

        UNIT_ASSERT_VALUES_EQUAL(children[1].first, 4u);
        UNIT_ASSERT_VALUES_EQUAL(children[1].second, (TVector<ui32>{5u, 6u}));

        UNIT_ASSERT_VALUES_EQUAL(children[2].first, 7u);
        UNIT_ASSERT_VALUES_EQUAL(children[2].second, (TVector<ui32>{8u, 9u}));
    }

    Y_UNIT_TEST(UnevenGroupsK3) {
        // K=3, 7 nodes -> groups of 3,2,2: {1,[2,3]}, {4,[5]}, {6,[7]}
        TVector<ui32> nodes = {1, 2, 3, 4, 5, 6, 7};
        auto children = GetDirectChildren(nodes, 3);
        UNIT_ASSERT_VALUES_EQUAL(children.size(), 3u);

        UNIT_ASSERT_VALUES_EQUAL(children[0].first, 1u);
        UNIT_ASSERT_VALUES_EQUAL(children[0].second, (TVector<ui32>{2u, 3u}));

        UNIT_ASSERT_VALUES_EQUAL(children[1].first, 4u);
        UNIT_ASSERT_VALUES_EQUAL(children[1].second, (TVector<ui32>{5u}));

        UNIT_ASSERT_VALUES_EQUAL(children[2].first, 6u);
        UNIT_ASSERT_VALUES_EQUAL(children[2].second, (TVector<ui32>{7u}));
    }

    Y_UNIT_TEST(AllNodesVisitedExactlyOnce) {
        TVector<ui32> nodes;
        for (ui32 i = 1; i <= 20; ++i) {
            nodes.push_back(i);
        }

        std::function<void(const TVector<ui32>&)> visit = [&](const TVector<ui32>& subtree) {
            for (auto& [childId, childSubtree] : GetDirectChildren(subtree, 4)) {
                visit(childSubtree);
            }
        };

        THashSet<ui32> seen;
        for (auto& [childId, childSubtree] : GetDirectChildren(nodes, 4)) {
            UNIT_ASSERT(seen.insert(childId).second);
            std::function<void(const TVector<ui32>&)> collectAll = [&](const TVector<ui32>& s) {
                for (auto& [id, sub] : GetDirectChildren(s, 4)) {
                    UNIT_ASSERT(seen.insert(id).second);
                    collectAll(sub);
                }
            };
            collectAll(childSubtree);
        }

        UNIT_ASSERT_VALUES_EQUAL(seen.size(), nodes.size());
        for (ui32 id : nodes) {
            UNIT_ASSERT_C(seen.contains(id), "Node " << id << " not visited");
        }
    }
}

Y_UNIT_TEST_SUITE(TraceFetchGatherer) {

    Y_UNIT_TEST(LocalTraceWinsIfLarger) {
        TTestActorRuntimeBase runtime(1, false);
        runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&, TDuration, TInstant&) {
            return false;
        });
        runtime.Initialize();

        auto edgeActor = runtime.AllocateEdgeActor();

        auto makeReplyActor = [](TString traceData) -> IActor* {
            struct TFakeService : TActorBootstrapped<TFakeService> {
                TString TraceData;
                TFakeService(TString data) : TraceData(std::move(data)) {}
                void Bootstrap(const TActorContext&) { Become(&TThis::StateWork); }
                STFUNC(StateWork) {
                    if (ev->GetTypeRewrite() == TEvTracing::TEvTraceFetch::EventType) {
                        auto result = MakeHolder<TEvTracing::TEvTraceFetchResult>();
                        result->Record.SetSuccess(true);
                        result->Record.SetTraceData(TraceData);
                        Send(ev->Sender, result.Release());
                    }
                }
            };
            return new TFakeService(std::move(traceData));
        };

        const ui32 node0 = runtime.GetNodeId(0);
        const ui32 childNode = node0 + 1;
        const TString childTrace = "x";
        const TString localTrace = "local trace is the longest one here";

        runtime.RegisterService(MakeActorTracingServiceId(childNode), runtime.Register(makeReplyActor(childTrace)), 0);

        TVector<ui32> subtree = {childNode};
        runtime.Register(CreateTraceFetchGatherer(edgeActor, localTrace, std::move(subtree)));

        TAutoPtr<IEventHandle> handle;
        auto* result = runtime.GrabEdgeEventRethrow<TEvTracing::TEvTraceFetchResult>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetTraceData(), localTrace);
    }
}
