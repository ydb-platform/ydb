#include "ut/ut_utils.h"
#include <ydb/core/mon/ut_utils/ut_utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/http/fetch/httpheader.h>
#include <library/cpp/http/misc/httpcodes.h>

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/viewer/viewer.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include "json_pipe_req.h"

#include <algorithm>

using namespace NKikimr;
using namespace NViewer;
using namespace NNodeWhiteboard;
using namespace Tests;
using namespace NKikimr::NViewerTests;

namespace {

// Collect the recipient of every TEvUnsubscribe once collection is armed.
// TViewerPipeClient::PassAway() sends TEvUnsubscribe to
// TActivationContext::InterconnectProxy(nodeId); CountForNode() below maps a
// recipient back to its destination node. Collection is armed only after the
// request is dispatched so that we observe exactly the unsubscribes emitted by
// PassAway (and not any interconnect churn during cluster startup).
struct TUnsubscribeCollector {
    bool Armed = false;
    std::vector<TActorId> Recipients;

    TTestActorRuntime::TEventObserver MakeObserver(std::function<TTestActorRuntime::EEventAction(TAutoPtr<IEventHandle>&)> chain = {}) {
        return [this, chain](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
            if (Armed && ev->GetTypeRewrite() == TEvents::TEvUnsubscribe::EventType) {
                Recipients.push_back(ev->Recipient);
            }
            if (chain) {
                return chain(ev);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
    }

    // PassAway sends TEvUnsubscribe to TActivationContext::InterconnectProxy(nodeId).
    // Depending on whether the interconnect proxy actor is already registered in
    // the sim, the observer sees either the resolved proxy actor id (resolvedProxy)
    // or the unresolved ICProxy service id (NActors::MakeInterconnectProxyId(nodeId)).
    // Accept both forms.
    size_t CountForNode(TNodeId nodeId, const TActorId& resolvedProxy) const {
        size_t count = 0;
        for (const TActorId& r : Recipients) {
            if (r == resolvedProxy) {
                ++count;
            } else if (NActors::IsInterconnectProxyId(r) && NActors::GetInterconnectProxyNode(r) == nodeId) {
                ++count;
            }
        }
        return count;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(ViewerSubscriptions) {

    // ---------------------------------------------------------------------
    // Test 0 - deterministic seam tests. Drive the fan-out helpers directly
    // through minimal stub actors so that we can assert on the protected
    // SubscriptionNodeIds member without depending on any handler routing
    // logic. Each helper has its own Y_UNIT_TEST below so that its tracking and
    // unsubscribe assertions are reached independently (independent per-helper
    // RED->GREEN attribution for leaks #1 and #2).
    // ---------------------------------------------------------------------

    class TWhiteboardStubPipeClient : public TViewerPipeClient {
    public:
        using TViewerPipeClient::SubscriptionNodeIds;
        TNodeId TargetNode = 0;

        void Bootstrap() override {
            auto r1 = MakeWhiteboardRequest(TargetNode, new TEvWhiteboard::TEvSystemStateRequest());
            auto r2 = MakeWhiteboardRequest(TargetNode, new TEvWhiteboard::TEvSystemStateRequest()); // duplicate
            Y_UNUSED(r1);
            Y_UNUSED(r2);
            Become(&TWhiteboardStubPipeClient::StateWork);
        }

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                case TEvents::TSystem::Poison:
                    ReplyAndPassAway();
                    break;
                default:
                    break;
            }
        }

        void ReplyAndPassAway() override {
            PassAway();
        }
    };

    class TViewerStubPipeClient : public TViewerPipeClient {
    public:
        using TViewerPipeClient::SubscriptionNodeIds;
        TNodeId TargetNode = 0;

        void Bootstrap() override {
            auto r1 = MakeViewerRequest(TargetNode, new TEvViewer::TEvViewerRequest());
            Y_UNUSED(r1);
            Become(&TViewerStubPipeClient::StateWork);
        }

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                case TEvents::TSystem::Poison:
                    ReplyAndPassAway();
                    break;
                default:
                    break;
            }
        }

        void ReplyAndPassAway() override {
            PassAway();
        }
    };

    // Seam test for leak #1 (whiteboard fan-out). Drives MakeWhiteboardRequest in
    // isolation so both its tracking and its PassAway unsubscribe are asserted
    // independently of the viewer helper.
    //   RED  (pre-fix): SubscriptionNodeIds empty -> wbCount 0 != 2, and no unsubscribe.
    //   GREEN (post-fix): both calls track (count == 2 pre-dedup) and PassAway
    //                     emits exactly one TEvUnsubscribe to the remote node.
    Y_UNIT_TEST(WhiteboardHelperTracksAndUnsubscribes) {
        TPortManager tp;
        auto settings = TServerSettings(tp.GetPort(2134))
                .SetNodeCount(2)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .InitKikimrRunConfig();
        TServer server(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        const TNodeId remoteNodeId = runtime.GetNodeId(1);
        const TActorId remoteProxy = runtime.GetInterconnectProxy(0, 1);

        TUnsubscribeCollector collector;
        runtime.SetObserverFunc(collector.MakeObserver());

        auto* wbStub = new TWhiteboardStubPipeClient();
        wbStub->TargetNode = remoteNodeId;
        TActorId wbActor = runtime.Register(wbStub, 0);
        runtime.SimulateSleep(TDuration::Seconds(1));

        // MakeWhiteboardRequest was called twice for the remote node -> tracked twice.
        const size_t wbCount = std::count(wbStub->SubscriptionNodeIds.begin(),
                                          wbStub->SubscriptionNodeIds.end(), remoteNodeId);
        UNIT_ASSERT_VALUES_EQUAL_C(wbCount, 2u,
            "MakeWhiteboardRequest must record the target node in SubscriptionNodeIds for each subscribed request");

        collector.Armed = true;
        runtime.Send(new IEventHandle(wbActor, TActorId(), new TEvents::TEvPoison()));
        runtime.SimulateSleep(TDuration::Seconds(1));

        // PassAway sorts+dedups SubscriptionNodeIds, so exactly one unsubscribe
        // reaches the remote node's interconnect proxy.
        UNIT_ASSERT_VALUES_EQUAL_C(collector.CountForNode(remoteNodeId, remoteProxy), 1u,
            "PassAway must send exactly one TEvUnsubscribe to the remote node for the whiteboard helper subscription");
    }

    // Seam test for leak #2 (viewer fan-out). Drives MakeViewerRequest in
    // isolation for deterministic per-helper attribution of the MakeViewerRequest
    // edit; the end-to-end coverage is PassAwayUnsubscribesViewerOffloadFanout
    // below.
    //   RED  (pre-fix): SubscriptionNodeIds empty -> vCount 0 != 1, and no unsubscribe.
    //   GREEN (post-fix): the target is tracked once and PassAway emits exactly
    //                     one TEvUnsubscribe to the remote node.
    Y_UNIT_TEST(ViewerHelperTracksAndUnsubscribes) {
        TPortManager tp;
        auto settings = TServerSettings(tp.GetPort(2134))
                .SetNodeCount(2)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .InitKikimrRunConfig();
        TServer server(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();

        const TNodeId remoteNodeId = runtime.GetNodeId(1);
        const TActorId remoteProxy = runtime.GetInterconnectProxy(0, 1);

        TUnsubscribeCollector collector;
        runtime.SetObserverFunc(collector.MakeObserver());

        auto* vStub = new TViewerStubPipeClient();
        vStub->TargetNode = remoteNodeId;
        TActorId vActor = runtime.Register(vStub, 0);
        runtime.SimulateSleep(TDuration::Seconds(1));

        const size_t vCount = std::count(vStub->SubscriptionNodeIds.begin(),
                                         vStub->SubscriptionNodeIds.end(), remoteNodeId);
        UNIT_ASSERT_VALUES_EQUAL_C(vCount, 1u,
            "MakeViewerRequest must record the target node in SubscriptionNodeIds");

        collector.Armed = true;
        runtime.Send(new IEventHandle(vActor, TActorId(), new TEvents::TEvPoison()));
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL_C(collector.CountForNode(remoteNodeId, remoteProxy), 1u,
            "PassAway must send exactly one TEvUnsubscribe to the remote node for the viewer helper subscription");
    }

    // ---------------------------------------------------------------------
    // Shared integration scaffolding: drive the real /viewer/json/cluster handler
    // (TJsonCluster) via the HTTP request path and observe which interconnect
    // proxies receive TEvUnsubscribe when the handler tears down in PassAway.
    // The mon (NMon::TEvHttpInfo) path is not used because TJsonCluster is
    // registered as an HTTP-only handler.
    // ---------------------------------------------------------------------

    NHttp::THttpIncomingRequestPtr MakeGetRequest(const TString& uri) {
        auto endpoint = std::make_shared<NHttp::THttpEndpointInfo>();
        return new NHttp::THttpIncomingRequest(
            TStringBuilder() << "GET " << uri << " HTTP/1.1\r\n\r\n", endpoint, {});
    }

    Y_UNIT_TEST(PassAwayUnsubscribesWhiteboardFanout) {
        TPortManager tp;
        auto settings = TServerSettings(tp.GetPort(2134))
                .SetNodeCount(2)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .InitKikimrRunConfig();
        TServer server(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();
        TActorId sender = runtime.AllocateEdgeActor();

        const TNodeId remoteNodeId = runtime.GetNodeId(1);
        const TActorId remoteProxy = runtime.GetInterconnectProxy(0, 1);

        bool sawWhiteboardToRemote = false;
        TUnsubscribeCollector collector;
        auto pathGuard = [&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
            switch (ev->GetTypeRewrite()) {
                case TEvWhiteboard::EvSystemStateRequest:
                case TEvWhiteboard::EvTabletStateRequest:
                    if (ev->Recipient.NodeId() == remoteNodeId) {
                        sawWhiteboardToRemote = true;
                    }
                    break;
                default:
                    break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(collector.MakeObserver(pathGuard));

        // offload_merge=false forces the whiteboard fan-out (viewer_cluster.h
        // non-offload branch) which subscribes to every non-disconnected node.
        auto request = MakeGetRequest("/viewer/json/cluster?tablets=true&offload_merge=false&direct=1");

        collector.Armed = true;
        TAutoPtr<IEventHandle> handle;
        runtime.Send(new IEventHandle(NKikimr::NViewer::MakeViewerID(0), sender,
            new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(request), 0));
        runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_C(sawWhiteboardToRemote,
            "path guard: the non-offload whiteboard fan-out must send a whiteboard request to the remote node");
        // TJsonCluster subscribes to the remote node through two independent
        // pipe-client actors that PassAway independently: one via the generic
        // MakeRequest helper (which already tracked the node before this fix) and
        // one via the whiteboard fan-out MakeWhiteboardRequest (the leak this fix
        // closes). Each tracked actor emits exactly one TEvUnsubscribe to the
        // remote node on teardown, so:
        //   RED   (pre-fix): count == 1 (only the generic MakeRequest path)
        //   GREEN (post-fix): count == 2 (generic path + whiteboard fan-out)
        // Asserting the exact count isolates the whiteboard-fan-out regression.
        UNIT_ASSERT_VALUES_EQUAL_C(collector.CountForNode(remoteNodeId, remoteProxy), 2u,
            "PassAway must unsubscribe the remote node for the whiteboard fan-out subscription (in addition to the generic request subscription)");
    }

    // Integration test for leak #2 (viewer offload fan-out). Drives the real
    // TJsonCluster handler with offload_merge=true (default) and forces the
    // offload merge target onto the remote node by rewriting the local
    // TEvWhiteboard::TEvNodeStateResponse so the remote node reports Connected.
    // Per TNode::GetCandidateScore (viewer_cluster.h:51) that gives the remote
    // node score 110 (Connected*100 + Static*10) vs the local node's 10, so the
    // stable candidate sort (SplitBatch:188) puts the remote node first and
    // ChooseNodeId returns it -> MakeViewerRequest(remoteNode).
    Y_UNIT_TEST(PassAwayUnsubscribesViewerOffloadFanout) {
        TPortManager tp;
        auto settings = TServerSettings(tp.GetPort(2134))
                .SetNodeCount(2)
                .SetUseRealThreads(false)
                .SetDomainName("Root")
                .SetUseSectorMap(true)
                .InitKikimrRunConfig();
        TServer server(settings);
        TTestActorRuntime& runtime = *server.GetRuntime();
        TActorId sender = runtime.AllocateEdgeActor();

        const TNodeId remoteNodeId = runtime.GetNodeId(1);
        const TActorId remoteProxy = runtime.GetInterconnectProxy(0, 1);

        bool sawViewerToRemote = false;
        // In the test harness the per-node viewer service is registered under
        // MakeViewerID(nodeIndex) (see test_client.cpp), whereas the offload
        // fan-out addresses MakeViewerID(nodeId). For nodeIndex 1 that is
        // MakeViewerID(1) vs the handler's MakeViewerID(GetNodeId(1)), so the
        // cross-node TEvViewerRequest would otherwise be undeliverable. Register
        // an edge actor on the remote node under the id the handler actually
        // targets, so the request is delivered and observable.
        TActorId remoteViewerEdge = runtime.AllocateEdgeActor(1);
        runtime.RegisterService(NKikimr::NViewer::MakeViewerID(remoteNodeId), remoteViewerEdge, 1);

        TUnsubscribeCollector collector;
        auto forceRemoteAndGuard = [&](TAutoPtr<IEventHandle>& ev) -> TTestActorRuntime::EEventAction {
            switch (ev->GetTypeRewrite()) {
                case TEvWhiteboard::EvNodeStateResponse: {
                    // Mark the remote node Connected so it wins the offload
                    // candidate sort (score 110 vs the local node's 10) and
                    // batch.ChooseNodeId() selects it -> MakeViewerRequest(remote).
                    auto* msg = ev->Get<TEvWhiteboard::TEvNodeStateResponse>();
                    auto& record = msg->Record;
                    record.ClearNodeStateInfo();
                    auto* info = record.AddNodeStateInfo();
                    info->SetPeerName(TStringBuilder() << remoteNodeId << ":65535");
                    info->SetConnected(true);
                    break;
                }
                case TEvViewer::EvViewerRequest:
                    if (ev->Recipient.NodeId() == remoteNodeId) {
                        sawViewerToRemote = true;
                    }
                    break;
                default:
                    break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(collector.MakeObserver(forceRemoteAndGuard));

        // Default offload_merge=true routes the fan-out through MakeViewerRequest.
        auto request = MakeGetRequest("/viewer/json/cluster?tablets=true&direct=1");

        collector.Armed = true;
        TAutoPtr<IEventHandle> handle;
        runtime.Send(new IEventHandle(NKikimr::NViewer::MakeViewerID(0), sender,
            new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(request), 0));
        runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        runtime.SimulateSleep(TDuration::Seconds(1));

        UNIT_ASSERT_C(sawViewerToRemote,
            "path guard: the offload viewer fan-out must send a TEvViewerRequest to the remote node");
        // TJsonCluster subscribes to the remote node through two independent
        // pipe-client actors that PassAway independently: one via the generic
        // MakeRequest helper (which already tracked the node before this fix) and
        // one via the offload viewer fan-out MakeViewerRequest (the leak this fix
        // closes). Each tracked actor emits exactly one TEvUnsubscribe to the
        // remote node on teardown, so:
        //   RED   (MakeViewerRequest edit reverted): count == 1 (generic path only)
        //   GREEN (fix applied):                     count == 2 (generic + offload viewer)
        // Asserting the exact count isolates the offload-viewer-fan-out regression.
        UNIT_ASSERT_VALUES_EQUAL_C(collector.CountForNode(remoteNodeId, remoteProxy), 2u,
            "PassAway must unsubscribe the remote node for the offload viewer fan-out subscription (in addition to the generic request subscription)");
    }
}
