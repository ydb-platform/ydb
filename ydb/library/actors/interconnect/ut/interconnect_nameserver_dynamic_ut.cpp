#include <ydb/library/actors/dnsresolver/dnsresolver.h>
#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NActors {
namespace {

class TDynamicNameserverTestEnv {
public:
    static constexpr ui32 NodeId = 42;

    explicit TDynamicNameserverTestEnv(TDuration pendingPeriod = TDuration::Seconds(3)) {
        Runtime.Initialize();
        Nameserver = Runtime.Register(CreateDynamicNameserver(
            MakeIntrusive<TTableNameserverSetup>(), pendingPeriod));
        Sender = Runtime.AllocateEdgeActor();
        Runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event,
                TDuration, TInstant&) {
            return event->GetTypeRewrite() != TEvents::TEvWakeup::EventType;
        });
    }

    void Resolve(TDuration timeout) {
        Runtime.Send(new IEventHandle(Nameserver, Sender,
            new TEvInterconnect::TEvResolveNode(NodeId, Runtime.GetCurrentMonotonicTime() + timeout)));
    }

    void AnnounceNode(const TString& address = "127.0.0.42", const TString& resolveHost = "") {
        auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
        nodes->emplace_back(NodeId, address, resolveHost, resolveHost, 19001, TNodeLocation());
        Runtime.Send(new IEventHandle(Nameserver, Sender, new TEvInterconnect::TEvNodesInfo(nodes)));
    }

    void CheckTimeout(TDuration requestTimeout, TDuration expectedTimeout) {
        const auto deadline = Runtime.GetCurrentMonotonicTime() + expectedTimeout;
        Resolve(requestTimeout);

        const auto reply = Runtime.GrabEdgeEvent<TEvLocalNodeInfo>(Sender, TDuration::Seconds(4));
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->NodeId, NodeId);
        UNIT_ASSERT(reply->Get()->Addresses.empty());
        const auto now = Runtime.GetCurrentMonotonicTime();
        UNIT_ASSERT(now >= deadline);
        // Expiry is checked by a periodic timer, so a reply may arrive one tick later.
        UNIT_ASSERT(now <= deadline + TDuration::MilliSeconds(200));

        // A later discovery must not produce a second reply to the timed-out request.
        AnnounceNode();
        Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(Runtime.CaptureMailboxEvents(Sender.Hint(), Sender.NodeId()).empty());
    }

    void CheckLateDiscovery(const TString& address, TDuration discoveryDelay) {
        Resolve(TDuration::MilliSeconds(1130));
        Runtime.SimulateSleep(discoveryDelay);
        // Discovery arrives after the 1 s tick but before the 1.2 s tick.
        AnnounceNode(address, "node.test");

        TAutoPtr<IEventHandle> reply;
        const auto [nodeInfo, resolveError] = Runtime.GrabEdgeEvents<TEvLocalNodeInfo, TEvResolveError>(
            reply, TDuration::Seconds(1));
        UNIT_ASSERT(nodeInfo);
        UNIT_ASSERT(!resolveError);
        UNIT_ASSERT_VALUES_EQUAL(nodeInfo->NodeId, NodeId);
        UNIT_ASSERT(nodeInfo->Addresses.empty());

        Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(Runtime.CaptureMailboxEvents(Sender.Hint(), Sender.NodeId()).empty());
    }

    TTestActorRuntimeBase Runtime;
    TActorId Nameserver;
    TActorId Sender;
};

} // namespace

Y_UNIT_TEST_SUITE(InterconnectDynamicNameserver) {
    Y_UNIT_TEST(ResolvesNodeDiscoveredBeforeDeadline) {
        TDynamicNameserverTestEnv env;
        env.Resolve(TDuration::Seconds(5));

        // Let the periodic timeout check run while the node is still unknown.
        env.Runtime.SimulateSleep(TDuration::Seconds(1));
        env.AnnounceNode();

        const auto reply = env.Runtime.GrabEdgeEvent<TEvLocalNodeInfo>(env.Sender, TDuration::Seconds(1));
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->NodeId, TDynamicNameserverTestEnv::NodeId);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Addresses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Addresses.front().GetAddress(), "127.0.0.42");
    }

    Y_UNIT_TEST(ExpiresAtRequestDeadline) {
        TDynamicNameserverTestEnv env;
        env.CheckTimeout(TDuration::MilliSeconds(1050), TDuration::MilliSeconds(1050));
    }

    Y_UNIT_TEST(ExpiresAtPendingPeriod) {
        TDynamicNameserverTestEnv env(TDuration::MilliSeconds(3050));
        env.CheckTimeout(TDuration::Seconds(5), TDuration::MilliSeconds(3050));
    }

    Y_UNIT_TEST(ExpiresBeforeLateAddressDiscovery) {
        TDynamicNameserverTestEnv env;
        env.CheckLateDiscovery("127.0.0.42", TDuration::MilliSeconds(1150));
    }

    Y_UNIT_TEST(ExpiresBeforeLateHostnameDiscovery) {
        TDynamicNameserverTestEnv env;
        env.CheckLateDiscovery("", TDuration::MilliSeconds(1150));
    }

    Y_UNIT_TEST(ExpiresBeforeDiscoveryAtDeadline) {
        TDynamicNameserverTestEnv env;
        env.CheckLateDiscovery("127.0.0.42", TDuration::MilliSeconds(1130));
    }

    Y_UNIT_TEST(PreservesResolveActorTimeout) {
        TDynamicNameserverTestEnv env;
        const auto dnsResolver = env.Runtime.AllocateEdgeActor();
        env.Runtime.RegisterService(NDnsResolver::MakeDnsResolverActorId(), dnsResolver);
        env.Resolve(TDuration::MilliSeconds(1050));
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
        env.AnnounceNode("", "node.test");

        const auto request = env.Runtime.GrabEdgeEvent<NDnsResolver::TEvDns::TEvGetHostByName>(
            dnsResolver, TDuration::Seconds(1));
        UNIT_ASSERT(request);
        // Leave DNS unanswered and let the resolve actor's own deadline timer fire.
        env.Runtime.SimulateSleep(TDuration::Seconds(1));
        const auto replies = env.Runtime.CaptureMailboxEvents(env.Sender.Hint(), env.Sender.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(replies.front()->GetTypeRewrite(), TEvResolveError::EventType);
        UNIT_ASSERT_VALUES_EQUAL(replies.front()->Get<TEvResolveError>()->Explain, "Deadline");
    }
}

} // namespace NActors
