#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NActors {
namespace {

class TDynamicNameserverTestEnv {
public:
    static constexpr ui32 NodeId = 42;

    TDynamicNameserverTestEnv() {
        Runtime.Initialize();
        Nameserver = Runtime.Register(CreateDynamicNameserver(
            MakeIntrusive<TTableNameserverSetup>(), TDuration::Seconds(3)));
        Sender = Runtime.AllocateEdgeActor();
        Runtime.SetScheduledEventFilter([this](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event,
                TDuration, TInstant&) {
            return event->GetRecipientRewrite() != Nameserver;
        });
    }

    void Resolve(TDuration timeout) {
        Runtime.Send(new IEventHandle(Nameserver, Sender,
            new TEvInterconnect::TEvResolveNode(NodeId, Runtime.GetCurrentMonotonicTime() + timeout)));
    }

    void AnnounceNode() {
        auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
        nodes->emplace_back(NodeId, "127.0.0.42", "", "", 19001, TNodeLocation());
        Runtime.Send(new IEventHandle(Nameserver, Sender, new TEvInterconnect::TEvNodesInfo(nodes)));
    }

    void CheckTimeout(TDuration requestTimeout, TDuration expectedTimeout) {
        const auto deadline = Runtime.GetCurrentMonotonicTime() + expectedTimeout;
        Resolve(requestTimeout);

        const auto reply = Runtime.GrabEdgeEvent<TEvLocalNodeInfo>(Sender, TDuration::Seconds(4));
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->NodeId, NodeId);
        UNIT_ASSERT(reply->Get()->Addresses.empty());
        UNIT_ASSERT_VALUES_EQUAL(Runtime.GetCurrentMonotonicTime(), deadline);

        // A later discovery must not produce a second reply to the timed-out request.
        AnnounceNode();
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
        env.CheckTimeout(TDuration::Seconds(1), TDuration::Seconds(1));
    }

    Y_UNIT_TEST(ExpiresAtPendingPeriod) {
        TDynamicNameserverTestEnv env;
        env.CheckTimeout(TDuration::Seconds(5), TDuration::Seconds(3));
    }
}

} // namespace NActors
