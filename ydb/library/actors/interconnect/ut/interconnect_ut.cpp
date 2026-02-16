#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/rdma/ut/utils/utils.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/digest/md5/md5.h>
#include <util/random/fast.h>
#include <util/string/vector.h>

using namespace NActors;

class TSenderActor : public TActorBootstrapped<TSenderActor> {
    const TActorId Recipient;
    const size_t SendLimit;
    using TSessionToCookie = std::unordered_multimap<TActorId, ui64, THash<TActorId>>;
    TSessionToCookie SessionToCookie;
    std::unordered_map<ui64, std::pair<TSessionToCookie::iterator, TString>> InFlight;
    std::unordered_map<ui64, TString> Tentative;
    ui64 NextCookie = 0;
    TActorId SessionId;
    bool SubscribeInFlight = false;

public:
    TSenderActor(TActorId recipient, size_t sendLimit = -1)
        : Recipient(recipient)
        , SendLimit(sendLimit)
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
        Subscribe();
    }

    void Subscribe() {
        Cerr << (TStringBuilder() << "Subscribe" << Endl);
        Y_ABORT_UNLESS(!SubscribeInFlight);
        SubscribeInFlight = true;
        Send(TActivationContext::InterconnectProxy(Recipient.NodeId()), new TEvents::TEvSubscribe);
    }

    void IssueQueries() {
        if (!SessionId) {
            return;
        }
        while (InFlight.size() < 10 && NextCookie < SendLimit) {
            size_t len = RandomNumber<size_t>(65536) + 1;
            TString data = TString::Uninitialized(len);
            TReallyFastRng32 rng(RandomNumber<ui32>());
            char *p = data.Detach();
            for (size_t i = 0; i < len; ++i) {
                p[i] = rng();
            }
            const TSessionToCookie::iterator s2cIt = SessionToCookie.emplace(SessionId, NextCookie);
            InFlight.emplace(NextCookie, std::make_tuple(s2cIt, MD5::CalcRaw(data)));
            TActivationContext::Send(new IEventHandle(TEvents::THelloWorld::Ping, IEventHandle::FlagTrackDelivery, Recipient,
                SelfId(), MakeIntrusive<TEventSerializedData>(std::move(data), TEventSerializationInfo{}), NextCookie));
//            Cerr << (TStringBuilder() << "Send# " << NextCookie << Endl);
            ++NextCookie;
        }
    }

    void HandlePong(TAutoPtr<IEventHandle> ev) {
//        Cerr << (TStringBuilder() << "Receive# " << ev->Cookie << Endl);
        if (const auto it = InFlight.find(ev->Cookie); it != InFlight.end()) {
            auto& [s2cIt, hash] = it->second;
            Y_ABORT_UNLESS(hash == ev->GetChainBuffer()->GetString());
            SessionToCookie.erase(s2cIt);
            InFlight.erase(it);
        } else if (const auto it = Tentative.find(ev->Cookie); it != Tentative.end()) {
            Y_ABORT_UNLESS(it->second == ev->GetChainBuffer()->GetString());
            Tentative.erase(it);
        } else {
            Y_ABORT("Cookie# %" PRIu64, ev->Cookie);
        }
        IssueQueries();
    }

    void Handle(TEvInterconnect::TEvNodeConnected::TPtr ev) {
        Cerr << (TStringBuilder() << "TEvNodeConnected" << Endl);
        Y_ABORT_UNLESS(SubscribeInFlight);
        SubscribeInFlight = false;
        Y_ABORT_UNLESS(!SessionId);
        SessionId = ev->Sender;
        IssueQueries();
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
        Cerr << (TStringBuilder() << "TEvNodeDisconnected" << Endl);
        SubscribeInFlight = false;
        if (SessionId) {
            Y_ABORT_UNLESS(SessionId == ev->Sender);
            auto r = SessionToCookie.equal_range(SessionId);
            for (auto it = r.first; it != r.second; ++it) {
                const auto inFlightIt = InFlight.find(it->second);
                Y_ABORT_UNLESS(inFlightIt != InFlight.end());
                Tentative.emplace(inFlightIt->first, inFlightIt->second.second);
                InFlight.erase(it->second);
            }
            SessionToCookie.erase(r.first, r.second);
            SessionId = TActorId();
        }
        Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
    }

    void Handle(TEvents::TEvUndelivered::TPtr ev) {
        Cerr << (TStringBuilder() << "TEvUndelivered Cookie# " << ev->Cookie << Endl);
        if (const auto it = InFlight.find(ev->Cookie); it != InFlight.end()) {
            auto& [s2cIt, hash] = it->second;
            Tentative.emplace(it->first, hash);
            SessionToCookie.erase(s2cIt);
            InFlight.erase(it);
            IssueQueries();
        }
    }

    STRICT_STFUNC(StateFunc,
        fFunc(TEvents::THelloWorld::Pong, HandlePong);
        hFunc(TEvInterconnect::TEvNodeConnected, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        cFunc(TEvents::TSystem::Wakeup, Subscribe);
    )
};

class TRecipientActor : public TActor<TRecipientActor> {
public:
    TRecipientActor()
        : TActor(&TThis::StateFunc)
        , Received(0)
    {}

    void HandlePing(TAutoPtr<IEventHandle>& ev) {
        const TString& data = ev->GetChainBuffer()->GetString();
        const TString& response = MD5::CalcRaw(data);
        TActivationContext::Send(new IEventHandle(TEvents::THelloWorld::Pong, 0, ev->Sender, SelfId(),
            MakeIntrusive<TEventSerializedData>(response, TEventSerializationInfo{}), ev->Cookie));
        Received.fetch_add(1, std::memory_order_relaxed);
    }

    size_t GetReceived() const noexcept {
        return Received.load(std::memory_order_relaxed);
    }

    STRICT_STFUNC(StateFunc,
        fFunc(TEvents::THelloWorld::Ping, HandlePing);
    )
private:
    std::atomic<size_t> Received;
};

Y_UNIT_TEST_SUITE(Interconnect) {

    Y_UNIT_TEST(SessionContinuation) {
        TTestICCluster cluster(2);
        const TActorId recipient = cluster.RegisterActor(new TRecipientActor, 1);
        cluster.RegisterActor(new TSenderActor(recipient), 2);
        for (ui32 i = 0; i < 100; ++i) {
            const ui32 nodeId = 1 + RandomNumber(2u);
            const ui32 peerNodeId = 3 - nodeId;
            const ui32 action = RandomNumber(3u);
            auto *node = cluster.GetNode(nodeId);
            TActorId proxyId = node->InterconnectProxy(peerNodeId);

            switch (action) {
                case 0:
                    node->Send(proxyId, new TEvInterconnect::TEvClosePeerSocket);
                    Cerr << (TStringBuilder() << "nodeId# " << nodeId << " peerNodeId# " << peerNodeId
                        << " TEvClosePeerSocket" << Endl);
                    break;

                case 1:
                    node->Send(proxyId, new TEvInterconnect::TEvCloseInputSession);
                    Cerr << (TStringBuilder() << "nodeId# " << nodeId << " peerNodeId# " << peerNodeId
                        << " TEvCloseInputSession" << Endl);
                    break;

                case 2:
                    node->Send(proxyId, new TEvInterconnect::TEvPoisonSession);
                    Cerr << (TStringBuilder() << "nodeId# " << nodeId << " peerNodeId# " << peerNodeId
                        << " TEvPoisonSession" << Endl);
                    break;

                default:
                    Y_ABORT();
            }

            Sleep(TDuration::MilliSeconds(RandomNumber<ui32>(500) + 100));
        }
    }

    Y_UNIT_TEST(SetupRdmaSession) {
        if (NRdmaTest::IsRdmaTestDisabled()) {
            Cerr << "SetupRdmaSession test skipped" << Endl;
            return;
        }
        TTestICCluster cluster(2);
        const size_t limit = 10;
        auto receiverPtr = new TRecipientActor;
        const TActorId recipient = cluster.RegisterActor(receiverPtr, 1);
        auto senderPtr = new TSenderActor(recipient, limit);
        cluster.RegisterActor(senderPtr, 2);

        while (receiverPtr->GetReceived() < limit) {
            Sleep(TDuration::MilliSeconds(100));
        }

        {
            auto s = GetRdmaQpStatus(cluster, 1, 2);
            auto tokens = SplitString(s, ",");
            UNIT_ASSERT(tokens.size() > 2);
            UNIT_ASSERT(tokens[1] == "QPS_RTS");
        }

        {
            auto s = GetRdmaChecksumStatus(cluster, 2, 1);
            UNIT_ASSERT_VALUES_EQUAL(s, "On | SoftwareChecksum");
        }
    }
}
