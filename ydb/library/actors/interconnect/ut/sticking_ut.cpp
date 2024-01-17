#include <ydb/library/actors/interconnect/ut/lib/node.h>
#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

struct TEvPing : TEventBase<TEvPing, TEvents::THelloWorld::Ping> {
    TString Data;

    TEvPing(TString data)
        : Data(data)
    {}

    TEvPing() = default;

    ui32 CalculateSerializedSize() const override { return Data.size(); }
    bool IsSerializable() const override { return true; }
    bool SerializeToArcadiaStream(TChunkSerializer *serializer) const override { serializer->WriteAliasedRaw(Data.data(), Data.size()); return true; }
    TString ToStringHeader() const override { return {}; }
};

class TPonger : public TActor<TPonger> {
public:
    TPonger()
        : TActor(&TThis::StateFunc)
    {}

    void Handle(TEvPing::TPtr ev) {
        Send(ev->Sender, new TEvents::TEvPong(), 0, ev->Cookie);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPing, Handle);
    )
};

class TPinger : public TActorBootstrapped<TPinger> {
    ui32 PingInFlight = 0;
    TActorId PongerId;
    TDuration MaxRTT;

public:
    TPinger(TActorId pongerId)
        : PongerId(pongerId)
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
        Action();
    }

    void Action() {
        if (PingInFlight) {
            return;
        }
        const ui32 max = 1 + RandomNumber(10u);
        while (PingInFlight < max) {
            IssuePing();
        }
    }

    void IssuePing() {
        TString data = TString::Uninitialized(RandomNumber<size_t>(256 * 1024) + 1);
        memset(data.Detach(), 0, data.size());
        Send(PongerId, new TEvPing(data), 0, GetCycleCountFast());
        ++PingInFlight;
    }

    void Handle(TEvents::TEvPong::TPtr ev) {
        const TDuration rtt = CyclesToDuration(GetCycleCountFast() - ev->Cookie);
        if (MaxRTT < rtt) {
            MaxRTT = rtt;
            Cerr << "Updated MaxRTT# " << MaxRTT << Endl;
            Y_ABORT_UNLESS(MaxRTT <= TDuration::MilliSeconds(500));
        }
        --PingInFlight;
        Action();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvents::TEvPong, Handle);
    )
};

Y_UNIT_TEST_SUITE(Sticking) {
    Y_UNIT_TEST(Check) {
        TPortManager portman;
        THashMap<ui32, ui16> nodeToPort;
        nodeToPort.emplace(1, portman.GetPort());
        nodeToPort.emplace(2, portman.GetPort());

        NMonitoring::TDynamicCounterPtr counters = new NMonitoring::TDynamicCounters;
        std::list<TNode> nodes;
        for (auto [nodeId, _] : nodeToPort) {
            nodes.emplace_back(nodeId, nodeToPort.size(), nodeToPort, "127.1.0.0",
                counters->GetSubgroup("nodeId", TStringBuilder() << nodeId), TDuration::Seconds(10),
                TChannelsConfig(), 0, 1, nullptr, 40 << 20);
        }

        auto& node1 = *nodes.begin();
        auto& node2 = *++nodes.begin();

        const TActorId ponger = node2.RegisterActor(new TPonger());
        node1.RegisterActor(new TPinger(ponger));

        Sleep(TDuration::Seconds(10));
    }
}
