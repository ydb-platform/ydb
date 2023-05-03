#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/actors/core/executor_pool_basic.h>
#include <library/cpp/actors/core/scheduler_basic.h>
#include <library/cpp/actors/testlib/test_runtime.h>

#include <ydb/core/pgproxy/pg_proxy.h>
#include <ydb/core/pgproxy/pg_listener.h>
#include <ydb/core/pgproxy/pg_log.h>
#include <ydb/core/pgproxy/pg_proxy_events.h>
#include <ydb/core/protos/services.pb.h>

#include <util/network/socket.h>
#include <util/string/hex.h>

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

class TTestActorRuntime : public NActors::TTestActorRuntimeBase {
public:
    void InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) override {
        NActors::TTestActorRuntimeBase::InitNodeImpl(node, nodeIndex);
        node->LogSettings->Append(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name
        );
    }
};

size_t Send(TSocket& s, const TString& data) {
    char buf[1024];
    HexDecode(data.data(), data.size(), buf);
    return s.Send(buf, data.size() / 2);
}

TString Receive(TSocket& s) {
    char buf[1024];
    size_t received = s.Recv(buf, sizeof(buf));
    return HexEncode(buf, received);
}

Y_UNIT_TEST_SUITE(TPGTest) {
    Y_UNIT_TEST(TestLogin) {
        TTestActorRuntime actorSystem;
        TPortManager portManager;
        TIpPort port = portManager.GetTcpPort();
        TAutoPtr<NActors::IEventHandle> handle;
        actorSystem.Initialize();
        actorSystem.SetLogPriority(NKikimrServices::PGWIRE, NActors::NLog::PRI_DEBUG);
        NActors::TActorId database = actorSystem.AllocateEdgeActor();
        NActors::TActorId poller = actorSystem.Register(NActors::CreatePollerActor());
        NActors::IActor* listener = NPG::CreatePGListener(poller, database, {
            .Port = port,
        });
        actorSystem.Register(listener);
        // waiting for port become listening
        {
            NActors::TDispatchOptions options;
            options.FinalEvents.push_back(NActors::TDispatchOptions::TFinalEventCondition(ui32(NActors::ENetwork::EvPollerRegister)));
            actorSystem.DispatchEvents(options);
        }
        TSocket s(TNetworkAddress("::", port));
        Send(s, "0000001300030000" "7573657200757365720000");  // user=user
        NPG::TEvPGEvents::TEvAuth* authRequest = actorSystem.GrabEdgeEvent<NPG::TEvPGEvents::TEvAuth>(handle);
        UNIT_ASSERT(authRequest);
        UNIT_ASSERT_VALUES_EQUAL(authRequest->InitialMessage->GetClientParams()["user"], "user");
        actorSystem.Send(new NActors::IEventHandle(handle->Sender, database, new NPG::TEvPGEvents::TEvAuthResponse()));
        TString received = Receive(s);
        UNIT_ASSERT_VALUES_EQUAL(received, "520000000800000000530000001B496E74657276616C5374796C6500706F737467726573005300000012446174655374796C650049534F0053000000197365727665725F656E636F64696E670055544638005300000019696E74656765725F6461746574696D6573006F6E005300000019636C69656E745F656E636F64696E670055544638005A0000000549");
    }

}
