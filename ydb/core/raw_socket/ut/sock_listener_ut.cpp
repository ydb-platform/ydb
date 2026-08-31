#include <library/cpp/testing/unittest/registar.h>
#include <util/network/sock.h>
#include <ydb/core/raw_socket/sock_listener.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/services/services.pb.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NRawSocket;

namespace {

class TDummyConnectionActor: public TActor<TDummyConnectionActor> {
public:
    TDummyConnectionActor()
        : TActor(&TDummyConnectionActor::StateWork)
    {
    }

    STRICT_STFUNC(StateWork,
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    )
};

} // namespace

Y_UNIT_TEST_SUITE(TSocketListener) {
    Y_UNIT_TEST(RetryBindUntilPortIsFree) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());

        TInet6StreamSocket occupying;
        occupying.CheckSock();
        TSockAddrInet6 addr("::", 0);
        UNIT_ASSERT_VALUES_EQUAL(occupying.Bind(&addr), 0);
        UNIT_ASSERT_VALUES_EQUAL(occupying.Listen(10), 0);
        const ui16 port = addr.GetPort();
        UNIT_ASSERT(port != 0);

        const TActorId pollerId = runtime.Register(CreatePollerActor());
        TListenerSettings settings;
        settings.Port = port;
        settings.Address = "::";

        const TActorId listenerId = runtime.Register(CreateSocketListener(
            pollerId,
            settings,
            [](const TActorId&, TIntrusivePtr<TSocketDescriptor>, TNetworkConfig::TSocketAddressType) -> IActor* {
                return new TDummyConnectionActor();
            },
            NKikimrServices::KAFKA_PROXY));
        runtime.EnableScheduleForActor(listenerId, true);

        // Bootstrap fails to bind and schedules a retry. Sleep delivers that retry while the port is still busy.
        runtime.SimulateSleep(TDuration::Seconds(1));

        occupying.Close();

        runtime.SimulateSleep(TDuration::Seconds(1));

        TInet6StreamSocket probe;
        probe.CheckSock();
        TSockAddrInet6 probeAddr("::", port);
        UNIT_ASSERT_C(probe.Bind(&probeAddr) != 0,
            "listener should own the port after the occupying socket is closed");
    }
}
