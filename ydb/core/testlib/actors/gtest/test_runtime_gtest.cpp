#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/gtest/gtest.h>

// TTestActorRuntime must stay usable from gtest binaries: they cannot link
// library/cpp/testing/unittest, because both test frameworks PROVIDES(test_framework).

namespace NKikimr {
namespace {

    using namespace NActors;

    struct TEvPing: TEventLocal<TEvPing, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    };

    struct TEvPong: TEventLocal<TEvPong, EventSpaceBegin(TEvents::ES_PRIVATE) + 1> {
        explicit TEvPong(ui64 value)
            : Value(value)
        {
        }

        const ui64 Value;
    };

    class TEchoActor: public TActorBootstrapped<TEchoActor> {
    public:
        void Bootstrap() {
            Become(&TThis::StateWork);
        }

        void Handle(TEvPing::TPtr& ev) {
            Send(ev->Sender, new TEvPong(ev->Cookie), 0, ev->Cookie);
        }

        STRICT_STFUNC(StateWork,
            hFunc(TEvPing, Handle);
        )
    };

    TEST(TTestActorRuntimeGTest, DispatchesEvents) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        app.ClearDomainsAndHive();
        runtime.Initialize(app.Unwrap());

        const TActorId edge = runtime.AllocateEdgeActor();
        const TActorId echo = runtime.Register(new TEchoActor);

        constexpr ui64 cookie = 42;
        // viaActorSystem, so that the event queues behind the Bootstrap one
        runtime.Send(new IEventHandle(echo, edge, new TEvPing, 0, cookie), 0, true);

        auto pong = runtime.GrabEdgeEvent<TEvPong>(edge);
        ASSERT_TRUE(pong);
        ASSERT_EQ(cookie, pong->Get()->Value);
    }

    TEST(TTestActorRuntimeGTest, ProvidesPortManager) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        app.ClearDomainsAndHive();
        runtime.Initialize(app.Unwrap());

        NTesting::TPortManager& portManager = runtime.GetPortManager();
        const ui16 port = portManager.GetPort();
        ASSERT_NE(0u, port);
        ASSERT_NE(port, portManager.GetPort());
    }

} // anonymous namespace
} // namespace NKikimr
