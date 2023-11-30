#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(ActorBootstrapped) {
    class TTestBoostrapped: public TActorBootstrapped<TTestBoostrapped>
    {
    public:
        TTestBoostrapped(const TActorId& edge)
            : Edge(edge)
        {}

        void Bootstrap(const TActorContext& ctx) {
            ctx.Send(Edge, new TEvents::TEvWakeup);
        }

        TActorId Edge;
    };

    class TTestBoostrappedParent: public TActorBootstrapped<TTestBoostrappedParent>
    {
    public:
        TTestBoostrappedParent(const TActorId& edge)
            : Edge(edge)
        {}

        void Bootstrap(const TActorId&, const TActorContext& ctx) {
            ctx.Send(Edge, new TEvents::TEvWakeup);
        }

        TActorId Edge;
    };

    template <typename TDerivedActor>
    void TestBootrappedActor() {
        TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
        const auto edge = runtime.AllocateEdgeActor(0);
        auto actor = new TDerivedActor(edge);
        runtime.Register(actor);
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);
    }


    Y_UNIT_TEST(TestBootstrapped) {
        TestBootrappedActor<TTestBoostrapped>();
    }

    Y_UNIT_TEST(TestBootstrappedParent) {
        TestBootrappedActor<TTestBoostrappedParent>();
    }
}
