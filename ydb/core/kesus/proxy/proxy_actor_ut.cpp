#include "ut_helpers.h"
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>

namespace NKikimr {
namespace NKesus {

Y_UNIT_TEST_SUITE(TProxyActorTest) {
    Y_UNIT_TEST(TestCreateSemaphore) {
        TTestContext ctx;
        ctx.Setup();
        ctx.CreateSemaphore(42, "Sem1", 1);
        ctx.CreateSemaphore(42, "Sem2", 2, "\x81\x82\x83\x84");
        ctx.VerifySemaphoreParams(42, "Sem1", 1);
        ctx.VerifySemaphoreParams(42, "Sem2", 2, "\x81\x82\x83\x84");
    }

    Y_UNIT_TEST(TestCreateSemaphoreInterrupted) {
        TTestContext ctx;
        ctx.Setup();
        ctx.CreateSemaphore(42, "Sem1", 1);

        ui64 cookie = RandomNumber<ui64>();
        auto edge = ctx.Runtime->AllocateEdgeActor(1);
        ctx.SendFromEdge(42, edge, new TEvKesus::TEvCreateSemaphore("", "Sem2", 2), cookie);
        ctx.RebootTablet();
        ctx.ExpectProxyError(edge, cookie, Ydb::StatusIds::UNAVAILABLE);
    }

    Y_UNIT_TEST(TestAttachSession) {
        TTestContext ctx;
        ctx.Setup();

        // Create session 1
        auto req1 = ctx.Runtime->AllocateEdgeActor(1);
        auto req1cookie = ctx.SendAttachSession(42, req1, 0, 0, "", 111);
        UNIT_ASSERT_VALUES_EQUAL(ctx.ExpectAttachSessionResult(req1, req1cookie), 1);

        // Reattach session 1 from a different request actor
        auto req2 = ctx.Runtime->AllocateEdgeActor(1);
        auto req2cookie = ctx.SendAttachSession(42, req2, 1, 0, "", 222);
        UNIT_ASSERT_VALUES_EQUAL(ctx.ExpectAttachSessionResult(req2, req2cookie), 1);
        ctx.ExpectProxyError(req1, req1cookie, Ydb::StatusIds::BAD_SESSION);

        // Reattach session 1 from a different proxy
        auto req3 = ctx.Runtime->AllocateEdgeActor(1);
        auto req3cookie = ctx.SendAttachSession(43, req3, 1, 0, "", 333);
        UNIT_ASSERT_VALUES_EQUAL(ctx.ExpectAttachSessionResult(req3, req3cookie), 1);
        ctx.ExpectProxyError(req2, req2cookie, Ydb::StatusIds::BAD_SESSION);

        // Test out of sequence on the owner proxy
        auto req4 = ctx.Runtime->AllocateEdgeActor(1);
        auto req4cookie = ctx.SendAttachSession(43, req4, 1, 0, "", 42);
        ctx.ExpectAttachSessionResult(req4, req4cookie, Ydb::StatusIds::BAD_SESSION);

        // Test out of sequence on the non-owner proxy
        auto req5 = ctx.Runtime->AllocateEdgeActor(1);
        auto req5cookie = ctx.SendAttachSession(42, req5, 1, 0, "", 42);
        ctx.ExpectAttachSessionResult(req5, req5cookie, Ydb::StatusIds::BAD_SESSION);
    }

    Y_UNIT_TEST(TestDisconnectWhileAttaching) {
        TTestContext ctx;
        ctx.Setup();

        auto& runtime = *ctx.Runtime;

        TBlockEvents<TEvKesus::TEvRegisterProxy> blockedRegister(runtime);

        auto req1 = runtime.AllocateEdgeActor(1);
        auto req1cookie = ctx.SendAttachSession(42, req1, 0, 0, "", 111);

        runtime.WaitFor("blocked registrations", [&]{ return blockedRegister.size() >= 1; });

        // drop link between 2 nodes
        {
            runtime.Send(
                new IEventHandle(
                    runtime.GetInterconnectProxy(0, 1),
                    TActorId(),
                    new TEvInterconnect::TEvDisconnect()),
                0, true);
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
            runtime.DispatchEvents(options);
        }

        ctx.ExpectProxyError(req1, req1cookie, Ydb::StatusIds::UNAVAILABLE);

        // Proxy must not crash due to unexpected registration after a disconnect
        blockedRegister.Stop().Unblock();
        runtime.SimulateSleep(TDuration::Seconds(1));
    }
}

}
}
