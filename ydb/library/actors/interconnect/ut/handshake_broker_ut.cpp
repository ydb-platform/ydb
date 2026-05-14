#include <ydb/library/actors/interconnect/handshake_broker.h>
#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(HandshakeBroker) {

    // Regression test for the over-credit bug in THandshakeBroker.
    //
    // When a waiter (a Take that was queued because Capacity == 0) is
    // canceled by sending Free before it ever received Permit, the broker
    // still calls PermitNext(). With an empty Waiters queue, PermitNext
    // falls into its `else { Capacity += 1; }` branch and credits a slot
    // that was never actually consumed — letting a subsequent Take be
    // granted while all original leases are still outstanding.
    //
    // With inflightLimit = 2 the bug allows 3 simultaneous permits.
    Y_UNIT_TEST(CapacityIsNotOverCreditedOnWaiterCancel) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();

        const ui32 inflightLimit = 2;
        const TActorId brokerId = runtime.Register(CreateHandshakeBroker(inflightLimit));

        const TActorId a = runtime.AllocateEdgeActor();
        const TActorId b = runtime.AllocateEdgeActor();
        const TActorId c = runtime.AllocateEdgeActor();
        const TActorId d = runtime.AllocateEdgeActor();

        // A and B take both available slots. Capacity becomes 0.
        runtime.Send(new IEventHandle(brokerId, a, new TEvHandshakeBrokerTake()));
        runtime.Send(new IEventHandle(brokerId, b, new TEvHandshakeBrokerTake()));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvHandshakeBrokerPermit>(a, TDuration::Seconds(5)));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvHandshakeBrokerPermit>(b, TDuration::Seconds(5)));

        // C asks for a slot — none available, C is queued in Waiters.
        runtime.Send(new IEventHandle(brokerId, c, new TEvHandshakeBrokerTake()));

        // C is canceled before it ever got a Permit. No slot was actually
        // consumed by C, so this Free must not change Capacity.
        runtime.Send(new IEventHandle(brokerId, c, new TEvHandshakeBrokerFree()));

        // D asks for a slot. A and B still hold their leases, so D must
        // wait — it must NOT receive a Permit.
        runtime.Send(new IEventHandle(brokerId, d, new TEvHandshakeBrokerTake()));

        bool dGotPermit = false;
        try {
            auto permit = runtime.GrabEdgeEvent<TEvHandshakeBrokerPermit>(d, TDuration::Seconds(1));
            if (permit) {
                dGotPermit = true;
            }
        } catch (const TEmptyEventQueueException&) {
            // expected: no Permit is on the way to D
        }
        UNIT_ASSERT_C(!dGotPermit,
            "D received a Permit while A and B still hold leases — broker over-credited "
            "Capacity after a queued waiter was canceled");

        // Sanity check: once A releases, D should then be permitted.
        runtime.Send(new IEventHandle(brokerId, a, new TEvHandshakeBrokerFree()));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvHandshakeBrokerPermit>(d, TDuration::Seconds(5)));
    }
}
