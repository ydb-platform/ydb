#include "blobstorage_replbroker.h"

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

TActorId StartReplBroker(TTestActorRuntime& runtime) {
    runtime.Initialize(TAppPrepare().Unwrap());
    const TActorId brokerId = runtime.Register(CreateReplBrokerActor(1_GB));
    runtime.DispatchEvents();
    return brokerId;
}

void Query(TTestActorRuntime& runtime, const TActorId& brokerId, const TActorId& clientId, ui32 pdiskId,
        TReplTokenPriority priority, bool supportsYield = true) {
    runtime.Send(brokerId, clientId, new TEvQueryReplToken(pdiskId, priority, supportsYield));
}

void Release(TTestActorRuntime& runtime, const TActorId& brokerId, const TActorId& clientId) {
    runtime.Send(brokerId, clientId, new TEvReleaseReplToken);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TReplBrokerPriority) {
    Y_UNIT_TEST(CriticalRequestPrecedesNormalWaiter) {
        TTestActorRuntime runtime;
        const TActorId brokerId = StartReplBroker(runtime);
        const TActorId holder = runtime.AllocateEdgeActor();
        const TActorId normal = runtime.AllocateEdgeActor();
        const TActorId critical = runtime.AllocateEdgeActor();

        Query(runtime, brokerId, holder, 1, 2);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(holder);
        Query(runtime, brokerId, normal, 1, 2);
        Query(runtime, brokerId, critical, 1, 1);
        runtime.GrabEdgeEventRethrow<TEvYieldReplToken>(holder);

        Release(runtime, brokerId, holder);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(critical);
        Release(runtime, brokerId, critical);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(normal);
    }

    Y_UNIT_TEST(YieldCapabilityBelongsToHolder) {
        TTestActorRuntime runtime;
        const TActorId brokerId = StartReplBroker(runtime);
        const TActorId holder = runtime.AllocateEdgeActor();
        const TActorId critical = runtime.AllocateEdgeActor();

        // SupportsYield is a capability of a client while that client holds the token. It tells the broker whether
        // TEvYieldReplToken is part of the holder's protocol. The flag of a waiting request has no bearing on whether
        // the current holder can yield: it only becomes relevant after that waiting request is granted the token.
        //
        // Consequently, a yield-capable holder must be asked to yield to a higher-priority waiter even when the waiter
        // itself does not support yielding. Using the incoming waiter's SupportsYield value to make this decision would
        // suppress the yield below and leave the critical request blocked until the holder releases the token normally.
        Query(runtime, brokerId, holder, 1, 2, true);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(holder);

        Query(runtime, brokerId, critical, 1, 1, false);
        runtime.GrabEdgeEventRethrow<TEvYieldReplToken>(holder, TDuration::Seconds(5));

        Release(runtime, brokerId, holder);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(critical);
    }

    Y_UNIT_TEST(PriorityUpdateReordersWaitingRequest) {
        TTestActorRuntime runtime;
        const TActorId brokerId = StartReplBroker(runtime);
        const TActorId holder = runtime.AllocateEdgeActor();
        const TActorId first = runtime.AllocateEdgeActor();
        const TActorId promoted = runtime.AllocateEdgeActor();

        Query(runtime, brokerId, holder, 1, 2);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(holder);
        Query(runtime, brokerId, first, 1, 2);
        Query(runtime, brokerId, promoted, 1, 3);
        runtime.Send(brokerId, promoted, new TEvUpdateReplTokenPriority(1));
        runtime.GrabEdgeEventRethrow<TEvYieldReplToken>(holder);

        Release(runtime, brokerId, holder);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(promoted);
        Release(runtime, brokerId, promoted);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(first);
    }

    Y_UNIT_TEST(EqualPriorityPreservesFifo) {
        TTestActorRuntime runtime;
        const TActorId brokerId = StartReplBroker(runtime);
        const TActorId holder = runtime.AllocateEdgeActor();
        const TActorId first = runtime.AllocateEdgeActor();
        const TActorId second = runtime.AllocateEdgeActor();

        Query(runtime, brokerId, holder, 1, 1);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(holder);
        Query(runtime, brokerId, first, 1, 1);
        Query(runtime, brokerId, second, 1, 1);

        Release(runtime, brokerId, holder);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(first);
        Release(runtime, brokerId, first);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(second);
    }

    Y_UNIT_TEST(QueuesForDifferentPDisksAreIndependent) {
        TTestActorRuntime runtime;
        const TActorId brokerId = StartReplBroker(runtime);
        const TActorId first = runtime.AllocateEdgeActor();
        const TActorId second = runtime.AllocateEdgeActor();

        Query(runtime, brokerId, first, 1, 2);
        Query(runtime, brokerId, second, 2, 2);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(first);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(second);
    }

    Y_UNIT_TEST(LegacyHolderIsNotAskedToYield) {
        TTestActorRuntime runtime;
        const TActorId brokerId = StartReplBroker(runtime);
        const TActorId legacyHolder = runtime.AllocateEdgeActor();
        const TActorId critical = runtime.AllocateEdgeActor();
        ui32 yieldRequests = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvYieldReplToken
                    && ev->GetRecipientRewrite() == legacyHolder) {
                ++yieldRequests;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Query(runtime, brokerId, legacyHolder, 1, DefaultReplTokenPriority, false);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(legacyHolder);
        Query(runtime, brokerId, critical, 1, 1);

        Release(runtime, brokerId, legacyHolder);
        runtime.GrabEdgeEventRethrow<TEvReplToken>(critical);
        UNIT_ASSERT_VALUES_EQUAL(yieldRequests, 0);
    }
}

} // namespace NKikimr
