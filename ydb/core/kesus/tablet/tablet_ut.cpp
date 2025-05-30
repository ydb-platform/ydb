#include "tablet.h"
#include "events.h"
#include "ut_helpers.h"
#include "rate_accounting.h"

#include <ydb/core/metering/metering.h>

#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>

#include <util/random/random.h>

#include <limits>

namespace NKikimr {
namespace NKesus {

namespace {
    struct TDyingActor : public TActorBootstrapped<TDyingActor> {
        void Bootstrap(const TActorContext& ctx) {
            Die(ctx);
        }
    };

    Ydb::Coordination::Config MakeConfig(const TString& kesusPath) {
        Ydb::Coordination::Config config;
        config.set_path(kesusPath);
        return config;
    }

    void EnableRelaxedAttach(TTestContext& ctx) {
        Ydb::Coordination::Config config;
        config.set_attach_consistency_mode(Ydb::Coordination::CONSISTENCY_MODE_RELAXED);
        ctx.SetConfig(12345, config, 42);
    }
}

Y_UNIT_TEST_SUITE(TKesusTest) {
    Y_UNIT_TEST(TestKesusConfig) {
        TTestContext ctx;
        ctx.Setup();

        ctx.SetConfig(12345, MakeConfig("/foo/bar/baz"), 42);

        {
            const auto getConfigResult = ctx.GetConfig();
            UNIT_ASSERT_VALUES_EQUAL(getConfigResult.GetConfig().path(), "/foo/bar/baz");
            UNIT_ASSERT_VALUES_EQUAL(getConfigResult.GetVersion(), 42);
            UNIT_ASSERT_EQUAL_C(getConfigResult.GetConfig().rate_limiter_counters_mode(), Ydb::Coordination::RATE_LIMITER_COUNTERS_MODE_AGGREGATED, "Record: " << getConfigResult);
        }

        {

            auto configWithModes = MakeConfig("/foo/bar/baz");
            configWithModes.set_rate_limiter_counters_mode(Ydb::Coordination::RATE_LIMITER_COUNTERS_MODE_DETAILED);
            ctx.SetConfig(12345, configWithModes, 42);
        }

        {
            const auto getConfigResult = ctx.GetConfig();
            UNIT_ASSERT_VALUES_EQUAL(getConfigResult.GetConfig().path(), "/foo/bar/baz");
            UNIT_ASSERT_VALUES_EQUAL(getConfigResult.GetVersion(), 42);
            UNIT_ASSERT_EQUAL_C(getConfigResult.GetConfig().rate_limiter_counters_mode(), Ydb::Coordination::RATE_LIMITER_COUNTERS_MODE_DETAILED, "Record: " << getConfigResult);
        }

        // Verify it is restored after reboot
        ctx.RebootTablet();

        {
            const auto getConfigResult = ctx.GetConfig();
            UNIT_ASSERT_VALUES_EQUAL(getConfigResult.GetConfig().path(), "/foo/bar/baz");
            UNIT_ASSERT_VALUES_EQUAL(getConfigResult.GetVersion(), 42);
            UNIT_ASSERT_EQUAL_C(getConfigResult.GetConfig().rate_limiter_counters_mode(), Ydb::Coordination::RATE_LIMITER_COUNTERS_MODE_DETAILED, "Record: " << getConfigResult);
        }

        // Verify it is ok to repeat the event
        ctx.SetConfig(12345, MakeConfig("/foo/bar/baz"), 42);

        // Verify it is not ok to downgrade
        ctx.SetConfig(12345, MakeConfig("/foo/bar/baz"), 41, Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(TestRegisterProxy) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.VerifyProxyRegistered(proxy, 1);
    }

    Y_UNIT_TEST(TestRegisterProxyBadGeneration) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        // cannot use generation 0
        ctx.MustRegisterProxy(proxy, 0, Ydb::StatusIds::BAD_REQUEST);
        ctx.MustRegisterProxy(proxy, 2);
        // cannot reuse the same generation
        ctx.MustRegisterProxy(proxy, 2, Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(TestRegisterProxyFromDeadActor) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->Register(new TDyingActor());
        ctx.SendFromProxy(proxy, 1, new TEvKesus::TEvRegisterProxy("", 1));
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back((ui32)TEvents::TEvUndelivered::EventType);
            ctx.Runtime->DispatchEvents(options);
        }
        ctx.VerifyProxyNotRegistered(proxy);
    }

    Y_UNIT_TEST(TestRegisterProxyLinkFailure) {
        TTestContext ctx;
        ctx.Setup(2);
        // register proxy from the second node
        auto proxy = ctx.Runtime->AllocateEdgeActor(1);
        ctx.MustRegisterProxy(proxy, 1);
        ctx.VerifyProxyRegistered(proxy, 1);
        // drop link between 2 nodes
        {
            ctx.Runtime->Send(
                new IEventHandle(
                    ctx.Runtime->GetInterconnectProxy(0, 1),
                    TActorId(),
                    new TEvInterconnect::TEvDisconnect()),
                0, true);
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
            ctx.Runtime->DispatchEvents(options);
        }
        ctx.VerifyProxyNotRegistered(proxy);
    }

    Y_UNIT_TEST(TestRegisterProxyLinkFailureRace) {
        TTestContext ctx;
        ctx.Setup(2);

        // block registration requests
        TBlockEvents<TEvKesus::TEvRegisterProxy> blockedRegister(*ctx.Runtime);

        // start registering proxy from the second node
        auto proxy = ctx.Runtime->AllocateEdgeActor(1);
        ctx.SendRegisterProxy(proxy, 1);

        ctx.Runtime->WaitFor("register request", [&]{ return blockedRegister.size() >= 1; });

        // drop link between 2 nodes and unblock the request
        {
            ctx.Runtime->Send(
                new IEventHandle(
                    ctx.Runtime->GetInterconnectProxy(0, 1),
                    TActorId(),
                    new TEvInterconnect::TEvDisconnect()),
                0, true);
            blockedRegister.Stop().Unblock();
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
            ctx.Runtime->DispatchEvents(options);
        }

        // Verify proxy is not registered
        ctx.Runtime->SimulateSleep(TDuration::Seconds(1));
        ctx.VerifyProxyNotRegistered(proxy);
    }

    Y_UNIT_TEST(TestUnregisterProxy) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustUnregisterProxy(proxy, 1);
        ctx.VerifyProxyNotRegistered(proxy);
    }

    Y_UNIT_TEST(TestUnregisterProxyBadGeneration) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustRegisterProxy(proxy, 2);
        ctx.MustUnregisterProxy(proxy, 1, Ydb::StatusIds::BAD_SESSION);
    }

    Y_UNIT_TEST(TestAttachNewSessions) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        UNIT_ASSERT_VALUES_EQUAL(ctx.MustAttachSession(proxy, 1, 0), 1);
        UNIT_ASSERT_VALUES_EQUAL(ctx.MustAttachSession(proxy, 1, 0), 2);
        ctx.VerifyProxyHasSessions(proxy, 1, {1, 2});
    }

    Y_UNIT_TEST(TestAttachMissingSession) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        auto result = ctx.AttachSession(proxy, 1, 1);
        UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetStatus(), Ydb::StatusIds::SESSION_EXPIRED);
    }

    Y_UNIT_TEST(TestAttachOldGeneration) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.RegisterProxy(proxy, 2); // register with generation 2
        auto result1 = ctx.AttachSession(proxy, 1, 0); // attach with generation 1
        UNIT_ASSERT_VALUES_EQUAL(result1.GetError().GetStatus(), Ydb::StatusIds::BAD_SESSION);
        ui64 sessionId = ctx.MustAttachSession(proxy, 2, 0); // retry with generation 2
        UNIT_ASSERT_VALUES_EQUAL(sessionId, 1);
        ctx.VerifyProxyHasSessions(proxy, 2, {1});
    }

    Y_UNIT_TEST(TestAttachOutOfSequence) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.RegisterProxy(proxy1, 1);
        ctx.RegisterProxy(proxy2, 1);
        ctx.MustAttachSession(proxy1, 1, 0, 0, "", 222);
        auto result = ctx.AttachSession(proxy2, 1, 1, 0, "", 111);
        UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetStatus(), Ydb::StatusIds::BAD_SESSION);

        // verify proxy2 did not steal the session
        ctx.VerifyProxyHasSessions(proxy1, 1, {1});
    }

    Y_UNIT_TEST(TestAttachOutOfSequenceInTx) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.RegisterProxy(proxy, 1);
        ctx.SendAttachSession(111, proxy, 1, 0, 0, "", 42);
        ctx.SendAttachSession(222, proxy, 1, 1, 0, "", 41);
        ctx.ExpectAttachSessionResult(111, proxy, 1);
        ctx.ExpectAttachSessionResult(222, proxy, 1, Ydb::StatusIds::BAD_SESSION);
        ctx.VerifyProxyHasSessions(proxy, 1, {1});
    }

    Y_UNIT_TEST(TestAttachThenReRegister) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ui64 sessionId = ctx.MustAttachSession(proxy, 1, 0);
        UNIT_ASSERT_VALUES_EQUAL(sessionId, 1);
        ctx.VerifyProxyHasSessions(proxy, 1, {1});
        ctx.MustRegisterProxy(proxy, 2);
        ctx.VerifyProxyHasSessions(proxy, 2, {});
        ctx.VerifySessionExists(1);
    }

    Y_UNIT_TEST(TestAttachFastPath) {
        TTestContext ctx;
        ctx.Setup();
        EnableRelaxedAttach(ctx);
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy1, 1);
        ctx.MustRegisterProxy(proxy2, 1);
        // Create new session1 from proxy1
        ctx.MustAttachSession(proxy1, 1, 0);
        ctx.VerifyProxyHasSessions(proxy1, 1, {1});
        // Now send two attaches for session2 and session1 from proxy2
        ctx.SendAttachSession(111, proxy2, 1, 0);
        ctx.SendAttachSession(222, proxy2, 1, 1);
        // Even though request session2 was first, session1 must attach out of order
        UNIT_ASSERT_VALUES_EQUAL(ctx.ExpectAttachSessionResult(222, proxy2, 1), 1);
        UNIT_ASSERT_VALUES_EQUAL(ctx.ExpectAttachSessionResult(111, proxy2, 1), 2);
    }

    Y_UNIT_TEST(TestAttachFastPathBlocked) {
        TTestContext ctx;
        ctx.Setup();
        EnableRelaxedAttach(ctx);
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy1, 1);
        ctx.MustRegisterProxy(proxy2, 1);
        // Create new session1 from proxy1
        ctx.MustAttachSession(proxy1, 1, 0);
        ctx.VerifyProxyHasSessions(proxy1, 1, {1});
        // Now send lock request from proxy1 and two attaches from proxy2
        ctx.SendAcquireLock(123, proxy1, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.SyncProxy(proxy1, 1);
        ctx.SendAttachSession(111, proxy2, 1, 0);
        ctx.SendAttachSession(222, proxy2, 1, 1);
        // Now that there's a concurrent request from session1 it must no be out of order
        ctx.ExpectAcquireLockResult(123, proxy1, 1);
        UNIT_ASSERT_VALUES_EQUAL(ctx.ExpectAttachSessionResult(111, proxy2, 1), 2);
        UNIT_ASSERT_VALUES_EQUAL(ctx.ExpectAttachSessionResult(222, proxy2, 1), 1);
    }

    Y_UNIT_TEST(TestSessionDetach) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy1, 1);
        ctx.MustAttachSession(proxy1, 1, 0, 1000);
        ctx.VerifyProxyHasSessions(proxy1, 1, {1});
        // Cannot detach using wrong generation
        ctx.MustDetachSession(proxy1, 2, 1, Ydb::StatusIds::BAD_SESSION);
        // Cannot detach from unregistered proxy
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustDetachSession(proxy2, 1, 1, Ydb::StatusIds::BAD_SESSION);
        // Cannot detach from registered non-owner proxy
        ctx.MustRegisterProxy(proxy2, 1);
        ctx.MustDetachSession(proxy2, 1, 1, Ydb::StatusIds::BAD_SESSION);
        // Cannot detach sessions that don't exist
        ctx.MustDetachSession(proxy1, 1, 2, Ydb::StatusIds::SESSION_EXPIRED);
        // OK to detach if everything checks out
        ctx.VerifyProxyHasSessions(proxy1, 1, {1});
        ctx.MustDetachSession(proxy1, 1, 1);
        ctx.VerifyProxyHasSessions(proxy1, 1, {});
        // Cannot detach it twice in a row
        ctx.MustDetachSession(proxy1, 1, 1, Ydb::StatusIds::BAD_SESSION);
        // Attach it again and make sure it works with in-flight transactions
        ctx.MustAttachSession(proxy1, 1, 1);
        ctx.SendAcquireLock(111, proxy1, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.MustDetachSession(proxy1, 1, 1);
    }

    Y_UNIT_TEST(TestSessionDetachFutureId) {
        TTestContext ctx;
        ctx.Setup();
        EnableRelaxedAttach(ctx);
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        // start attaching session1
        ctx.SendAttachSession(111, proxy, 1, 0);
        // detach before receiving a reply should succeed (fast path disabled)
        ctx.MustDetachSession(proxy, 1, 1);
        // verify attach result was successful too
        UNIT_ASSERT_VALUES_EQUAL(ctx.ExpectAttachSessionResult(111, proxy, 1), 1);
    }

    Y_UNIT_TEST(TestSessionDestroy) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy1, 1);
        ctx.MustAttachSession(proxy1, 1, 0, 30000);
        ctx.VerifyProxyHasSessions(proxy1, 1, {1});
        ctx.MustDestroySession(proxy1, 1, 1);
        ctx.VerifyProxyHasSessions(proxy1, 1, {});
        ctx.VerifySessionNotFound(1);
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy2, 1);
        ctx.MustAttachSession(proxy2, 1, 0, 30000);
        ctx.VerifyProxyHasSessions(proxy1, 1, {});
        ctx.VerifyProxyHasSessions(proxy2, 1, {2});
        // proxy1 must be able to destroy the session
        ctx.MustDestroySession(proxy1, 1, 2);
        ctx.VerifyProxyHasSessions(proxy1, 1, {});
        ctx.VerifyProxyHasSessions(proxy2, 1, {});
        // proxy2 must receive notification
        auto event = ctx.ExpectEdgeEvent<TEvKesus::TEvSessionExpired>(proxy2);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetSessionId(), 2);
    }

    Y_UNIT_TEST(TestAttachTimeoutTooBig) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.RegisterProxy(proxy, 1);
        auto result = ctx.AttachSession(proxy, 1, 0, TDuration::Days(2).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetStatus(), Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(TestSessionTimeoutAfterDetach) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 60000);
        ctx.VerifyProxyHasSessions(proxy, 1, {1});
        ctx.MustDetachSession(proxy, 1, 1);
        ctx.VerifySessionExists(1);
        ctx.VerifyProxyHasSessions(proxy, 1, {});
        ctx.Sleep(60001); // must timeout after 60001ms
        ctx.VerifySessionNotFound(1);
        ctx.VerifyProxyHasSessions(proxy, 1, {});
    }

    Y_UNIT_TEST(TestSessionTimeoutAfterReboot) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 60000);
        ctx.VerifyProxyHasSessions(proxy, 1, {1});
        ctx.RebootTablet();
        ctx.VerifyProxyNotRegistered(proxy);
        ctx.VerifySessionExists(1);
        ctx.Sleep(70001); // must timeout after 60001ms + grace period (10s)
        ctx.VerifySessionNotFound(1);
    }

    Y_UNIT_TEST(TestSessionTimeoutAfterUnregister) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 60000);
        ctx.VerifySessionExists(1);
        ctx.MustUnregisterProxy(proxy, 1);
        ctx.Sleep(60001); // must timeout after 60001ms
        ctx.VerifySessionNotFound(1);
        // even unregistered proxy must receive notification
        auto event = ctx.ExpectEdgeEvent<TEvKesus::TEvSessionExpired>(proxy);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetSessionId(), 1);
    }

    Y_UNIT_TEST(TestSessionStealing) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy1, 111);
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy2, 222);
        ctx.SendAttachSession(12345, proxy1, 111, 0);
        ctx.ExpectAttachSessionResult(12345, proxy1, 111);
        ctx.VerifyProxyHasSessions(proxy1, 111, {1});
        ctx.SendAttachSession(23456, proxy2, 222, 1);
        ctx.ExpectAttachSessionResult(23456, proxy2, 222);
        ctx.VerifyProxyHasSessions(proxy1, 111, {});
        ctx.VerifyProxyHasSessions(proxy2, 222, {1});
        auto event = ctx.ExpectEdgeEvent<TEvKesus::TEvSessionStolen>(proxy1, 12345);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetProxyGeneration(), 111);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetSessionId(), 1);
    }

    Y_UNIT_TEST(TestSessionStealingAnyKey) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy1, 111);
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy2, 222);
        ctx.SendAttachSession(12345, proxy1, 111, 0);
        ctx.ExpectAttachSessionResult(12345, proxy1, 111);
        ctx.VerifyProxyHasSessions(proxy1, 111, {1});
        ctx.SendAttachSession(23456, proxy2, 222, 1, 0, "", 0, "foobar");
        ctx.ExpectAttachSessionResult(23456, proxy2, 222);
        ctx.VerifyProxyHasSessions(proxy1, 111, {});
        ctx.VerifyProxyHasSessions(proxy2, 222, {1});
        auto event = ctx.ExpectEdgeEvent<TEvKesus::TEvSessionStolen>(proxy1, 12345);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetProxyGeneration(), 111);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetSessionId(), 1);
    }

    Y_UNIT_TEST(TestSessionStealingSameKey) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy1, 111);
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy2, 222);
        ctx.SendAttachSession(12345, proxy1, 111, 0, 0, "", 0, "foobar");
        ctx.ExpectAttachSessionResult(12345, proxy1, 111);
        ctx.VerifyProxyHasSessions(proxy1, 111, {1});
        ctx.SendAttachSession(23456, proxy2, 222, 1, 0, "", 0, "foobar");
        ctx.ExpectAttachSessionResult(23456, proxy2, 222);
        ctx.VerifyProxyHasSessions(proxy1, 111, {});
        ctx.VerifyProxyHasSessions(proxy2, 222, {1});
        auto event = ctx.ExpectEdgeEvent<TEvKesus::TEvSessionStolen>(proxy1, 12345);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetProxyGeneration(), 111);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetSessionId(), 1);
    }

    Y_UNIT_TEST(TestSessionStealingDifferentKey) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy1, 111);
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy2, 222);
        ctx.SendAttachSession(12345, proxy1, 111, 0, 0, "", 0, "foobar");
        ctx.ExpectAttachSessionResult(12345, proxy1, 111);
        ctx.VerifyProxyHasSessions(proxy1, 111, {1});
        ctx.SendAttachSession(23456, proxy2, 222, 1, 0, "", 0, "foobaz");
        ctx.ExpectAttachSessionResult(23456, proxy2, 222, Ydb::StatusIds::BAD_SESSION);
        ctx.VerifyProxyHasSessions(proxy1, 111, {1});
        ctx.VerifyProxyHasSessions(proxy2, 222, {});
    }

    Y_UNIT_TEST(TestLockNotFound) {
        TTestContext ctx;
        ctx.Setup();
        ctx.VerifyLockNotFound("Lock1");
    }

    Y_UNIT_TEST(TestAcquireLocks) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 40000);
        ctx.MustAttachSession(proxy, 1, 0, 60000);
        ctx.VerifyProxyHasSessions(proxy, 1, {1, 2});
        ctx.SendAcquireLock(111, proxy, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE, 30000);
        ctx.SendAcquireLock(222, proxy, 1, 2, "Lock2", LOCK_MODE_SHARED, 30000);
        ctx.SendAcquireLock(333, proxy, 1, 1, "Lock2", LOCK_MODE_SHARED, 30000);
        ctx.ExpectAcquireLockResult(111, proxy, 1);
        ctx.ExpectAcquireLockResult(222, proxy, 1);
        ctx.ExpectAcquireLockResult(333, proxy, 1);
        ctx.VerifyLockExclusive("Lock1", 1);
        ctx.VerifyLockShared("Lock2", {1, 2});

        // verify state after reboot
        ctx.RebootTablet();
        ctx.VerifyLockExclusive("Lock1", 1);
        ctx.VerifyLockShared("Lock2", {1, 2});

        // locks have 30s timeout, but taken locks never expire
        ctx.Sleep(30001);
        ctx.VerifyLockExclusive("Lock1", 1);
        ctx.VerifyLockShared("Lock2", {1, 2});

        // at 50001ms: session1 expired, with lock ownership
        ctx.Sleep(20000);
        ctx.VerifyLockNotFound("Lock1");
        ctx.VerifyLockShared("Lock2", {2});
        ctx.VerifySessionNotFound(1);

        // at 70001ms: session2 expired, with lock ownership
        ctx.Sleep(20000);
        ctx.VerifyLockNotFound("Lock2");
        ctx.VerifySessionNotFound(2);
    }

    Y_UNIT_TEST(TestAcquireRepeat) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.VerifyProxyHasSessions(proxy, 1, {1});
        ctx.SendAcquireLock(111, proxy, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(111, proxy, 1);
        ctx.SendAcquireLock(222, proxy, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(222, proxy, 1);
        ctx.VerifyLockExclusive("Lock1", 1);

        // verify state after reboot
        ctx.RebootTablet();
        ctx.VerifyLockExclusive("Lock1", 1);
    }

    Y_UNIT_TEST(TestAcquireDowngrade) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.VerifyProxyHasSessions(proxy, 1, {1});
        ctx.SendAcquireLock(111, proxy, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(111, proxy, 1);
        ctx.SendAcquireLock(222, proxy, 1, 1, "Lock1", LOCK_MODE_SHARED);
        ctx.ExpectAcquireLockResult(222, proxy, 1);
        ctx.VerifyLockShared("Lock1", {1});

        // verify state after reboot
        ctx.RebootTablet();
        ctx.VerifyLockShared("Lock1", {1});
    }

    Y_UNIT_TEST(TestAcquireUpgrade) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.VerifyProxyHasSessions(proxy, 1, {1});
        ctx.SendAcquireLock(111, proxy, 1, 1, "Lock1", LOCK_MODE_SHARED);
        ctx.ExpectAcquireLockResult(111, proxy, 1);
        ctx.SendAcquireLock(222, proxy, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(222, proxy, 1, Ydb::StatusIds::BAD_REQUEST);
        ctx.VerifyLockShared("Lock1", {1}); // still locked in shared mode
    }

    Y_UNIT_TEST(TestAcquireTimeout) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy1, 1);
        ctx.MustRegisterProxy(proxy2, 1);
        ctx.MustAttachSession(proxy1, 1, 0, 30000);
        ctx.MustAttachSession(proxy2, 1, 0, 30000);
        ctx.VerifyProxyHasSessions(proxy1, 1, {1});
        ctx.VerifyProxyHasSessions(proxy2, 1, {2});
        // session1 takes two locks
        ctx.SendAcquireLock(111, proxy1, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(111, proxy1, 1);
        ctx.SendAcquireLock(112, proxy1, 1, 1, "Lock2", LOCK_MODE_SHARED);
        ctx.ExpectAcquireLockResult(112, proxy1, 1);
        // another session should fail both attempts immediately
        ctx.SendAcquireLock(222, proxy2, 1, 2, "Lock1", LOCK_MODE_SHARED);
        ctx.SendAcquireLock(223, proxy2, 1, 2, "Lock2", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(222, proxy2, 1, false);
        ctx.ExpectAcquireLockResult(223, proxy2, 1, false);
        // another session should fail both attemps after a timeout
        ctx.SendAcquireLock(333, proxy2, 1, 2, "Lock1", LOCK_MODE_SHARED, 60000);
        ctx.SendAcquireLock(334, proxy2, 1, 2, "Lock2", LOCK_MODE_EXCLUSIVE, 30000);
        // note the inverted order of replies
        ctx.ExpectAcquireLockResult(334, proxy2, 1, false);
        ctx.ExpectAcquireLockResult(333, proxy2, 1, false);
        // verify final lock ownership
        ctx.VerifyLockExclusive("Lock1", 1);
        ctx.VerifyLockShared("Lock2", {1});

        // verify state after reboot
        ctx.RebootTablet();
        ctx.VerifyLockExclusive("Lock1", 1);
        ctx.VerifyLockShared("Lock2", {1});
    }

    Y_UNIT_TEST(TestAcquireBeforeTimeoutViaRelease) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy1, 1);
        ctx.MustRegisterProxy(proxy2, 1);
        ctx.MustAttachSession(proxy1, 1, 0, 30000);
        ctx.MustAttachSession(proxy2, 1, 0, 30000);
        ctx.VerifyProxyHasSessions(proxy1, 1, {1});
        ctx.VerifyProxyHasSessions(proxy2, 1, {2});
        // session1 takes two locks
        ctx.SendAcquireLock(111, proxy1, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(111, proxy1, 1);
        ctx.SendAcquireLock(112, proxy1, 1, 1, "Lock2", LOCK_MODE_SHARED);
        ctx.ExpectAcquireLockResult(112, proxy1, 1);
        // session2 tries to take two locks with a big enough timeout
        ctx.SendAcquireLock(222, proxy2, 1, 2, "Lock1", LOCK_MODE_SHARED, 60000);
        ctx.SendAcquireLock(223, proxy2, 1, 2, "Lock2", LOCK_MODE_EXCLUSIVE, 60000);
        // now we explicitly release both locks
        ctx.MustReleaseLock(333, proxy1, 1, 1, "Lock1");
        ctx.MustReleaseLock(334, proxy1, 1, 1, "Lock2");
        // locks must already be atomically locked by session2
        ctx.VerifyLockShared("Lock1", {2});
        ctx.VerifyLockExclusive("Lock2", 2);
        // and session2 should receive successful results
        ctx.ExpectAcquireLockResult(222, proxy2, 1);
        ctx.ExpectAcquireLockResult(223, proxy2, 1);

        // verify state after reboot
        ctx.RebootTablet();
        ctx.VerifyLockShared("Lock1", {2});
        ctx.VerifyLockExclusive("Lock2", 2);
    }

    Y_UNIT_TEST(TestAcquireBeforeTimeoutViaSessionTimeout) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy1, 1);
        ctx.MustRegisterProxy(proxy2, 1);
        ctx.MustAttachSession(proxy1, 1, 0, 30000);
        ctx.MustAttachSession(proxy2, 1, 0, 30000);
        ctx.VerifyProxyHasSessions(proxy1, 1, {1});
        ctx.VerifyProxyHasSessions(proxy2, 1, {2});
        // session1 takes two locks
        ctx.SendAcquireLock(111, proxy1, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(111, proxy1, 1);
        ctx.SendAcquireLock(112, proxy1, 1, 1, "Lock2", LOCK_MODE_SHARED);
        ctx.ExpectAcquireLockResult(112, proxy1, 1);
        // session2 tries to take two locks with a big enough timeout
        ctx.SendAcquireLock(222, proxy2, 1, 2, "Lock1", LOCK_MODE_SHARED, 60000);
        ctx.SendAcquireLock(223, proxy2, 1, 2, "Lock2", LOCK_MODE_EXCLUSIVE, 60000);
        // now we unregister the first proxy, starting timeout for session1
        ctx.MustUnregisterProxy(proxy1, 1);
        // note that ownership still belongs to session1
        ctx.VerifyLockExclusive("Lock1", 1);
        ctx.VerifyLockShared("Lock2", {1});
        // some time passes and session2 receives successful results
        ctx.ExpectAcquireLockResult(222, proxy2, 1);
        ctx.ExpectAcquireLockResult(223, proxy2, 1);
        // now session2 is owner of those locks
        ctx.VerifyLockShared("Lock1", {2});
        ctx.VerifyLockExclusive("Lock2", 2);
        ctx.VerifySessionNotFound(1);

        // verify state after reboot
        ctx.RebootTablet();
        ctx.VerifySessionNotFound(1);
        ctx.VerifyLockShared("Lock1", {2});
        ctx.VerifyLockExclusive("Lock2", 2);
    }

    Y_UNIT_TEST(TestAcquireBeforeTimeoutViaModeChange) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy1 = ctx.Runtime->AllocateEdgeActor();
        auto proxy2 = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy1, 1);
        ctx.MustRegisterProxy(proxy2, 1);
        ctx.MustAttachSession(proxy1, 1, 0, 30000);
        ctx.MustAttachSession(proxy2, 1, 0, 30000);
        ctx.VerifyProxyHasSessions(proxy1, 1, {1});
        ctx.VerifyProxyHasSessions(proxy2, 1, {2});
        // session1 takes two locks
        ctx.SendAcquireLock(111, proxy1, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(111, proxy1, 1);
        ctx.SendAcquireLock(112, proxy1, 1, 1, "Lock2", LOCK_MODE_SHARED);
        ctx.ExpectAcquireLockResult(112, proxy1, 1);
        // session2 tries to take two locks with a big enough timeout
        ctx.SendAcquireLock(222, proxy2, 1, 2, "Lock1", LOCK_MODE_SHARED, 60000);
        ctx.SendAcquireLock(223, proxy2, 1, 2, "Lock2", LOCK_MODE_EXCLUSIVE, 60000);
        // now session1 changes mode to shared, allowing session2 to proceed
        ctx.SendAcquireLock(333, proxy1, 1, 1, "Lock1", LOCK_MODE_SHARED);
        ctx.ExpectAcquireLockResult(333, proxy1, 1);
        ctx.VerifyLockShared("Lock1", {1, 2});
        ctx.VerifyLockShared("Lock2", {1});
        ctx.VerifyLockWaiters("Lock1", {}); // nobody should wait for Lock1
        ctx.ExpectAcquireLockResult(222, proxy2, 1); // verify session2 gets the reply
        ctx.VerifyLockWaiters("Lock2", {2}); // verify session2 still waiting for Lock2
        // now session2 changes mode to shared, old request is dropped, new request succeeds
        ctx.SendAcquireLock(444, proxy2, 1, 2, "Lock2", LOCK_MODE_SHARED);
        ctx.ExpectAcquireLockResult(223, proxy2, 1, Ydb::StatusIds::ABORTED);
        ctx.ExpectAcquireLockResult(444, proxy2, 1);
        ctx.VerifyLockShared("Lock2", {1, 2});
        ctx.VerifyLockWaiters("Lock2", {});

        // verify after reboot locks stay valid
        ctx.RebootTablet();
        ctx.VerifyLockShared("Lock1", {1, 2});
        ctx.VerifyLockShared("Lock2", {1, 2});
    }

    Y_UNIT_TEST(TestAcquireSharedBlocked) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        // session1 takes shared lock
        ctx.SendAcquireLock(111, proxy, 1, 1, "Lock1", LOCK_MODE_SHARED);
        ctx.ExpectAcquireLockResult(111, proxy, 1);
        // session2 enqueues an exclusive lock
        ctx.SendAcquireLock(222, proxy, 1, 2, "Lock1", LOCK_MODE_EXCLUSIVE, 60000);
        // session3 enqueues a shared lock (blocked by enqueued exclusive lock)
        ctx.SendAcquireLock(333, proxy, 1, 3, "Lock1", LOCK_MODE_SHARED, 60000);
        // synchronize and verify lock state
        ctx.SyncProxy(proxy, 1, true);
        ctx.VerifyLockShared("Lock1", {1});
        ctx.VerifyLockWaiters("Lock1", {2, 3});
        // destroy session2, release everything it owns
        ctx.MustDestroySession(proxy, 1, 2);
        // observe session3 taking the lock too
        ctx.ExpectAcquireLockResult(333, proxy, 1);
        ctx.VerifyLockShared("Lock1", {1, 3});
        ctx.VerifyLockWaiters("Lock1", {});
    }

    Y_UNIT_TEST(TestAcquireTimeoutAfterReboot) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 60000);
        ctx.MustAttachSession(proxy, 1, 0, 60000);
        // session1 takes exclusive lock
        ctx.SendAcquireLock(111, proxy, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(111, proxy, 1);
        // session2 enqueues a shared lock
        ctx.SendAcquireLock(222, proxy, 1, 2, "Lock1", LOCK_MODE_SHARED, 60000);
        // synchronize and verify lock state
        ctx.SyncProxy(proxy, 1, true);
        ctx.VerifyLockExclusive("Lock1", 1);
        ctx.VerifyLockWaiters("Lock1", {2});
        // reboot and reattach
        ctx.RebootTablet();
        ctx.MustRegisterProxy(proxy, 2);
        ctx.MustAttachSession(proxy, 2, 1, 60000);
        ctx.MustAttachSession(proxy, 2, 2, 60000);
        ctx.VerifyLockExclusive("Lock1", 1);
        ctx.VerifyLockWaiters("Lock1", {2});
        // at 60001ms mark the lock times out (even though sessions are alive)
        ctx.Sleep(60001);
        ctx.VerifySessionExists(1);
        ctx.VerifySessionExists(2);
        ctx.VerifyLockExclusive("Lock1", 1);
        ctx.VerifyLockWaiters("Lock1", {});
    }

    Y_UNIT_TEST(TestAcquireWaiterDowngrade) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        // session1 takes shared lock
        ctx.SendAcquireLock(111, proxy, 1, 1, "Lock1", LOCK_MODE_SHARED);
        ctx.ExpectAcquireLockResult(111, proxy, 1);
        // session2 enqueues an exclusive lock
        ctx.SendAcquireLock(222, proxy, 1, 2, "Lock1", LOCK_MODE_EXCLUSIVE, 30000);
        // now it immediately changes mode to shared
        ctx.SendAcquireLock(333, proxy, 1, 2, "Lock1", LOCK_MODE_SHARED, 30000);
        // first request gets cancelled, second succeeds
        ctx.ExpectAcquireLockResult(222, proxy, 1, Ydb::StatusIds::ABORTED);
        ctx.ExpectAcquireLockResult(333, proxy, 1);
        ctx.VerifyLockShared("Lock1", {1, 2});
    }

    Y_UNIT_TEST(TestAcquireWaiterUpgrade) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        // session1 takes exclusive lock
        ctx.SendAcquireLock(111, proxy, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(111, proxy, 1);
        // session2 enqueues a shared lock
        ctx.SendAcquireLock(222, proxy, 1, 2, "Lock1", LOCK_MODE_SHARED, 30000);
        // now it immediately changes mode to exclusive
        ctx.SendAcquireLock(333, proxy, 1, 2, "Lock1", LOCK_MODE_EXCLUSIVE, 30000);
        ctx.ExpectAcquireLockResult(333, proxy, 1, Ydb::StatusIds::BAD_REQUEST);
        // the old request must still be waiting
        ctx.VerifyLockExclusive("Lock1", 1);
        ctx.VerifyLockWaiters("Lock1", {2});
    }

    Y_UNIT_TEST(TestAcquireWaiterChangeTimeoutToZero) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        // first make sure tablet doesn't use any timers
        ctx.Runtime->EnableScheduleForActor(ctx.GetTabletActorId(), false);
        // session1 takes exclusive lock
        ctx.SendAcquireLock(111, proxy, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(111, proxy, 1);
        // session2 enqueues a shared lock
        ctx.SendAcquireLock(222, proxy, 1, 2, "Lock1", LOCK_MODE_SHARED, 30000);
        // now it immediately changes timeout to zero
        ctx.SendAcquireLock(333, proxy, 1, 2, "Lock1", LOCK_MODE_SHARED, 0);
        // it must receive correct replies
        ctx.ExpectAcquireLockResult(222, proxy, 1, Ydb::StatusIds::ABORTED);
        ctx.ExpectAcquireLockResult(333, proxy, 1, false);
        // there must be no waiters
        ctx.VerifyLockExclusive("Lock1", 1);
        ctx.VerifyLockWaiters("Lock1", {});

        // reboot and double check the state
        ctx.RebootTablet();
        ctx.VerifyLockExclusive("Lock1", 1);
        ctx.VerifyLockWaiters("Lock1", {});
    }

    Y_UNIT_TEST(TestAcquireWaiterRelease) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        // session1 takes exclusive lock
        ctx.SendAcquireLock(111, proxy, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(111, proxy, 1);
        // session2 enqueues a shared lock
        ctx.SendAcquireLock(222, proxy, 1, 2, "Lock1", LOCK_MODE_SHARED, 30000);
        // now it immediately tries to release it
        ctx.MustReleaseLock(333, proxy, 1, 2, "Lock1");
        ctx.ExpectAcquireLockResult(222, proxy, 1, Ydb::StatusIds::ABORTED);
    }

    Y_UNIT_TEST(TestReleaseLockFailure) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        // session1 cannot release missing lock
        ctx.MustReleaseLock(111, proxy, 1, 1, "Lock1", false);
        // session1 takes exclusive lock
        ctx.SendAcquireLock(222, proxy, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(222, proxy, 1);
        // session2 cannot release lock it doesn't have locked
        ctx.MustReleaseLock(333, proxy, 1, 2, "Lock1", false);
    }

    Y_UNIT_TEST(TestCreateSemaphore) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.SendAcquireLock(111, proxy, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(111, proxy, 1);

        ctx.CreateSemaphore("Sem1", 0, Ydb::StatusIds::BAD_REQUEST);
        ctx.CreateSemaphore("Sem1", 42);
        ctx.CreateSemaphore("Sem1", 42, Ydb::StatusIds::ALREADY_EXISTS);
        ctx.CreateSemaphore("Sem1", 51, Ydb::StatusIds::ALREADY_EXISTS);
        ctx.CreateSemaphore("Lock1", 42, Ydb::StatusIds::PRECONDITION_FAILED);
        ctx.CreateSemaphore("Lock1", -1, Ydb::StatusIds::PRECONDITION_FAILED);
        ctx.VerifySemaphoreOwners("Sem1", {});
        ctx.VerifySemaphoreNotFound("Sem2");

        ctx.RebootTablet();
        ctx.VerifySemaphoreOwners("Sem1", {});
        ctx.VerifySemaphoreNotFound("Sem2");
    }

    Y_UNIT_TEST(TestDeleteSemaphore) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.SendAcquireLock(111, proxy, 1, 1, "Lock1", LOCK_MODE_EXCLUSIVE);
        ctx.ExpectAcquireLockResult(111, proxy, 1);

        ctx.DeleteSemaphore("Lock1", Ydb::StatusIds::PRECONDITION_FAILED);
        ctx.DeleteSemaphore("Sem1", Ydb::StatusIds::NOT_FOUND);
        ctx.CreateSemaphore("Sem1", 42);
        ctx.DeleteSemaphore("Sem1");
        ctx.DeleteSemaphore("Sem1", Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(TestAcquireSemaphore) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);

        ctx.SendAcquireSemaphore(111, proxy, 1, 1, "Sem1", 1);
        ctx.ExpectAcquireSemaphoreResult(111, proxy, 1, Ydb::StatusIds::NOT_FOUND);

        ctx.CreateSemaphore("Sem1", 1);

        // cannot acquire 0 tokens
        ctx.SendAcquireSemaphore(222, proxy, 1, 1, "Sem1", 0);
        ctx.ExpectAcquireSemaphoreResult(222, proxy, 1, Ydb::StatusIds::BAD_REQUEST);

        // cannot acquire more tokens than available
        ctx.SendAcquireSemaphore(333, proxy, 1, 1, "Sem1", 100500);
        ctx.ExpectAcquireSemaphoreResult(333, proxy, 1, Ydb::StatusIds::BAD_REQUEST);

        ctx.SendAcquireSemaphore(222, proxy, 1, 1, "Sem1", 1);
        ctx.SendAcquireSemaphore(333, proxy, 1, 2, "Sem1", 1, 60000);
        ctx.ExpectAcquireSemaphoreResult(222, proxy, 1);
        ctx.ExpectAcquireSemaphorePending(333, proxy, 1);
        ctx.VerifySemaphoreOwners("Sem1", {1});
        ctx.VerifySemaphoreWaiters("Sem1", {2});

        // Cannot destroy semaphore unless forced
        ctx.DeleteSemaphore("Sem1", Ydb::StatusIds::PRECONDITION_FAILED);
        ctx.DeleteSemaphore("Sem1", true);

        // Forced destruction aborts the request
        ctx.ExpectAcquireSemaphoreResult(333, proxy, 1, Ydb::StatusIds::ABORTED);
    }

    Y_UNIT_TEST(TestReleaseSemaphore) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);

        ctx.CreateSemaphore("Sem1", 1);
        ctx.SendAcquireSemaphore(111, proxy, 1, 1, "Sem1", 1);
        ctx.SendAcquireSemaphore(222, proxy, 1, 2, "Sem1", 1, 60000);
        ctx.ExpectAcquireSemaphoreResult(111, proxy, 1);
        ctx.ExpectAcquireSemaphorePending(222, proxy, 1);
        ctx.VerifySemaphoreOwners("Sem1", {1});
        ctx.VerifySemaphoreWaiters("Sem1", {2});

        // But it's ok to release with ephemeral=false
        ctx.MustReleaseSemaphore(333, proxy, 1, 2, "Sem1");
        ctx.VerifySemaphoreOwners("Sem1", {1});
        ctx.VerifySemaphoreWaiters("Sem1", {});

        ctx.MustReleaseSemaphore(444, proxy, 1, 1, "Sem1");
        ctx.VerifySemaphoreOwners("Sem1", {});
        ctx.VerifySemaphoreWaiters("Sem1", {});
    }

    Y_UNIT_TEST(TestAcquireSemaphoreTimeout) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);

        ctx.CreateSemaphore("Sem1", 1);
        ctx.SendAcquireSemaphore(111, proxy, 1, 1, "Sem1", 1);
        ctx.SendAcquireSemaphore(222, proxy, 1, 2, "Sem1", 1, 60000);
        ctx.ExpectAcquireSemaphoreResult(111, proxy, 1);
        ctx.ExpectAcquireSemaphorePending(222, proxy, 1);
        ctx.VerifySemaphoreOwners("Sem1", {1});
        ctx.VerifySemaphoreWaiters("Sem1", {2});

        ctx.Sleep(60001);
        ctx.ExpectAcquireSemaphoreResult(222, proxy, 1, false);
        ctx.VerifySemaphoreOwners("Sem1", {1});
        ctx.VerifySemaphoreWaiters("Sem1", {});
    }

    Y_UNIT_TEST(TestAcquireSemaphoreTimeoutTooBig) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);

        ctx.CreateSemaphore("Sem1", 1);
        ctx.SendAcquireSemaphore(111, proxy, 1, 1, "Sem1", 1, TDuration::Days(2).MilliSeconds());
        ctx.ExpectAcquireSemaphoreResult(111, proxy, 1, Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(TestAcquireSemaphoreTimeoutInfinite) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);

        ctx.CreateSemaphore("Sem1", 1);
        ctx.SendAcquireSemaphore(111, proxy, 1, 1, "Sem1", 1);
        ctx.SendAcquireSemaphore(222, proxy, 1, 2, "Sem1", 1, Max<ui64>());
        ctx.ExpectAcquireSemaphoreResult(111, proxy, 1);
        ctx.ExpectAcquireSemaphorePending(222, proxy, 1);

        // No way to test if timeout is not firing...
        ctx.VerifySemaphoreOwners("Sem1", {1});
        ctx.VerifySemaphoreWaiters("Sem1", {2});
    }

    Y_UNIT_TEST(TestAcquireSemaphoreRebootTimeout) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);

        ctx.CreateSemaphore("Sem1", 1);
        ctx.SendAcquireSemaphore(111, proxy, 1, 1, "Sem1", 1);
        ctx.ExpectAcquireSemaphoreResult(111, proxy, 1);

        ctx.RebootTablet();
        ctx.VerifySemaphoreOwners("Sem1", {1});
        ctx.Sleep(40001);
        ctx.VerifySemaphoreOwners("Sem1", {});
    }

    Y_UNIT_TEST(TestAcquireSemaphoreViaDecrease) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);

        ctx.CreateSemaphore("Sem1", 3);
        ctx.SendAcquireSemaphore(111, proxy, 1, 1, "Sem1", 2);
        ctx.SendAcquireSemaphore(222, proxy, 1, 2, "Sem1", 1);
        ctx.SendAcquireSemaphore(333, proxy, 1, 3, "Sem1", 1, 60000);
        ctx.ExpectAcquireSemaphoreResult(111, proxy, 1);
        ctx.ExpectAcquireSemaphoreResult(222, proxy, 1);
        ctx.ExpectAcquireSemaphorePending(333, proxy, 1);
        ctx.VerifySemaphoreOwners("Sem1", {1, 2});
        ctx.VerifySemaphoreWaiters("Sem1", {3});

        // now decrease count in session1
        ctx.SendAcquireSemaphore(444, proxy, 1, 1, "Sem1", 1);
        ctx.ExpectAcquireSemaphoreResult(444, proxy, 1);
        ctx.ExpectAcquireSemaphoreResult(333, proxy, 1);
        ctx.VerifySemaphoreOwners("Sem1", {1, 2, 3});
        ctx.VerifySemaphoreWaiters("Sem1", {});

        // double check state after reboot
        ctx.RebootTablet();
        ctx.VerifySemaphoreOwners("Sem1", {1, 2, 3});
        ctx.VerifySemaphoreWaiters("Sem1", {});
    }

    Y_UNIT_TEST(TestAcquireSemaphoreViaRelease) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);

        ctx.CreateSemaphore("Sem1", 3);
        ctx.SendAcquireSemaphore(111, proxy, 1, 1, "Sem1", 2);
        ctx.SendAcquireSemaphore(222, proxy, 1, 2, "Sem1", 2, 60000);
        ctx.SendAcquireSemaphore(333, proxy, 1, 3, "Sem1", 1, 60000);
        ctx.ExpectAcquireSemaphoreResult(111, proxy, 1);
        ctx.ExpectAcquireSemaphorePending(222, proxy, 1);
        ctx.ExpectAcquireSemaphorePending(333, proxy, 1);
        ctx.VerifySemaphoreOwners("Sem1", {1});
        ctx.VerifySemaphoreWaiters("Sem1", {2, 3});

        ctx.MustReleaseSemaphore(444, proxy, 1, 1, "Sem1");
        ctx.ExpectAcquireSemaphoreResult(222, proxy, 1);
        ctx.ExpectAcquireSemaphoreResult(333, proxy, 1);
        ctx.VerifySemaphoreOwners("Sem1", {2, 3});
        ctx.VerifySemaphoreWaiters("Sem1", {});
    }

    Y_UNIT_TEST(TestSemaphoreData) {
        TTestContext ctx;
        ctx.Setup();

        ctx.CreateSemaphore("Sem1", 1);
        ctx.CreateSemaphore("Sem2", 1, "\x81\x82\x83\x84");
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeSemaphore("Sem1").Data, "");
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeSemaphore("Sem2").Data, "\x81\x82\x83\x84");

        // Verify it persists across reboots
        ctx.RebootTablet();
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeSemaphore("Sem1").Data, "");
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeSemaphore("Sem2").Data, "\x81\x82\x83\x84");

        // ALREADY_EXISTS should not update the data
        ctx.CreateSemaphore("Sem1", 1, "\x81\x82\x83\x84", Ydb::StatusIds::ALREADY_EXISTS);
        ctx.CreateSemaphore("Sem2", 1, "", Ydb::StatusIds::ALREADY_EXISTS);
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeSemaphore("Sem1").Data, "");
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeSemaphore("Sem2").Data, "\x81\x82\x83\x84");

        ctx.UpdateSemaphore("Sem1", "\x85\x86\x87\x88");
        ctx.UpdateSemaphore("Sem2", "foobar");
        ctx.UpdateSemaphore("Sem3", "foobarbaz", Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeSemaphore("Sem1").Data, "\x85\x86\x87\x88");
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeSemaphore("Sem2").Data, "foobar");
        ctx.VerifySemaphoreNotFound("Sem3");

        // Verify it persists across reboots
        ctx.RebootTablet();
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeSemaphore("Sem1").Data, "\x85\x86\x87\x88");
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeSemaphore("Sem2").Data, "foobar");
        ctx.VerifySemaphoreNotFound("Sem3");
    }

    Y_UNIT_TEST(TestDescribeSemaphoreWatches) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustDetachSession(proxy, 1, 2);

        ui64 reqId = 111;

        ctx.CreateSemaphore("Sem1", 3);
        ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 1, "Sem1");
        ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1);
        ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 2, "Sem1");
        ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1, Ydb::StatusIds::BAD_SESSION);

        ctx.MustAttachSession(proxy, 1, 2, 30000);

        ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 1, "Sem1", false, true);
        {
            auto desc = ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1);
            UNIT_ASSERT_VALUES_EQUAL(desc.WatchAdded, true);
        }
        ui64 watch1 = reqId;

        ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 2, "Sem1", true, false);
        {
            auto desc = ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1);
            UNIT_ASSERT_VALUES_EQUAL(desc.WatchAdded, true);
        }
        ui64 watch2 = reqId;

        ctx.UpdateSemaphore("Sem1", "new data");
        {
            auto changes = ctx.ExpectDescribeSemaphoreChanged(watch2, proxy, 1);
            UNIT_ASSERT_VALUES_EQUAL(changes.DataChanged, true);
            UNIT_ASSERT_VALUES_EQUAL(changes.OwnersChanged, false);
        }

        ctx.SendAcquireSemaphore(++reqId, proxy, 1, 1, "Sem1", 1);
        ctx.ExpectAcquireSemaphoreResult(reqId, proxy, 1);
        {
            auto changes = ctx.ExpectDescribeSemaphoreChanged(watch1, proxy, 1);
            UNIT_ASSERT_VALUES_EQUAL(changes.DataChanged, false);
            UNIT_ASSERT_VALUES_EQUAL(changes.OwnersChanged, true);
        }

        // Arrange for Sem1 to have:
        // - session 1 count 1
        // - session 2 count 2
        // - session 3 count 1 (waiting)
        // - session 4 count 1 (waiting)
        ctx.SendAcquireSemaphore(++reqId, proxy, 1, 2, "Sem1", 2);
        ctx.ExpectAcquireSemaphoreResult(reqId, proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.SendAcquireSemaphore(++reqId, proxy, 1, 3, "Sem1", 1, 30000);
        ctx.ExpectAcquireSemaphorePending(reqId, proxy, 1);
        ui64 acquire3 = reqId;
        ctx.SendAcquireSemaphore(++reqId, proxy, 1, 4, "Sem1", 1, 30000);
        ctx.ExpectAcquireSemaphorePending(reqId, proxy, 1);
        ui64 acquire4 = reqId;

        // Add owner watch in session 4
        ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 4, "Sem1", false, true);
        ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1);
        ui64 watch3 = reqId;

        // Changing acquire3 data shouldn't trigger a change notification
        ctx.SendAcquireSemaphore(++reqId, proxy, 1, 3, "Sem1", 1, 30000, "waiter3");
        ctx.ExpectAcquireSemaphoreResult(acquire3, proxy, 1, Ydb::StatusIds::ABORTED);
        ctx.ExpectAcquireSemaphorePending(reqId, proxy, 1);
        acquire3 = reqId;
        ctx.ExpectNoEdgeEvent<TEvKesus::TEvDescribeSemaphoreChanged>(proxy, TDuration::Seconds(1));

        // However changing session 2 data should trigger it (since it is an owner)
        ctx.SendAcquireSemaphore(++reqId, proxy, 1, 2, "Sem1", 2, 0, "owner2");
        ctx.ExpectAcquireSemaphoreResult(reqId, proxy, 1);
        ctx.ExpectDescribeSemaphoreChanged(watch3, proxy, 1);

        // Add owner watches in sessions 2 and 4
        ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 2, "Sem1", false, true);
        ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1);
        ui64 watch4 = reqId;
        ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 4, "Sem1", false, true);
        ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1);
        ui64 watch5 = reqId;

        // Now destroy session 2 (which releases its ownership)
        ctx.MustDestroySession(proxy, 1, 2);
        ctx.ExpectAcquireSemaphoreResult(acquire3, proxy, 1);
        ctx.ExpectAcquireSemaphoreResult(acquire4, proxy, 1);

        // Session 4 should receive notification
        ctx.ExpectDescribeSemaphoreChanged(watch5, proxy, 1);

        // However session 2 is destroyed and should not
        ctx.ExpectNoEdgeEvent<TEvKesus::TEvDescribeSemaphoreChanged>(proxy, TDuration::Seconds(1));
        Y_UNUSED(watch4);

        // Add owners watch in session 5
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 5, "Sem1", false, true);
        ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1);
        ui64 watch6 = reqId;

        // Forcibly destroying semaphore should produce notification with both flags set
        ctx.DeleteSemaphore("Sem1", true);
        {
            auto changes = ctx.ExpectDescribeSemaphoreChanged(watch6, proxy, 1);
            UNIT_ASSERT_VALUES_EQUAL(changes.DataChanged, true);
            UNIT_ASSERT_VALUES_EQUAL(changes.OwnersChanged, true);
        }

        // Create a new semaphore, lock it and watch for owners changes
        ctx.CreateSemaphore("Sem2", 3);
        ctx.SendAcquireSemaphore(++reqId, proxy, 1, 1, "Sem2", 3);
        ctx.ExpectAcquireSemaphoreResult(reqId, proxy, 1);
        ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 5, "Sem2", false, true);
        ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1);
        ui64 watch7 = reqId;

        // No changes should result in no notification
        ctx.SendAcquireSemaphore(++reqId, proxy, 1, 1, "Sem2", 3);
        ctx.ExpectAcquireSemaphoreResult(reqId, proxy, 1);
        ctx.ExpectNoEdgeEvent<TEvKesus::TEvDescribeSemaphoreChanged>(proxy, TDuration::Seconds(1));

        // Changing count should produce notification
        ctx.SendAcquireSemaphore(++reqId, proxy, 1, 1, "Sem2", 2);
        ctx.ExpectAcquireSemaphoreResult(reqId, proxy, 1);
        ctx.ExpectDescribeSemaphoreChanged(watch7, proxy, 1);

        // Changing both count and data should produce owners notification
        ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 5, "Sem2", false, true);
        ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1);
        ui64 watch8 = reqId;
        ctx.SendAcquireSemaphore(++reqId, proxy, 1, 1, "Sem2", 1, 0, "some data");
        ctx.ExpectAcquireSemaphoreResult(reqId, proxy, 1);
        ctx.ExpectDescribeSemaphoreChanged(watch8, proxy, 1);

        // Replacing watch should produce an empty notification
        ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 5, "Sem2", false, true);
        ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1);
        ui64 watch9 = reqId;
        ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 5, "Sem2", true, false);
        ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1);
        {
            auto changes = ctx.ExpectDescribeSemaphoreChanged(watch9, proxy, 1);
            UNIT_ASSERT_VALUES_EQUAL(changes.DataChanged, false);
            UNIT_ASSERT_VALUES_EQUAL(changes.OwnersChanged, false);
        }
    }

    Y_UNIT_TEST(TestSemaphoreReleaseReacquire) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);
        ctx.MustAttachSession(proxy, 1, 0, 30000);

        for (int i = 0; i < 10; ++i) {
            // Create and destroy semaphore several times to increment next ids
            ctx.CreateSemaphore("Sem1", 1);
            ctx.DeleteSemaphore("Sem1");
        }

        ctx.CreateSemaphore("Sem1", 1);

        // The first session owns semaphore immediately
        ctx.SendAcquireSemaphore(111, proxy, 1, 1, "Sem1", 1);
        ctx.ExpectAcquireSemaphoreResult(111, proxy, 1);

        // The second session tries to acquire semaphore, but releases immediately
        ctx.SendAcquireSemaphore(222, proxy, 1, 2, "Sem1", 1, 10000);
        ctx.ExpectAcquireSemaphorePending(222, proxy, 1);
        ctx.MustReleaseSemaphore(333, proxy, 1, 2, "Sem1");
        ctx.ExpectAcquireSemaphoreResult(222, proxy, 1, Ydb::StatusIds::ABORTED);

        // The second session now tries to reacquire semaphore
        ctx.SendAcquireSemaphore(444, proxy, 1, 2, "Sem1", 1, 10000);
        ctx.ExpectAcquireSemaphorePending(444, proxy, 1);

        // When the first session releases semaphore it must be acquired by the second session
        ctx.MustReleaseSemaphore(555, proxy, 1, 1, "Sem1");
        ctx.ExpectAcquireSemaphoreResult(444, proxy, 1);
    }

    Y_UNIT_TEST(TestSemaphoreSessionFailures) {
        TTestContext ctx;
        ctx.Setup();
        auto proxy = ctx.Runtime->AllocateEdgeActor();
        ctx.MustRegisterProxy(proxy, 1);
        ctx.MustAttachSession(proxy, 1, 0, 30000);

        ui64 reqId = 111;

        ctx.SessionCreateSemaphore(++reqId, proxy, 1, 1, "Sem1", 5, "some data");
        ctx.SessionUpdateSemaphore(++reqId, proxy, 1, 1, "Sem1", "other data");
        ctx.SessionDeleteSemaphore(++reqId, proxy, 1, 1, "Sem1");

        auto testAllFailures = [&](Ydb::StatusIds::StatusCode status) {
            ctx.SessionCreateSemaphore(++reqId, proxy, 1, 1, "Sem1", 5, "some data", status);
            ctx.SessionUpdateSemaphore(++reqId, proxy, 1, 1, "Sem1", "other data", status);
            ctx.SessionDeleteSemaphore(++reqId, proxy, 1, 1, "Sem1", status);
            ctx.SendAcquireSemaphore(++reqId, proxy, 1, 1, "Sem1", 1);
            ctx.ExpectAcquireSemaphoreResult(reqId, proxy, 1, status);
            ctx.MustReleaseSemaphore(++reqId, proxy, 1, 1, "Sem1", status);
            ctx.SendSessionDescribeSemaphore(++reqId, proxy, 1, 1, "Sem1");
            ctx.ExpectDescribeSemaphoreResult(reqId, proxy, 1, status);
        };

        ctx.MustDetachSession(proxy, 1, 1);
        testAllFailures(Ydb::StatusIds::BAD_SESSION);
        ctx.MustDestroySession(proxy, 1, 1);
        testAllFailures(Ydb::StatusIds::SESSION_EXPIRED);
        ctx.MustRegisterProxy(proxy, 2);
        testAllFailures(Ydb::StatusIds::BAD_SESSION);
    }

    Y_UNIT_TEST(TestQuoterResourceDescribe) {
        TTestContext ctx;
        ctx.Setup();

        NKikimrKesus::THierarchicalDRRResourceConfig cfg1;
        cfg1.SetMaxUnitsPerSecond(100500);
        cfg1.SetMaxBurstSizeCoefficient(1.5);

        NKikimrKesus::THierarchicalDRRResourceConfig cfg2;
        cfg2.SetMaxUnitsPerSecond(10);

        ctx.AddQuoterResource("/Root", cfg1); // id=1
        ctx.AddQuoterResource("/Root/Folder", cfg1); // id=2
        ctx.AddQuoterResource("/Root/Q1", cfg2); // id=3
        ctx.AddQuoterResource("/Root/Folder/Q1", cfg2); // id=4
        ctx.AddQuoterResource("/Root/Folder/Q2", cfg2); // id=5
        ctx.AddQuoterResource("/Root/Folder/Q3", cfg2); // id=6

        ctx.AddQuoterResource("/Root2", cfg1); // id=7
        ctx.AddQuoterResource("/Root2/Q", cfg2); // id=8

        auto testDescribe = [&]() {
            ctx.VerifyDescribeQuoterResources({100}, {}, false, Ydb::StatusIds::NOT_FOUND); // no such id
            ctx.VerifyDescribeQuoterResources({}, {"Nonexistent/Path"}, false, Ydb::StatusIds::NOT_FOUND); // no such path
            ctx.VerifyDescribeQuoterResources({}, {"/Root", ""}, false, Ydb::StatusIds::NOT_FOUND); // empty path
            ctx.VerifyDescribeQuoterResources({1, 1}, {}, false); // two times is OK
            ctx.VerifyDescribeQuoterResources({}, {"/Root2/Q", "/Root2/Q"}, false); // two times is OK

            {
                // All
                const auto resources = ctx.DescribeQuoterResources({}, {}, true);
                UNIT_ASSERT_VALUES_EQUAL(resources.ResourcesSize(), 8);
                UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(0).GetResourcePath(), "Root");
                UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(1).GetResourcePath(), "Root/Folder");
                UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(2).GetResourcePath(), "Root/Folder/Q1");
                UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(3).GetResourcePath(), "Root/Folder/Q2");
                UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(4).GetResourcePath(), "Root/Folder/Q3");
                UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(5).GetResourcePath(), "Root/Q1");
                UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(6).GetResourcePath(), "Root2");
                UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(7).GetResourcePath(), "Root2/Q");

                UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(0).GetResourceId(), 1);
                UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(1).GetResourceId(), 2);
                UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(2).GetResourceId(), 4);
                UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(3).GetResourceId(), 5);
                UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(4).GetResourceId(), 6);
                UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(5).GetResourceId(), 3);
                UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(6).GetResourceId(), 7);
                UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(7).GetResourceId(), 8);

                UNIT_ASSERT_DOUBLES_EQUAL(
                   resources.GetResources(1).GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond(),
                   100500, 0.001);
                UNIT_ASSERT_DOUBLES_EQUAL(
                   resources.GetResources(6).GetHierarchicalDRRResourceConfig().GetMaxBurstSizeCoefficient(),
                   1.5, 0.001);
                UNIT_ASSERT_DOUBLES_EQUAL(
                   resources.GetResources(7).GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond(),
                   10, 0.001);
            }
            {
                // All upper level
                const auto resources = ctx.DescribeQuoterResources({}, {}, false);
                UNIT_ASSERT_VALUES_EQUAL(resources.ResourcesSize(), 2);
                UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(0).GetResourcePath(), "Root");
                UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(1).GetResourcePath(), "Root2");

                UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(0).GetResourceId(), 1);
                UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(1).GetResourceId(), 7);
            }
            {
                // By id
                const auto resources = ctx.DescribeQuoterResources({3, 2}, {}, true);
                UNIT_ASSERT_VALUES_EQUAL(resources.ResourcesSize(), 5);

                const auto resources2 = ctx.DescribeQuoterResources({3, 2}, {}, false);
                UNIT_ASSERT_VALUES_EQUAL(resources2.ResourcesSize(), 2);
            }
            {
                // By path
                const auto resources = ctx.DescribeQuoterResources({}, {"Root2/"}, true);
                UNIT_ASSERT_VALUES_EQUAL(resources.ResourcesSize(), 2);

                const auto resources2 = ctx.DescribeQuoterResources({}, {"Root2/"}, false);
                UNIT_ASSERT_VALUES_EQUAL(resources2.ResourcesSize(), 1);
            }
        };
        testDescribe();
        ctx.RebootTablet();
        testDescribe();
    }

    Y_UNIT_TEST(TestQuoterResourceCreation) {
        TTestContext ctx;
        ctx.Setup();

        // validation
        ctx.AddQuoterResource("/a/b", Ydb::StatusIds::BAD_REQUEST); // no parent resource
        ctx.AddQuoterResource(":-)", Ydb::StatusIds::BAD_REQUEST); // invalid resource name
        ctx.AddQuoterResource("", Ydb::StatusIds::BAD_REQUEST); // empty path
        ctx.AddQuoterResource("/", Ydb::StatusIds::BAD_REQUEST); // empty path
        ctx.AddQuoterResource("//", Ydb::StatusIds::BAD_REQUEST); // empty path
        {
            NKikimrKesus::TStreamingQuoterResource res;
            res.SetResourceId(42);
            res.SetResourcePath("/CorrentPath");
            res.MutableHierarchicalDRRResourceConfig();
            ctx.AddQuoterResource(res, Ydb::StatusIds::BAD_REQUEST); // resource id specified
        }
        {
            NKikimrKesus::TStreamingQuoterResource res;
            res.SetResourcePath("/CorrentPath");
            ctx.AddQuoterResource(res, Ydb::StatusIds::BAD_REQUEST); // DRR config is not specified
        }

        ctx.AddQuoterResource("RootQuoter", 42.0); // OK
        ctx.AddQuoterResource("/RootQuoter/", 42.0, Ydb::StatusIds::ALREADY_EXISTS);

        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetMaxUnitsPerSecond(100500);
        ctx.AddQuoterResource("/RootQuoter/", cfg, Ydb::StatusIds::BAD_REQUEST); // different settings

        ctx.AddQuoterResource("RootQuoter/SubQuoter");
        ctx.AddQuoterResource("/RootQuoter//OtherSubQuoter/", cfg);

        auto checkResources = [&]() {
            const auto resources = ctx.DescribeQuoterResources({}, {}, true);
            UNIT_ASSERT_VALUES_EQUAL(resources.ResourcesSize(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(0).GetResourcePath(), "RootQuoter");
            UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(1).GetResourcePath(), "RootQuoter/OtherSubQuoter");
            UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(2).GetResourcePath(), "RootQuoter/SubQuoter");

            UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(0).GetResourceId(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(1).GetResourceId(), 3);
            UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(2).GetResourceId(), 2);

            UNIT_ASSERT_DOUBLES_EQUAL(
                resources.GetResources(1).GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond(),
                100500, 0.001);
        };
        checkResources();
        ctx.RebootTablet();
        checkResources();

        ctx.AddQuoterResource("/RootQuoter", 42.0, Ydb::StatusIds::ALREADY_EXISTS); // properly loaded

        // check that resource id was persisted
        ctx.AddQuoterResource("OtherRootQuoter", 100.0);
        const auto resources = ctx.DescribeQuoterResources({}, {"OtherRootQuoter"}, false);
        UNIT_ASSERT_VALUES_EQUAL(resources.ResourcesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resources.GetResources(0).GetResourceId(), 4);
    }

    Y_UNIT_TEST(TestQuoterHDRRParametersValidation) {
        TTestContext ctx;
        ctx.Setup();

        {
            NKikimrKesus::THierarchicalDRRResourceConfig cfg;
            cfg.SetMaxUnitsPerSecond(-100); // negative
            ctx.AddQuoterResource("/Res", cfg, Ydb::StatusIds::BAD_REQUEST);
        }

        {
            NKikimrKesus::THierarchicalDRRResourceConfig cfg;
            // no max units per second in root resource
            ctx.AddQuoterResource("/ResWithoutMaxUnitsPerSecond", cfg, Ydb::StatusIds::BAD_REQUEST);

            cfg.SetMaxUnitsPerSecond(1);
            ctx.AddQuoterResource("/ResWithMaxUnitsPerSecond", cfg, Ydb::StatusIds::SUCCESS);

            cfg.ClearMaxUnitsPerSecond(); // child can have no MaxUnitsPerSecond
            ctx.AddQuoterResource("/ResWithMaxUnitsPerSecond/ChildWithoutMaxUnitsPerSecond", cfg, Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(TestQuoterResourceModification) {
        TTestContext ctx;
        ctx.Setup();

        NKikimrKesus::THierarchicalDRRResourceConfig cfg1;
        cfg1.SetMaxUnitsPerSecond(100);

        NKikimrKesus::THierarchicalDRRResourceConfig cfg2;
        cfg2.SetMaxUnitsPerSecond(5);

        NKikimrKesus::THierarchicalDRRResourceConfig cfg3;
        cfg3.SetMaxUnitsPerSecond(42);

        ctx.AddQuoterResource("/Root", cfg1); // id=1
        ctx.AddQuoterResource("/Root/Q", cfg1); // id=2

        auto testBeforeModification = [&]() {
            const auto resources = ctx.DescribeQuoterResources({}, {}, true);
            UNIT_ASSERT_VALUES_EQUAL(resources.ResourcesSize(), 2);
            UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(0).GetResourcePath(), "Root");
            UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(1).GetResourcePath(), "Root/Q");

            UNIT_ASSERT_DOUBLES_EQUAL(
                resources.GetResources(0).GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond(),
                100, 0.001);
            UNIT_ASSERT_DOUBLES_EQUAL(
                resources.GetResources(1).GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond(),
                100, 0.001);
        };
        testBeforeModification();
        ctx.RebootTablet();
        testBeforeModification();

        auto testAfterModification = [&]() {
            const auto resources = ctx.DescribeQuoterResources({}, {}, true);
            UNIT_ASSERT_VALUES_EQUAL(resources.ResourcesSize(), 2);
            UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(0).GetResourcePath(), "Root");
            UNIT_ASSERT_STRINGS_EQUAL(resources.GetResources(1).GetResourcePath(), "Root/Q");

            UNIT_ASSERT_DOUBLES_EQUAL(
                resources.GetResources(0).GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond(),
                5, 0.001);
            UNIT_ASSERT_DOUBLES_EQUAL(
                resources.GetResources(1).GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond(),
                5, 0.001);
        };

        ctx.UpdateQuoterResource(1, cfg2);
        ctx.UpdateQuoterResource("/Root/Q", cfg2);
        testAfterModification();
        ctx.RebootTablet();
        testAfterModification();

        // test validation
        {
            NKikimrKesus::TStreamingQuoterResource req;
            *req.MutableHierarchicalDRRResourceConfig() = cfg3;

            ctx.UpdateQuoterResource(req, Ydb::StatusIds::BAD_REQUEST); // no resource path and id
            testAfterModification();
            ctx.RebootTablet();
            testAfterModification();

            req.SetResourcePath("?Invalid/Path?");
            ctx.UpdateQuoterResource(req, Ydb::StatusIds::BAD_REQUEST); // invalid path
            testAfterModification();
            ctx.RebootTablet();
            testAfterModification();

            req.SetResourcePath("/Root/Q");
            req.ClearAlgorithmSpecificConfig();
            ctx.UpdateQuoterResource(req, Ydb::StatusIds::BAD_REQUEST); // no config
            testAfterModification();
            ctx.RebootTablet();
            testAfterModification();

            req.SetResourcePath("/Root/P");
            *req.MutableHierarchicalDRRResourceConfig() = cfg3;
            ctx.UpdateQuoterResource(req, Ydb::StatusIds::NOT_FOUND); // no such resource
            testAfterModification();
            ctx.RebootTablet();
            testAfterModification();

            req.ClearResourcePath();
            req.SetResourceId(42);
            ctx.UpdateQuoterResource(req, Ydb::StatusIds::NOT_FOUND); // no such resource
            testAfterModification();
            ctx.RebootTablet();
            testAfterModification();
        }
    }

    Y_UNIT_TEST(TestQuoterResourceDeletion) {
        TTestContext ctx;
        ctx.Setup();

        ctx.AddQuoterResource("/Root", 1.0); // id=1
        ctx.AddQuoterResource("/Root/Q"); // id=2
        ctx.AddQuoterResource("/Root/Folder"); // id=3
        ctx.AddQuoterResource("/Root/Folder/Q1"); // id=4

        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeQuoterResources({}, {}, true).ResourcesSize(), 4);

        // validation
        {
            NKikimrKesus::TEvDeleteQuoterResource req;
            ctx.DeleteQuoterResource(req, Ydb::StatusIds::BAD_REQUEST); // no resource
            UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeQuoterResources({}, {}, true).ResourcesSize(), 4);

            req.SetResourcePath("?Invalid/Path?");
            ctx.DeleteQuoterResource(req, Ydb::StatusIds::BAD_REQUEST); // invalid path
            UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeQuoterResources({}, {}, true).ResourcesSize(), 4);

            req.SetResourcePath("/Root/Folder/NonexistingRes");
            ctx.DeleteQuoterResource(req, Ydb::StatusIds::NOT_FOUND);
            UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeQuoterResources({}, {}, true).ResourcesSize(), 4);

            req.ClearResourcePath();
            req.SetResourceId(100);
            ctx.DeleteQuoterResource(req, Ydb::StatusIds::NOT_FOUND);
            UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeQuoterResources({}, {}, true).ResourcesSize(), 4);

            ctx.DeleteQuoterResource(3, Ydb::StatusIds::BAD_REQUEST); // Folder is not empty
            UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeQuoterResources({}, {}, true).ResourcesSize(), 4);
        }

        ctx.DeleteQuoterResource("/Root/Folder/Q1"); // By name
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeQuoterResources({}, {}, true).ResourcesSize(), 3);
        ctx.RebootTablet();
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeQuoterResources({}, {}, true).ResourcesSize(), 3);

        ctx.DeleteQuoterResource(3); // By id
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeQuoterResources({}, {}, true).ResourcesSize(), 2);
        ctx.RebootTablet();
        UNIT_ASSERT_VALUES_EQUAL(ctx.DescribeQuoterResources({}, {}, true).ResourcesSize(), 2);
    }

    Y_UNIT_TEST(TestQuoterSubscribeOnResource) {
        TTestContext ctx;
        ctx.Setup();

        ctx.AddQuoterResource("/Q1", 10.0); // id=1
        ctx.AddQuoterResource("/Q2", 10.0); // id=2

        auto edge = ctx.Runtime->AllocateEdgeActor();
        auto client = ctx.Runtime->AllocateEdgeActor();
        auto ans = ctx.SubscribeOnResource(client, edge, "Q1", false);
        UNIT_ASSERT_VALUES_EQUAL(ans.GetResults(0).GetResourceId(), 1);

        auto client2 = ctx.Runtime->AllocateEdgeActor();
        ans = ctx.SubscribeOnResources(
            client2,
            edge,
            {
                TTestContext::TResourceConsumingInfo("/Q1", false),
                TTestContext::TResourceConsumingInfo("/Q2", false),
                TTestContext::TResourceConsumingInfo("/Q3", false, 0.0, Ydb::StatusIds::NOT_FOUND),
            }
        );
        UNIT_ASSERT_VALUES_EQUAL(ans.GetResults(0).GetResourceId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ans.GetResults(1).GetResourceId(), 2);
    }

    Y_UNIT_TEST(TestAllocatesResources) {
        TTestContext ctx;
        ctx.Setup();

        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetMaxUnitsPerSecond(100.0);
        ctx.AddQuoterResource("/Root", cfg);
        ctx.AddQuoterResource("/Root/Res"); // With inherited settings.

        auto edge = ctx.Runtime->AllocateEdgeActor();
        auto client = ctx.Runtime->AllocateEdgeActor();
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult = ctx.SubscribeOnResource(client, edge, "/Root/Res", false, 0);
        UNIT_ASSERT(subscribeResult.GetResults(0).HasEffectiveProps());
        ctx.UpdateConsumptionState(client, edge, subscribeResult.GetResults(0).GetResourceId(), true, 50.0);

        double allocated = 0.0;
        do {
            auto result = ctx.ExpectEdgeEvent<TEvKesus::TEvResourcesAllocated>(edge);
            UNIT_ASSERT_VALUES_EQUAL(result->Record.ResourcesInfoSize(), 1);
            const auto& info = result->Record.GetResourcesInfo(0);
            UNIT_ASSERT_VALUES_EQUAL(info.GetStateNotification().GetStatus(), Ydb::StatusIds::SUCCESS);
            allocated += info.GetAmount();
        } while (allocated < 49.99);
    }

    Y_UNIT_TEST(TestQuoterAccountResourcesOnDemand) {
        TTestContext ctx;
        ctx.Setup();

        TString billRecord;
        ctx.Runtime->SetObserverFunc([&billRecord](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NMetering::TEvMetering::EvWriteMeteringJson) {
                billRecord = ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        NKikimrKesus::TStreamingQuoterResource cfg;
        cfg.SetResourcePath("/Root");
        cfg.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(100.0);
        cfg.MutableHierarchicalDRRResourceConfig()->SetPrefetchCoefficient(300.0);
        cfg.MutableAccountingConfig()->SetEnabled(true);
        cfg.MutableAccountingConfig()->SetReportPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetAccountPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetCollectPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetVersion("version");
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetSchema("schema");
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetCloudId("cloud");
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetFolderId("folder");
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetResourceId("resource");
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetSourceId("source");
        cfg.MutableAccountingConfig()->MutableOnDemand()->MutableTags()->insert({"key", "value"});
        ctx.AddQuoterResource(cfg);
        ctx.AddQuoterResource("/Root/Res"); // With inherited settings.

        auto edge = ctx.Runtime->AllocateEdgeActor();
        auto client = ctx.Runtime->AllocateEdgeActor();
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult = ctx.SubscribeOnResource(client, edge, "/Root/Res", false, 0);

        TInstant start = ctx.Runtime->GetCurrentTime();
        TDuration interval = TConsumptionHistory::Interval();
        ctx.AccountResources(client, edge, subscribeResult.GetResults(0).GetResourceId(), start, interval, {50.0});

        if (billRecord.empty()) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&billRecord](IEventHandle&) -> bool {
                return !billRecord.empty();
            });
            ctx.Runtime->DispatchEvents(opts);
        }

        const TString expectedBillRecord = R"({"usage":{"start":0,"quantity":50,"finish":1,"unit":"request_unit","type":"delta"},"tags":{"key":"value"},"id":"Root-ondemand-0-1","cloud_id":"cloud","source_wt":5,"source_id":"source","resource_id":"resource","schema":"schema","folder_id":"folder","version":"version"})";

        UNIT_ASSERT_NO_DIFF(billRecord, expectedBillRecord + "\n");
    }

    Y_UNIT_TEST(TestQuoterAccountResourcesBurst) {
        TTestContext ctx;
        ctx.Setup();

        std::vector<TString> bills;
        ctx.Runtime->SetObserverFunc([&bills](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NMetering::TEvMetering::EvWriteMeteringJson) {
                bills.push_back(ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        NKikimrKesus::TStreamingQuoterResource cfg;
        cfg.SetResourcePath("/Root");
        cfg.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(300.0);
        cfg.MutableHierarchicalDRRResourceConfig()->SetPrefetchCoefficient(1.0);
        cfg.MutableAccountingConfig()->SetEnabled(true);
        cfg.MutableAccountingConfig()->SetReportPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetAccountPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetCollectPeriodSec(2);
        cfg.MutableAccountingConfig()->SetProvisionedUnitsPerSecond(100.0);
        cfg.MutableAccountingConfig()->SetProvisionedCoefficient(1.0);
        cfg.MutableAccountingConfig()->SetOvershootCoefficient(1.0);
        cfg.MutableAccountingConfig()->MutableProvisioned()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableProvisioned()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOvershoot()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOvershoot()->SetBillingPeriodSec(2);
        ctx.AddQuoterResource(cfg);
        ctx.AddQuoterResource("/Root/Res"); // With inherited settings.

        auto edge = ctx.Runtime->AllocateEdgeActor();
        auto client = ctx.Runtime->AllocateEdgeActor();
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult = ctx.SubscribeOnResource(client, edge, "/Root/Res", false, 0);

        TInstant start = ctx.Runtime->GetCurrentTime();
        TDuration interval = TConsumptionHistory::Interval();
        ctx.AccountResources(client, edge, subscribeResult.GetResults(0).GetResourceId(), start, interval, {600.0});

        if (bills.size() < 3) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&bills](IEventHandle&) -> bool {
                return bills.size() >= 3;
            });
            ctx.Runtime->DispatchEvents(opts);
        }

        UNIT_ASSERT_NO_DIFF(bills[0], TString(R"({"usage":{"start":0,"quantity":100,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-provisioned-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
        UNIT_ASSERT_NO_DIFF(bills[1], TString(R"({"usage":{"start":0,"quantity":200,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-ondemand-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
        UNIT_ASSERT_NO_DIFF(bills[2], TString(R"({"usage":{"start":0,"quantity":300,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-overshoot-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
    }

    Y_UNIT_TEST(TestQuoterAccountResourcesPaced) {
        TTestContext ctx;
        ctx.Setup();

        std::vector<TString> bills;
        ctx.Runtime->SetObserverFunc([&bills](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NMetering::TEvMetering::EvWriteMeteringJson) {
                bills.push_back(ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        NKikimrKesus::TStreamingQuoterResource cfg;
        cfg.SetResourcePath("/Root");
        cfg.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(300.0);
        cfg.MutableHierarchicalDRRResourceConfig()->SetPrefetchCoefficient(1.0);
        cfg.MutableAccountingConfig()->SetEnabled(true);
        cfg.MutableAccountingConfig()->SetReportPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetAccountPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetCollectPeriodSec(2);
        cfg.MutableAccountingConfig()->SetProvisionedUnitsPerSecond(100.0);
        cfg.MutableAccountingConfig()->SetProvisionedCoefficient(1.0);
        cfg.MutableAccountingConfig()->SetOvershootCoefficient(1.0);
        cfg.MutableAccountingConfig()->MutableProvisioned()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableProvisioned()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOvershoot()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOvershoot()->SetBillingPeriodSec(2);
        ctx.AddQuoterResource(cfg);
        ctx.AddQuoterResource("/Root/Res"); // With inherited settings.

        auto edge = ctx.Runtime->AllocateEdgeActor();
        auto client = ctx.Runtime->AllocateEdgeActor();
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult = ctx.SubscribeOnResource(client, edge, "/Root/Res", false, 0);

        TInstant start = ctx.Runtime->GetCurrentTime();
        TDuration interval = TConsumptionHistory::Interval();
        std::vector<double> values;
        for (int i = 0; i < 100; i++) {
            values.push_back(6);
        }
        ctx.AccountResources(client, edge, subscribeResult.GetResults(0).GetResourceId(), start, interval, std::move(values));

        if (bills.size() < 3) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&bills](IEventHandle&) -> bool {
                return bills.size() >= 3;
            });
            ctx.Runtime->DispatchEvents(opts);
        }

        UNIT_ASSERT_NO_DIFF(bills[0], TString(R"({"usage":{"start":0,"quantity":199,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-provisioned-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
        UNIT_ASSERT_NO_DIFF(bills[1], TString(R"({"usage":{"start":0,"quantity":398,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-ondemand-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
        UNIT_ASSERT_NO_DIFF(bills[2], TString(R"({"usage":{"start":0,"quantity":3,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-overshoot-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
    }

    Y_UNIT_TEST(TestQuoterAccountResourcesAggregateClients) {
        TTestContext ctx;
        ctx.Setup();

        std::vector<TString> bills;
        ctx.Runtime->SetObserverFunc([&bills](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NMetering::TEvMetering::EvWriteMeteringJson) {
                bills.push_back(ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        NKikimrKesus::TStreamingQuoterResource cfg;
        cfg.SetResourcePath("/Root");
        cfg.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(300.0);
        cfg.MutableHierarchicalDRRResourceConfig()->SetPrefetchCoefficient(1.0);
        cfg.MutableAccountingConfig()->SetEnabled(true);
        cfg.MutableAccountingConfig()->SetReportPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetAccountPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetCollectPeriodSec(2);
        cfg.MutableAccountingConfig()->SetProvisionedUnitsPerSecond(100.0);
        cfg.MutableAccountingConfig()->SetProvisionedCoefficient(1.0);
        cfg.MutableAccountingConfig()->SetOvershootCoefficient(1.0);
        cfg.MutableAccountingConfig()->MutableProvisioned()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableProvisioned()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOvershoot()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOvershoot()->SetBillingPeriodSec(2);
        ctx.AddQuoterResource(cfg);

        auto edge = ctx.Runtime->AllocateEdgeActor();
        auto client1 = ctx.Runtime->AllocateEdgeActor();
        auto client2 = ctx.Runtime->AllocateEdgeActor();
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult1 = ctx.SubscribeOnResource(client1, edge, "/Root", false, 0);
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult2 = ctx.SubscribeOnResource(client2, edge, "/Root", false, 0);

        TInstant start = ctx.Runtime->GetCurrentTime();
        TDuration interval = TConsumptionHistory::Interval();
        std::vector<double> values1;
        std::vector<double> values2;
        for (int i = 0; i < 100; i++) {
            values1.push_back(3);
            values2.push_back(3);
        }
        ctx.AccountResources(client1, edge, subscribeResult1.GetResults(0).GetResourceId(), start, interval, std::move(values1));
        ctx.AccountResources(client2, edge, subscribeResult2.GetResults(0).GetResourceId(), start, interval, std::move(values2));

        if (bills.size() < 3) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&bills](IEventHandle&) -> bool {
                return bills.size() >= 3;
            });
            ctx.Runtime->DispatchEvents(opts);
        }

        UNIT_ASSERT_NO_DIFF(bills[0], TString(R"({"usage":{"start":0,"quantity":199,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-provisioned-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
        UNIT_ASSERT_NO_DIFF(bills[1], TString(R"({"usage":{"start":0,"quantity":398,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-ondemand-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
        UNIT_ASSERT_NO_DIFF(bills[2], TString(R"({"usage":{"start":0,"quantity":3,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-overshoot-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
    }

    Y_UNIT_TEST(TestQuoterAccountResourcesAggregateResources) {
        TTestContext ctx;
        ctx.Setup();

        std::vector<TString> bills;
        ctx.Runtime->SetObserverFunc([&bills](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NMetering::TEvMetering::EvWriteMeteringJson) {
                bills.push_back(ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        NKikimrKesus::TStreamingQuoterResource cfg;
        cfg.SetResourcePath("/Root");
        cfg.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(300.0);
        cfg.MutableHierarchicalDRRResourceConfig()->SetPrefetchCoefficient(1.0);
        cfg.MutableAccountingConfig()->SetEnabled(true);
        cfg.MutableAccountingConfig()->SetReportPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetAccountPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetCollectPeriodSec(2);
        cfg.MutableAccountingConfig()->SetProvisionedUnitsPerSecond(100.0);
        cfg.MutableAccountingConfig()->SetProvisionedCoefficient(1.0);
        cfg.MutableAccountingConfig()->SetOvershootCoefficient(1.0);
        cfg.MutableAccountingConfig()->MutableProvisioned()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableProvisioned()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOvershoot()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOvershoot()->SetBillingPeriodSec(2);
        ctx.AddQuoterResource(cfg);
        ctx.AddQuoterResource("/Root/Res1"); // With inherited settings.
        ctx.AddQuoterResource("/Root/Res2"); // With inherited settings.

        auto edge = ctx.Runtime->AllocateEdgeActor();
        auto client = ctx.Runtime->AllocateEdgeActor();
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult1 = ctx.SubscribeOnResource(client, edge, "/Root/Res1", false, 0);
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult2 = ctx.SubscribeOnResource(client, edge, "/Root/Res2", false, 0);

        TInstant start = ctx.Runtime->GetCurrentTime();
        TDuration interval = TConsumptionHistory::Interval();
        std::vector<double> values1;
        std::vector<double> values2;
        for (int i = 0; i < 100; i++) {
            values1.push_back(3);
            values2.push_back(3);
        }
        ctx.AccountResources(client, edge, {
            {subscribeResult1.GetResults(0).GetResourceId(), start, interval, std::move(values1)},
            {subscribeResult2.GetResults(0).GetResourceId(), start, interval, std::move(values2)}
        });

        if (bills.size() < 3) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&bills](IEventHandle&) -> bool {
                return bills.size() >= 3;
            });
            ctx.Runtime->DispatchEvents(opts);
        }

        UNIT_ASSERT_NO_DIFF(bills[0], TString(R"({"usage":{"start":0,"quantity":199,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-provisioned-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
        UNIT_ASSERT_NO_DIFF(bills[1], TString(R"({"usage":{"start":0,"quantity":398,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-ondemand-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
        UNIT_ASSERT_NO_DIFF(bills[2], TString(R"({"usage":{"start":0,"quantity":3,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-overshoot-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
    }

    Y_UNIT_TEST(TestQuoterAccountResourcesDeduplicateClient) {
        TTestContext ctx;
        ctx.Setup();

        std::vector<TString> bills;
        ctx.Runtime->SetObserverFunc([&bills](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NMetering::TEvMetering::EvWriteMeteringJson) {
                bills.push_back(ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        NKikimrKesus::TStreamingQuoterResource cfg;
        cfg.SetResourcePath("/Root");
        cfg.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(300.0);
        cfg.MutableHierarchicalDRRResourceConfig()->SetPrefetchCoefficient(1.0);
        cfg.MutableAccountingConfig()->SetEnabled(true);
        cfg.MutableAccountingConfig()->SetReportPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetAccountPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetCollectPeriodSec(2);
        cfg.MutableAccountingConfig()->SetProvisionedUnitsPerSecond(100.0);
        cfg.MutableAccountingConfig()->SetProvisionedCoefficient(1.0);
        cfg.MutableAccountingConfig()->SetOvershootCoefficient(1.0);
        cfg.MutableAccountingConfig()->MutableProvisioned()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableProvisioned()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOvershoot()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOvershoot()->SetBillingPeriodSec(2);
        ctx.AddQuoterResource(cfg);

        auto edge = ctx.Runtime->AllocateEdgeActor();
        auto client = ctx.Runtime->AllocateEdgeActor();
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult = ctx.SubscribeOnResource(client, edge, "/Root", false, 0);

        TInstant start = ctx.Runtime->GetCurrentTime();
        TDuration interval = TConsumptionHistory::Interval();
        std::vector<double> values1;
        std::vector<double> values2;
        for (int i = 0; i < 100; i++) {
            values1.push_back(6);
            values2.push_back(6);
        }
        ctx.AccountResources(client, edge, subscribeResult.GetResults(0).GetResourceId(), start, interval, std::move(values1));
        ctx.AccountResources(client, edge, subscribeResult.GetResults(0).GetResourceId(), start, interval, std::move(values2));

        if (bills.size() < 3) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&bills](IEventHandle&) -> bool {
                return bills.size() >= 3;
            });
            ctx.Runtime->DispatchEvents(opts);
        }

        UNIT_ASSERT_NO_DIFF(bills[0], TString(R"({"usage":{"start":0,"quantity":199,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-provisioned-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
        UNIT_ASSERT_NO_DIFF(bills[1], TString(R"({"usage":{"start":0,"quantity":398,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-ondemand-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
        UNIT_ASSERT_NO_DIFF(bills[2], TString(R"({"usage":{"start":0,"quantity":3,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-overshoot-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})") + "\n");
    }

    Y_UNIT_TEST(TestQuoterAccountResourcesForgetClient) {
        TTestContext ctx;
        ctx.Setup();

        std::vector<TString> bills;
        ctx.Runtime->SetObserverFunc([&bills](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NMetering::TEvMetering::EvWriteMeteringJson) {
                bills.push_back(ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        NKikimrKesus::TStreamingQuoterResource cfg;
        cfg.SetResourcePath("/Root");
        cfg.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(300.0);
        cfg.MutableHierarchicalDRRResourceConfig()->SetPrefetchCoefficient(1.0);
        cfg.MutableAccountingConfig()->SetEnabled(true);
        cfg.MutableAccountingConfig()->SetReportPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetAccountPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetCollectPeriodSec(2);
        cfg.MutableAccountingConfig()->SetProvisionedUnitsPerSecond(100.0);
        cfg.MutableAccountingConfig()->SetProvisionedCoefficient(1.0);
        cfg.MutableAccountingConfig()->SetOvershootCoefficient(1.0);
        cfg.MutableAccountingConfig()->MutableProvisioned()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableProvisioned()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOvershoot()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOvershoot()->SetBillingPeriodSec(2);
        ctx.AddQuoterResource(cfg);

        for (int i = 0; i < 3; i++) {
            auto edge = ctx.Runtime->AllocateEdgeActor();
            auto client = ctx.Runtime->AllocateEdgeActor();
            const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult = ctx.SubscribeOnResource(client, edge, "/Root", false, 0);

            TInstant begin = TInstant::Seconds(2) + i * TDuration::Seconds(6);
            TDuration interval = TConsumptionHistory::Interval();
            std::vector<double> values;
            for (int i = 0; i < 100; i++) {
                values.push_back(6);
            }
            ctx.AccountResources(client, edge, subscribeResult.GetResults(0).GetResourceId(), begin, interval, std::move(values));

            if (bills.size() < 3) {
                TDispatchOptions opts;
                opts.FinalEvents.emplace_back([&bills](IEventHandle&) -> bool {
                    return bills.size() >= 3;
                });
                ctx.Runtime->DispatchEvents(opts);
            }

            int start = 2 + i * 6;
            int finish = start + 1;
            int source_wt = start + 5;
            UNIT_ASSERT_NO_DIFF(bills[0], Sprintf(R"({"usage":{"start":%d,"quantity":199,"finish":%d,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-provisioned-%d-%d","cloud_id":"","source_wt":%d,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})", start, finish, start, finish, source_wt) + "\n");
            UNIT_ASSERT_NO_DIFF(bills[1], Sprintf(R"({"usage":{"start":%d,"quantity":398,"finish":%d,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-ondemand-%d-%d","cloud_id":"","source_wt":%d,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})", start, finish, start, finish, source_wt) + "\n");
            UNIT_ASSERT_NO_DIFF(bills[2], Sprintf(R"({"usage":{"start":%d,"quantity":3,"finish":%d,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-overshoot-%d-%d","cloud_id":"","source_wt":%d,"source_id":"","resource_id":"","schema":"","folder_id":"","version":""})", start, finish, start, finish, source_wt) + "\n");
            bills.clear();
        }
    }

    Y_UNIT_TEST(TestQuoterAccountLabels) {
        TTestContext ctx;
        ctx.Setup();

        TString billRecord;
        ctx.Runtime->SetObserverFunc([&billRecord](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NMetering::TEvMetering::EvWriteMeteringJson) {
                billRecord = ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        NKikimrKesus::TStreamingQuoterResource cfg;
        cfg.SetResourcePath("/Root");
        cfg.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(100.0);
        cfg.MutableHierarchicalDRRResourceConfig()->SetPrefetchCoefficient(300.0);
        cfg.MutableAccountingConfig()->SetEnabled(true);
        cfg.MutableAccountingConfig()->SetReportPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetAccountPeriodMs(1000);
        cfg.MutableAccountingConfig()->SetCollectPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetEnabled(true);
        cfg.MutableAccountingConfig()->MutableOnDemand()->SetBillingPeriodSec(2);
        cfg.MutableAccountingConfig()->MutableOnDemand()->MutableLabels()->insert({"k1", "v1"});
        cfg.MutableAccountingConfig()->MutableOnDemand()->MutableLabels()->insert({"k2", "v2"});
        ctx.AddQuoterResource(cfg);

        auto edge = ctx.Runtime->AllocateEdgeActor();
        auto client = ctx.Runtime->AllocateEdgeActor();
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult = ctx.SubscribeOnResource(client, edge, "/Root", false, 0);

        TInstant start = ctx.Runtime->GetCurrentTime();
        TDuration interval = TConsumptionHistory::Interval();
        ctx.AccountResources(client, edge, subscribeResult.GetResults(0).GetResourceId(), start, interval, {50.0});

        if (billRecord.empty()) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&billRecord](IEventHandle&) -> bool {
                return !billRecord.empty();
            });
            ctx.Runtime->DispatchEvents(opts);
        }

        const TString expectedBillRecord = R"({"usage":{"start":0,"quantity":50,"finish":1,"unit":"request_unit","type":"delta"},"tags":{},"id":"Root-ondemand-0-1","cloud_id":"","source_wt":5,"source_id":"","resource_id":"","schema":"","labels":{"k2":"v2","k1":"v1"},"folder_id":"","version":""})";

        UNIT_ASSERT_NO_DIFF(billRecord, expectedBillRecord + "\n");
    }

    Y_UNIT_TEST(TestPassesUpdatedPropsToSession) {
        TTestContext ctx;
        ctx.Setup();

        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetMaxUnitsPerSecond(100.0);
        ctx.AddQuoterResource("/Root", cfg);
        ctx.AddQuoterResource("/Root/Res");

        auto edge = ctx.Runtime->AllocateEdgeActor();
        auto client = ctx.Runtime->AllocateEdgeActor();
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult = ctx.SubscribeOnResource(client, edge, "/Root/Res", false, 0);
        UNIT_ASSERT(subscribeResult.GetResults(0).HasEffectiveProps());

        // update
        cfg.SetMaxUnitsPerSecond(150.0);
        ctx.UpdateQuoterResource("/Root", cfg);

        ctx.UpdateConsumptionState(client, edge, subscribeResult.GetResults(0).GetResourceId(), true, 50.0);

        auto result = ctx.ExpectEdgeEvent<TEvKesus::TEvResourcesAllocated>(edge);
        UNIT_ASSERT_VALUES_EQUAL(result->Record.ResourcesInfoSize(), 1);
        const auto& info = result->Record.GetResourcesInfo(0);
        UNIT_ASSERT_VALUES_EQUAL(info.GetStateNotification().GetStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_DOUBLES_EQUAL(info.GetEffectiveProps().GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond(), 150.0, 0.001);
    }

    Y_UNIT_TEST(TestGetQuoterResourceCounters) {
        TTestContext ctx;
        ctx.Setup();
        //ctx.Runtime->SetLogPriority(NKikimrServices::KESUS_TABLET, NLog::PRI_TRACE);

        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetMaxUnitsPerSecond(1000.0);
        ctx.AddQuoterResource("/Root1", cfg);
        ctx.AddQuoterResource("/Root1/Res");

        ctx.AddQuoterResource("/Root2", cfg);
        ctx.AddQuoterResource("/Root2/Res");
        ctx.AddQuoterResource("/Root2/Res/Subres");

        auto edge = ctx.Runtime->AllocateEdgeActor();
        auto client = ctx.Runtime->AllocateEdgeActor();
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult = ctx.SubscribeOnResource(client, edge, "/Root1/Res", true, 300);
        UNIT_ASSERT(subscribeResult.GetResults(0).HasEffectiveProps());

        auto WaitAllocated = [&ctx](const TActorId edge, double amount) -> double {
            double allocated = 0.0;
            do {
                auto result = ctx.ExpectEdgeEvent<TEvKesus::TEvResourcesAllocated>(edge);
                UNIT_ASSERT_VALUES_EQUAL(result->Record.ResourcesInfoSize(), 1);
                const auto& info = result->Record.GetResourcesInfo(0);
                UNIT_ASSERT_VALUES_EQUAL(info.GetStateNotification().GetStatus(), Ydb::StatusIds::SUCCESS);
                allocated += info.GetAmount();
            } while (allocated < amount - 0.01);
            return allocated;
        };

        // Wait allocation
        const double allocated1First = WaitAllocated(edge, 300);

        auto CheckCountersValues = [&ctx](ui64 v1, ui64 v2) {
            auto counters = ctx.GetQuoterResourceCounters();
            UNIT_ASSERT_VALUES_EQUAL_C(counters.ResourceCountersSize(), 5, counters);
            UNIT_ASSERT_VALUES_EQUAL(counters.GetResourceCounters(0).GetResourcePath(), "Root1");
            UNIT_ASSERT_VALUES_EQUAL(counters.GetResourceCounters(1).GetResourcePath(), "Root1/Res");
            UNIT_ASSERT_VALUES_EQUAL(counters.GetResourceCounters(2).GetResourcePath(), "Root2");
            UNIT_ASSERT_VALUES_EQUAL(counters.GetResourceCounters(3).GetResourcePath(), "Root2/Res");
            UNIT_ASSERT_VALUES_EQUAL(counters.GetResourceCounters(4).GetResourcePath(), "Root2/Res/Subres");
            UNIT_ASSERT_VALUES_EQUAL_C(counters.GetResourceCounters(0).GetAllocated(), v1, counters);
            UNIT_ASSERT_VALUES_EQUAL_C(counters.GetResourceCounters(1).GetAllocated(), v1, counters);
            UNIT_ASSERT_VALUES_EQUAL_C(counters.GetResourceCounters(2).GetAllocated(), v2, counters);
            UNIT_ASSERT_VALUES_EQUAL_C(counters.GetResourceCounters(3).GetAllocated(), v2, counters);
            UNIT_ASSERT_VALUES_EQUAL_C(counters.GetResourceCounters(4).GetAllocated(), v2, counters);
        };

        CheckCountersValues(static_cast<ui64>(allocated1First), 0);

        auto edge2 = ctx.Runtime->AllocateEdgeActor();
        auto client2 = ctx.Runtime->AllocateEdgeActor();
        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult2First = ctx.SubscribeOnResource(client2, edge2, "/Root2/Res/Subres", true, 200);
        UNIT_ASSERT(subscribeResult2First.GetResults(0).HasEffectiveProps());

        const double allocated2First = WaitAllocated(edge2, 200);
        CheckCountersValues(static_cast<ui64>(allocated1First), static_cast<ui64>(allocated2First));

        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult1Second = ctx.SubscribeOnResource(client, edge, "/Root1/Res", true, 20);
        UNIT_ASSERT(subscribeResult1Second.GetResults(0).HasEffectiveProps());

        const NKikimrKesus::TEvSubscribeOnResourcesResult subscribeResult2Second = ctx.SubscribeOnResource(client2, edge2, "/Root2/Res/Subres", true, 50);
        UNIT_ASSERT(subscribeResult2Second.GetResults(0).HasEffectiveProps());

        const double allocated1Second = WaitAllocated(edge, 20);
        const double allocated2Second = WaitAllocated(edge2, 50);

        CheckCountersValues(static_cast<ui64>(allocated1First + allocated1Second), static_cast<ui64>(allocated2First + allocated2Second));
    }

    Y_UNIT_TEST(TestStopResourceAllocationWhenPipeDestroyed) {
        TTestContext ctx;
        ctx.Setup();

        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetMaxUnitsPerSecond(100.0);
        ctx.AddQuoterResource("Root", cfg);

        auto CreateSession = [&]() -> std::pair<TActorId, TActorId> {
            TActorId edge = ctx.Runtime->AllocateEdgeActor();
            const TActorId sessionPipe = ctx.Runtime->ConnectToPipe(ctx.TabletId, edge, 0, GetPipeConfigWithRetries());
            auto req = MakeHolder<TEvKesus::TEvSubscribeOnResources>();
            ActorIdToProto(edge, req->Record.MutableActorID());
            auto* reqRes = req->Record.AddResources();
            reqRes->SetResourcePath("Root");
            reqRes->SetStartConsuming(true);
            reqRes->SetInitialAmount(std::numeric_limits<double>::infinity());
            ctx.Runtime->SendToPipe(
                ctx.TabletId,
                edge,
                req.Release(),
                0,
                GetPipeConfigWithRetries(),
                sessionPipe,
                0);
            return std::make_pair(edge, sessionPipe);
        };

        const std::pair<TActorId, TActorId> edgeAndSession1 = CreateSession();
        const std::pair<TActorId, TActorId> edgeAndSession2 = CreateSession();

        auto WaitAllocation = [&](TActorId edge, double expectedAmount) {
            size_t attempts = 30;
            do {
                auto result = ctx.ExpectEdgeEvent<TEvKesus::TEvResourcesAllocated>(edge);
                UNIT_ASSERT_VALUES_EQUAL(result->Record.ResourcesInfoSize(), 1);
                const auto& info = result->Record.GetResourcesInfo(0);
                UNIT_ASSERT_VALUES_EQUAL(info.GetStateNotification().GetStatus(), Ydb::StatusIds::SUCCESS);
                if (std::abs(info.GetAmount() - expectedAmount) <= 0.01) {
                    break; // OK
                }
            } while (--attempts);
            UNIT_ASSERT(attempts);
        };

        WaitAllocation(edgeAndSession1.first, 5);
        WaitAllocation(edgeAndSession2.first, 5);

        // Kill pipe and then session must be deactivated on kesus.
        ctx.Runtime->Send(new IEventHandle(edgeAndSession2.second, edgeAndSession2.first, new TEvents::TEvPoisonPill()));
        WaitAllocation(edgeAndSession1.first, 10); // Now first session is the only active session and it receives all resource.
    }
}

}
}
