#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/distributed_lock.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/session_pool.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

#include "coordination_grpc_mock.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/system/hostname.h>
#include <util/system/thread.h>

#include <mutex>
#include <optional>
#include <limits>

using namespace NYdb;
using namespace NYdb::NCoordination;
using namespace NCoordinationTest;

namespace {

constexpr TDuration TEST_TIMEOUT = TDuration::MilliSeconds(500);
constexpr const char* COORD_PATH = "/Some/CoordPath";
constexpr const char* SEMAPHORE_NAME = "test-lock";

struct TTestEnv {
    TPortManager PortManager;
    TMockDiscoveryService DiscoveryService;
    TMockCoordinationService CoordinationService;
    std::unique_ptr<grpc::Server> CoordinationServer;
    std::unique_ptr<grpc::Server> DiscoveryServer;
    std::optional<TDriver> Driver;
    std::optional<TClient> Client;

    TTestEnv() {
        ui16 coordinationPort = PortManager.GetPort();
        CoordinationServer = StartGrpcServer(
            TStringBuilder() << "0.0.0.0:" << coordinationPort,
            CoordinationService);

        auto& dbResult = DiscoveryService.MockResults["/Root/My/DB"];
        auto* endpoint = dbResult.add_endpoints();
        endpoint->set_address("localhost");
        endpoint->set_port(coordinationPort);

        ui16 discoveryPort = PortManager.GetPort();
        DiscoveryServer = StartGrpcServer(
            TStringBuilder() << "0.0.0.0:" << discoveryPort,
            DiscoveryService);

        Driver.emplace(TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << discoveryPort)
            .SetDatabase("/Root/My/DB"));
        Client.emplace(*Driver);
    }

    ~TTestEnv() {
        Client.reset();
        Driver.reset();
        if (CoordinationServer) {
            CoordinationServer->Shutdown();
            CoordinationServer->Wait();
        }
        if (DiscoveryServer) {
            DiscoveryServer->Shutdown();
            DiscoveryServer->Wait();
        }
    }
};

TDistributedLock MakeLock(TTestEnv& env, const char* name = SEMAPHORE_NAME) {
    return env.Client->CreateDistributedLock(
        TDistributedLockSettings().Path(COORD_PATH).Name(name).Timeout(TEST_TIMEOUT));
}

TCoordinationSessionPool MakePool(TTestEnv& env, size_t size = 1) {
    return env.Client->CreateSessionPool(
        COORD_PATH,
        TCoordinationSessionPoolSettings()
            .PoolSize(size)
            .SessionSettings(TSessionSettings().Timeout(TEST_TIMEOUT)));
}

} // namespace

Y_UNIT_TEST_SUITE(DistributedLock) {

    Y_UNIT_TEST(LockUnlock) {
        TTestEnv env;
        auto lock = MakeLock(env);
        lock.lock();
        lock.unlock();
    }

    Y_UNIT_TEST(LockGuard) {
        TTestEnv env;
        auto lock = MakeLock(env);
        std::lock_guard guard(lock);
    }

    Y_UNIT_TEST(TryLockSuccess) {
        TTestEnv env;
        auto lock = MakeLock(env);
        UNIT_ASSERT(lock.try_lock());
        lock.unlock();
    }

    Y_UNIT_TEST(TryLockFailsWhenHeld) {
        TTestEnv env;
        auto lockA = MakeLock(env);
        auto lockB = MakeLock(env);
        lockA.lock();
        env.CoordinationService.LastAcquireTimeoutMillis.store(std::numeric_limits<uint64_t>::max());
        UNIT_ASSERT(!lockB.try_lock());
        UNIT_ASSERT_VALUES_EQUAL(env.CoordinationService.LastAcquireTimeoutMillis.load(), 0u);
        lockA.unlock();
    }

    Y_UNIT_TEST(LockThrowsOnAcquireFailure) {
        TTestEnv env;
        env.CoordinationService.FailNextAcquire.store(true);
        auto lock = MakeLock(env);
        UNIT_ASSERT_EXCEPTION(lock.lock(), TYdbLockException);
        UNIT_ASSERT(!lock.GetStopToken().stop_requested());
        lock.lock();
        lock.unlock();
    }

    Y_UNIT_TEST(UnlockFailureNotifiesStopToken) {
        TTestEnv env;
        auto lock = MakeLock(env);
        lock.lock();
        auto token = lock.GetStopToken();
        env.CoordinationService.FailNextRelease.store(true);
        lock.unlock();
        UNIT_ASSERT(token.stop_requested());
    }

    Y_UNIT_TEST(SessionPoolStartsConfiguredSessions) {
        TTestEnv env;
        auto pool = MakePool(env, 3);
        UNIT_ASSERT_VALUES_EQUAL(env.CoordinationService.StartedSessions.load(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 3u);
    }

    Y_UNIT_TEST(SessionPoolGetAnyDrainsPool) {
        TTestEnv env;
        auto pool = MakePool(env, 2);

        auto sessionA = pool.GetAny();
        UNIT_ASSERT(sessionA);
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1u);

        auto sessionB = pool.GetAny();
        UNIT_ASSERT(sessionB);
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 0u);

        UNIT_ASSERT(!pool.GetAny());

        pool.Return(std::move(*sessionA));
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1u);
        pool.Return(std::move(*sessionB));
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 2u);
    }

    Y_UNIT_TEST(SessionPoolReturnMakesSessionAvailable) {
        TTestEnv env;
        auto pool = MakePool(env);

        auto session = pool.GetAny();
        UNIT_ASSERT(session);
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 0u);

        pool.Return(std::move(*session));
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1u);

        auto returnedSession = pool.GetAny();
        UNIT_ASSERT(returnedSession);
        pool.Return(std::move(*returnedSession));
    }

    Y_UNIT_TEST(SessionPoolReplacePreservesCapacity) {
        TTestEnv env;
        auto pool = MakePool(env);

        auto session = pool.GetAny();
        UNIT_ASSERT(session);
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 0u);

        UNIT_ASSERT(pool.Replace(std::move(*session)));
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(env.CoordinationService.StartedSessions.load(), 2u);
    }

    Y_UNIT_TEST(SessionPoolNotifiesCheckedOutConsumerOnSessionLoss) {
        TTestEnv env;
        auto pool = MakePool(env);
        auto lostPromise = NThreading::NewPromise();
        auto lostFuture = lostPromise.GetFuture();

        auto session = pool.GetAny([lostPromise]() mutable {
            lostPromise.SetValue();
        });
        UNIT_ASSERT(session);

        env.CoordinationService.MaxPingResponses.store(2);
        UNIT_ASSERT(lostFuture.Wait(TDuration::Seconds(5)));

        env.CoordinationService.MaxPingResponses.store(0);
        pool.Return(std::move(*session));
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1u);
    }

    Y_UNIT_TEST(PooledLockUnlock) {
        TTestEnv env;
        auto pool = MakePool(env);
        auto lock = pool.CreateDistributedLock(
            TDistributedLockSettings().Name(SEMAPHORE_NAME).Timeout(TEST_TIMEOUT));
        lock.lock();
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 0u);
        lock.unlock();
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1u);
    }

    Y_UNIT_TEST(PooledTryLockFailsWhenPoolIsEmpty) {
        TTestEnv env;
        auto pool = MakePool(env);
        auto lockA = pool.CreateDistributedLock(
            TDistributedLockSettings().Name(SEMAPHORE_NAME).Timeout(TEST_TIMEOUT));
        auto lockB = pool.CreateDistributedLock(
            TDistributedLockSettings().Name("another-lock").Timeout(TEST_TIMEOUT));

        lockA.lock();
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 0u);
        UNIT_ASSERT(!lockB.try_lock());
        lockA.unlock();
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1u);

        UNIT_ASSERT(lockB.try_lock());
        lockB.unlock();
    }

    Y_UNIT_TEST(PooledLockReusesReturnedSession) {
        TTestEnv env;
        auto pool = MakePool(env);
        auto lockA = pool.CreateDistributedLock(
            TDistributedLockSettings().Name(SEMAPHORE_NAME).Timeout(TEST_TIMEOUT));
        auto lockB = pool.CreateDistributedLock(
            TDistributedLockSettings().Name("another-lock").Timeout(TEST_TIMEOUT));

        lockA.lock();
        lockA.unlock();

        UNIT_ASSERT(lockB.try_lock());
        lockB.unlock();
        UNIT_ASSERT_VALUES_EQUAL(env.CoordinationService.StartedSessions.load(), 1u);
    }

    Y_UNIT_TEST(PooledLockReplacesIdleLostSession) {
        TTestEnv env;
        env.CoordinationService.StopNextSessionAfterStart.store(true);
        auto stoppedPromise = NThreading::NewPromise();
        auto stoppedFuture = stoppedPromise.GetFuture();
        auto pool = env.Client->CreateSessionPool(
            COORD_PATH,
            TCoordinationSessionPoolSettings()
                .PoolSize(1)
                .SessionSettings(TSessionSettings()
                    .Timeout(TEST_TIMEOUT)
                    .OnStopped([stoppedPromise]() mutable {
                        stoppedPromise.SetValue();
                    })));

        UNIT_ASSERT(stoppedFuture.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(env.CoordinationService.StartedSessions.load(), 1u);

        auto lock = pool.CreateDistributedLock(
            TDistributedLockSettings().Name(SEMAPHORE_NAME).Timeout(TEST_TIMEOUT));
        lock.lock();
        lock.unlock();
        UNIT_ASSERT_VALUES_EQUAL(env.CoordinationService.StartedSessions.load(), 2u);
    }

    Y_UNIT_TEST(PooledLockAcquireFailureReturnsUsableSession) {
        TTestEnv env;
        auto pool = MakePool(env);
        auto lock = pool.CreateDistributedLock(
            TDistributedLockSettings().Name(SEMAPHORE_NAME).Timeout(TEST_TIMEOUT));

        env.CoordinationService.FailNextAcquire.store(true);
        UNIT_ASSERT_EXCEPTION(lock.lock(), TYdbLockException);
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1u);

        lock.lock();
        lock.unlock();
    }

    Y_UNIT_TEST(PooledLockReleaseFailureReplacesSession) {
        TTestEnv env;
        auto pool = MakePool(env);
        auto lock = pool.CreateDistributedLock(
            TDistributedLockSettings().Name(SEMAPHORE_NAME).Timeout(TEST_TIMEOUT));

        lock.lock();
        auto token = lock.GetStopToken();
        env.CoordinationService.FailNextRelease.store(true);
        lock.unlock();
        UNIT_ASSERT(token.stop_requested());
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1u);

        lock.lock();
        UNIT_ASSERT(!lock.GetStopToken().stop_requested());
        lock.unlock();
    }

    Y_UNIT_TEST(PooledLockSessionExpiryWhileHoldingLock) {
        TTestEnv env;
        auto pool = MakePool(env);
        auto lock = pool.CreateDistributedLock(
            TDistributedLockSettings().Name(SEMAPHORE_NAME).Timeout(TEST_TIMEOUT));

        lock.lock();
        env.CoordinationService.MaxPingResponses.store(2);
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(5);
        while (!lock.GetStopToken().stop_requested() && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT(lock.GetStopToken().stop_requested());

        env.CoordinationService.MaxPingResponses.store(0);
        lock.unlock();
        UNIT_ASSERT_VALUES_EQUAL(pool.Size(), 1u);
    }

    Y_UNIT_TEST(OwnerDataIsHostName) {
        TTestEnv env;
        auto lock = MakeLock(env);
        lock.lock();

        auto sessionResult = env.Client->StartSession(COORD_PATH).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(sessionResult.GetStatus(), EStatus::SUCCESS, sessionResult.GetIssues().ToString());
        auto session = sessionResult.ExtractResult();

        auto describeResult = session.DescribeSemaphore(
            SEMAPHORE_NAME,
            TDescribeSemaphoreSettings().IncludeOwners(true)
        ).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(describeResult.GetStatus(), EStatus::SUCCESS, describeResult.GetIssues().ToString());

        const auto& description = describeResult.GetResult();
        UNIT_ASSERT_VALUES_EQUAL(description.GetOwners().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(description.GetOwners()[0].GetData(), FQDNHostName());

        lock.unlock();
    }

    Y_UNIT_TEST(GetStopTokenInitiallyValid) {
        TTestEnv env;
        auto lock = MakeLock(env);
        UNIT_ASSERT(!lock.GetStopToken().stop_requested());
    }

    Y_UNIT_TEST(SessionExpiryWhileHoldingLock) {
        TTestEnv env;
        env.CoordinationService.MaxPingResponses.store(2);
        auto lock = MakeLock(env);
        lock.lock();
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(5);
        while (!lock.GetStopToken().stop_requested() && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT(lock.GetStopToken().stop_requested());
        lock.unlock();
        lock.lock();
        UNIT_ASSERT(!lock.GetStopToken().stop_requested());
        lock.unlock();
    }

}
