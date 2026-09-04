#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/distributed_lock.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

#include "coordination_grpc_mock.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/system/hostname.h>
#include <util/system/thread.h>

#include <mutex>
#include <optional>

using namespace NYdb;
using namespace NYdb::NCoordination;
using namespace NCoordinationTest;

namespace {

constexpr TDuration TEST_TIMEOUT = TDuration::MilliSeconds(500);
constexpr const char* COORD_PATH = "/Some/CoordPath";
constexpr const char* SEMAPHORE_NAME = "test-lock";
constexpr const char* ANOTHER_SEMAPHORE_NAME = "another-test-lock";

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

TSession MakeSession(TTestEnv& env) {
    auto sessionResult = env.Client->StartSession(
        COORD_PATH,
        TSessionSettings().Timeout(TEST_TIMEOUT)
    ).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(sessionResult.GetStatus(), EStatus::SUCCESS, sessionResult.GetIssues().ToString());
    return sessionResult.ExtractResult();
}

TDistributedLock MakeLock(TSession session, const char* name = SEMAPHORE_NAME) {
    return session.CreateDistributedLock(
        TDistributedLockSettings().Name(name).Timeout(TEST_TIMEOUT));
}

} // namespace

Y_UNIT_TEST_SUITE(DistributedLock) {

    Y_UNIT_TEST(LockUnlock) {
        TTestEnv env;
        auto session = MakeSession(env);
        auto lock = MakeLock(session);
        lock.lock();
        lock.unlock();
    }

    Y_UNIT_TEST(LockGuard) {
        TTestEnv env;
        auto session = MakeSession(env);
        auto lock = MakeLock(session);
        std::lock_guard guard(lock);
    }

    Y_UNIT_TEST(TryLockSuccess) {
        TTestEnv env;
        auto session = MakeSession(env);
        auto lock = MakeLock(session);
        UNIT_ASSERT(lock.try_lock());
        lock.unlock();
    }

    Y_UNIT_TEST(TryLockFailsWhenHeld) {
        TTestEnv env;
        auto sessionA = MakeSession(env);
        auto sessionB = MakeSession(env);
        auto lockA = MakeLock(sessionA);
        auto lockB = MakeLock(sessionB);
        lockA.lock();
        UNIT_ASSERT(!lockB.try_lock());
        lockA.unlock();
    }

    Y_UNIT_TEST(MultipleLockNamesInOneSession) {
        TTestEnv env;
        auto session = MakeSession(env);
        auto lockA = MakeLock(session);
        auto lockB = MakeLock(session, ANOTHER_SEMAPHORE_NAME);
        lockA.lock();
        lockB.lock();
        lockB.unlock();
        lockA.unlock();
    }

    Y_UNIT_TEST(LockThrowsOnAcquireFailure) {
        TTestEnv env;
        env.CoordinationService.FailNextAcquire.store(true);
        auto session = MakeSession(env);
        auto lock = MakeLock(session);
        UNIT_ASSERT_EXCEPTION(lock.lock(), TYdbLockException);
        UNIT_ASSERT(!lock.getStopToken().stop_requested());
        lock.lock();
        lock.unlock();
    }

    Y_UNIT_TEST(UnlockFailureNotifiesStopToken) {
        TTestEnv env;
        auto session = MakeSession(env);
        auto lock = MakeLock(session);
        lock.lock();
        auto token = lock.getStopToken();
        env.CoordinationService.FailNextRelease.store(true);
        lock.unlock();
        UNIT_ASSERT(token.stop_requested());
    }

    Y_UNIT_TEST(OwnerDataIsHostName) {
        TTestEnv env;
        auto session = MakeSession(env);
        auto lock = MakeLock(session);
        lock.lock();

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
        auto session = MakeSession(env);
        auto lock = MakeLock(session);
        UNIT_ASSERT(!lock.getStopToken().stop_requested());
    }

    Y_UNIT_TEST(SessionExpiryWhileHoldingLock) {
        TTestEnv env;
        env.CoordinationService.MaxPingResponses.store(2);
        auto session = MakeSession(env);
        auto lock = MakeLock(session);
        lock.lock();
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(5);
        while (!lock.getStopToken().stop_requested() && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT(lock.getStopToken().stop_requested());
        lock.unlock();

        UNIT_ASSERT_EXCEPTION(lock.lock(), TYdbLockException);

        env.CoordinationService.MaxPingResponses.store(0);
        auto newSession = MakeSession(env);
        auto newLock = MakeLock(newSession);
        newLock.lock();
        UNIT_ASSERT(!newLock.getStopToken().stop_requested());
        newLock.unlock();
    }

    Y_UNIT_TEST(RecoverableTransportFailureDoesNotNotifyStopToken) {
        TTestEnv env;
        auto session = MakeSession(env);
        auto lock = MakeLock(session);
        lock.lock();
        auto token = lock.getStopToken();

        env.CoordinationService.BreakNextPingWithoutSessionLoss.store(true);
        auto pingResult = session.Ping().ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL(pingResult.GetStatus(), EStatus::SUCCESS);

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(5);
        while (session.GetConnectionState() != EConnectionState::CONNECTED && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_VALUES_EQUAL(session.GetConnectionState(), EConnectionState::CONNECTED);
        UNIT_ASSERT(!token.stop_requested());

        lock.unlock();
    }

}
