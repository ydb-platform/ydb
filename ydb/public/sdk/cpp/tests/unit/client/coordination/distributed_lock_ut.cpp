#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/distributed_lock.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

#include "coordination_grpc_mock.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/system/hostname.h>

#include <mutex>
#include <optional>

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
    return TDistributedLock(*env.Client, COORD_PATH, name, TEST_TIMEOUT);
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
        UNIT_ASSERT(!lockB.try_lock());
        lockA.unlock();
    }

    Y_UNIT_TEST(LockThrowsOnAcquireFailure) {
        TTestEnv env;
        env.CoordinationService.FailNextAcquire.store(true);
        auto lock = MakeLock(env);
        UNIT_ASSERT_EXCEPTION(lock.lock(), TYdbLockException);
        UNIT_ASSERT(!lock.getStopToken().stop_requested());
        lock.lock();
        lock.unlock();
    }

    Y_UNIT_TEST(UnlockFailureNotifiesStopToken) {
        TTestEnv env;
        auto lock = MakeLock(env);
        auto token = lock.getStopToken();
        lock.lock();
        env.CoordinationService.FailNextRelease.store(true);
        lock.unlock();
        UNIT_ASSERT(token.stop_requested());
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
        UNIT_ASSERT(!lock.getStopToken().stop_requested());
    }

}
