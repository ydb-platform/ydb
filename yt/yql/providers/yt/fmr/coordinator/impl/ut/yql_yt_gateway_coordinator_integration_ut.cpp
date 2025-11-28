/**
 * Integration tests for Gateway <-> Coordinator interaction
 *
 * Uses TFailureInjector from yql/essentials/utils/failure_injector/failure_injector.h
 * to simulate various failure scenarios.
 */

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/time_provider/time_provider.h>

#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file/yql_yt_file_coordinator_service.h>
#include <yt/yql/providers/yt/fmr/test_tools/fmr_gateway_helpers/yql_yt_fmr_gateway_helpers.h>
#include <yt/yql/providers/yt/fmr/test_tools/mock_time_provider/yql_yt_mock_time_provider.h>

#include <yql/essentials/utils/failure_injector/failure_injector.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NYql::NFmr {

void SetUpLogger() {
    TString logType = "cout";
    NLog::InitLogger(logType, false);
    NLog::EComponentHelpers::ForEach([](NLog::EComponent component) {
        NLog::YqlLogger().SetComponentLevel(component, NLog::ELevel::DEBUG);
    });
}

IYtGateway::TPtr CreateTestFmrGateway(
    IFmrCoordinator::TPtr coordinator,
    NKikimr::NMiniKQL::IFunctionRegistry::TPtr functionRegistry,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TDuration pingInterval = TDuration::MilliSeconds(300))
{
    return CreateTestFmrGatewayWithCoordinator(
        coordinator,
        functionRegistry,
        timeProvider,
        pingInterval);
}

Y_UNIT_TEST_SUITE(GatewayCoordinatorIntegrationTests) {

    /**
     * Test 1: Gateway actively pings coordinator and session stays alive
     *
     * Timeline:
     * - SessionInactivityTimeout = 1000ms
     * - HealthCheckInterval = 200ms (coordinator checks every 200ms)
     * - Gateway pings every 300ms (automatically via internal thread)
     *
     * Expected: Session stays alive because gateway's automatic pings refresh LastActivity
     */
    Y_UNIT_TEST(GatewayPingsAndSessionStaysAlive) {
        SetUpLogger();

        auto timeProvider = MakeIntrusive<TMockTimeProvider>(TDuration::MilliSeconds(200));

        auto coordinatorSettings = TFmrCoordinatorSettings();
        coordinatorSettings.TimeProvider = timeProvider;
        coordinatorSettings.SessionInactivityTimeout = TDuration::MilliSeconds(1000);
        coordinatorSettings.HealthCheckInterval = TDuration::MilliSeconds(200);

        auto coordinator = MakeFmrCoordinator(coordinatorSettings, MakeFileYtCoordinatorService());

        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::IBuiltinFunctionRegistry::TPtr());

        auto gateway = CreateTestFmrGateway(coordinator, functionRegistry, timeProvider, TDuration::MilliSeconds(300));

        TString sessionId = "gateway-active-session";

        IYtGateway::TOpenSessionOptions openOpts(sessionId);
        openOpts.UserName() = "test-user";
        openOpts.RandomProvider() = CreateDeterministicRandomProvider(1);
        gateway->OpenSession(std::move(openOpts));
        Sleep(TDuration::MilliSeconds(100));

        // Gateway session opened, internal ping thread started
        auto listResponse1 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse1.SessionIds.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listResponse1.SessionIds[0], sessionId);

        // Advance time, gateway should ping automatically
        for (int i = 0; i < 5; ++i) {
            timeProvider->Advance(TDuration::MilliSeconds(300));
        }

        // Session should still be active after 1500ms with gateway's automatic pings
        auto listResponse2 = coordinator->ListSessions({}).GetValueSync();
        Sleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(listResponse2.SessionIds.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listResponse2.SessionIds[0], sessionId);

        IYtGateway::TCloseSessionOptions closeOpts(sessionId);
        gateway->CloseSession(std::move(closeOpts)).GetValueSync();
    }

    /**
     * Test 2: Gateway stops (crashes) and coordinator cleans up inactive session
     *
     * Timeline:
     * - SessionInactivityTimeout = 800ms
     * - HealthCheckInterval = 200ms
     * - Gateway pings automatically for a while, then crashes (destroyed)
     *
     * Expected: Coordinator detects inactivity after gateway crash and cleans up session
     */
    Y_UNIT_TEST(GatewayStopsAndCoordinatorCleansUpSession) {
        SetUpLogger();

        auto timeProvider = MakeIntrusive<TMockTimeProvider>(TDuration::MilliSeconds(200));

        auto coordinatorSettings = TFmrCoordinatorSettings();
        coordinatorSettings.TimeProvider = timeProvider;
        coordinatorSettings.SessionInactivityTimeout = TDuration::MilliSeconds(800);
        coordinatorSettings.HealthCheckInterval = TDuration::MilliSeconds(200);

        auto coordinator = MakeFmrCoordinator(coordinatorSettings, MakeFileYtCoordinatorService());

        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::IBuiltinFunctionRegistry::TPtr());

        TString sessionId = "gateway-crash-session";

        {
            // Gateway opens session and pings automatically
            auto gateway = CreateTestFmrGateway(coordinator, functionRegistry, timeProvider, TDuration::MilliSeconds(200));
            IYtGateway::TOpenSessionOptions openOpts(sessionId);
            openOpts.UserName() = "test-user";
            openOpts.RandomProvider() = CreateDeterministicRandomProvider(1);
            gateway->OpenSession(std::move(openOpts));

            auto listResponse1 = coordinator->ListSessions({}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(listResponse1.SessionIds.size(), 1);

            // Gateway should ping at t=200ms and t=400ms
            timeProvider->Advance(TDuration::MilliSeconds(200));
            timeProvider->Advance(TDuration::MilliSeconds(200));

            auto listResponse2 = coordinator->ListSessions({}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(listResponse2.SessionIds.size(), 1);

            // Gateway crashes when going out of scope
        }

        // Advance to t=1000ms - inactivity is 600ms which is less than 800ms timeout
        timeProvider->Advance(TDuration::MilliSeconds(600));

        auto listResponse3 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse3.SessionIds.size(), 1);

        // Advance to t=1600ms - inactivity is 1200ms which exceeds the 800ms timeout
        timeProvider->Advance(TDuration::MilliSeconds(600), TDuration::Seconds(2));

        // Session should be cleaned up
        auto listResponse4 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse4.SessionIds.size(), 0);
    }

    /**
     * Test 3: Gateway with intermittent network failures - session stays alive
     *
     * Scenario:
     * - Gateway automatically pings 20 times via internal thread
     * - Some pings fail (failure injector simulates network issues)
     * - But successful pings keep coming often enough
     *
     * Expected: Session stays alive because there are enough successful automatic pings
     */
    Y_UNIT_TEST(GatewayWithIntermittentFailuresSessionStaysAlive) {
        SetUpLogger();
        TFailureInjector::Activate();

        auto timeProvider = MakeIntrusive<TMockTimeProvider>(TDuration::MilliSeconds(200));

        auto coordinatorSettings = TFmrCoordinatorSettings();
        coordinatorSettings.TimeProvider = timeProvider;
        coordinatorSettings.SessionInactivityTimeout = TDuration::MilliSeconds(2000);
        coordinatorSettings.HealthCheckInterval = TDuration::MilliSeconds(200);

        auto coordinator = MakeFmrCoordinator(coordinatorSettings, MakeFileYtCoordinatorService());
        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::IBuiltinFunctionRegistry::TPtr());
        auto gateway = CreateTestFmrGateway(coordinator, functionRegistry, timeProvider, TDuration::MilliSeconds(150));

        TString sessionId = "gateway-intermittent-failures";
        IYtGateway::TOpenSessionOptions openOpts(sessionId);
        openOpts.UserName() = "test-user";
        openOpts.RandomProvider() = CreateDeterministicRandomProvider(1);
        gateway->OpenSession(std::move(openOpts));

        auto listResponse1 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse1.SessionIds.size(), 1);
        TFailureInjector::Set("coordinator.ping_session", 5, 1);

        for (int i = 0; i < 20; ++i) {
            timeProvider->Advance(TDuration::MilliSeconds(150));
            if (i == 6) {
                TFailureInjector::Set("coordinator.ping_session", 5, 1);
            }
        }

        Sleep(TDuration::Seconds(2));
        auto listResponse2 = coordinator->ListSessions({}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(listResponse2.SessionIds.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listResponse2.SessionIds[0], sessionId);

        IYtGateway::TCloseSessionOptions closeOpts(sessionId);
        gateway->CloseSession(std::move(closeOpts)).GetValueSync();
    }
}

} // namespace NYql::NFmr
