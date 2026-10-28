#include "mock_query_service.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/tests/common/fake_metric_registry.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/system/event.h>

#include <atomic>
#include <chrono>
#include <optional>
#include <thread>

using namespace NYdb;
using namespace NYdb::NQuery;
using NYdb::NTests::StartGrpcServer;
using NYdb::NTests::TMockQueryService;

namespace {

template <typename TPredicate>
void WaitUntil(TPredicate predicate) {
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    while (!predicate() && TInstant::Now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    UNIT_ASSERT(predicate());
}

enum class EShutdownOrder { Active, Idle, Waiter, ConcurrentReturn };

void TestSessionShutdown(EShutdownOrder order, bool closeStream = false) {
    struct TCloseEvent {
        bool NodeShutdown;
        grpc::StatusCode Status;
        const char* Reason;
    };
    const TCloseEvent shutdowns[] = {
        {false, grpc::StatusCode::OK, "session_shutdown"},
        {true, grpc::StatusCode::OK, "node_shutdown"},
    };
    const TCloseEvent closures[] = {
        {false, grpc::StatusCode::OUT_OF_RANGE, "attach_closed"},
        {false, grpc::StatusCode::UNAVAILABLE, "transport_error"},
    };
    for (const auto& event : closeStream ? closures : shutdowns) {
        for (bool deferred : {false, true}) {
            NTesting::InitPortManagerFromEnv();
            const auto endpoint = TStringBuilder() << "127.0.0.1:" << NTesting::GetFreePort();
            std::atomic_uint completions = 0;
            TManualEvent shutdown;
            TMockQueryService service;
            service.AttachStatus = grpc::Status(event.Status, "test attach status");
            service.OnAttach = [&](auto* writer) {
                if (shutdown.WaitT(TDuration::Seconds(10)) && event.Status == grpc::StatusCode::OK) {
                    Ydb::Query::SessionState state;
                    if (event.NodeShutdown) {
                        state.mutable_node_shutdown();
                    } else {
                        state.mutable_session_shutdown();
                    }
                    writer->Write(state);
                }
            };
            auto server = StartGrpcServer(endpoint, service);
            UNIT_ASSERT(server);
            auto metrics = std::make_shared<NTests::TFakeMetricRegistry>();
            TDriver driver(TDriverConfig().SetEndpoint(endpoint).SetDiscoveryMode(EDiscoveryMode::Off)
                               .SetDatabase("/Root/My/DB").SetMetricRegistry(metrics));
            auto client = std::make_unique<TQueryClient>(driver, TClientSettings().SessionPoolSettings(
                TSessionPoolSettings().MaxActiveSessions(1).MinPoolSize(1).UseDeferredSessionCreation(deferred)));
            Y_DEFER {
                shutdown.Signal();
                client.reset();
                driver.Stop(true);
            };
            std::optional<TSession> session;
            {
                auto future = client->GetSession();
                UNIT_ASSERT(future.Wait(TDuration::Seconds(10)));
                auto result = future.ExtractValueSync();
                UNIT_ASSERT(result.IsSuccess());
                session = result.GetSession();
            }
            const auto oldId = session->GetId();
            UNIT_ASSERT_VALUES_EQUAL(client->GetActiveSessionCount(), 1);
            if (order == EShutdownOrder::Idle) {
                session.reset();
                UNIT_ASSERT_VALUES_EQUAL(client->GetCurrentPoolSize(), 1);
            }
            TAsyncCreateSessionResult waiter;
            if (order == EShutdownOrder::Waiter) {
                waiter = client->GetSession();
                waiter.Subscribe([&](const auto&) { ++completions; });
                UNIT_ASSERT(!waiter.HasValue());
            }
            shutdown.Signal();
            if (order == EShutdownOrder::ConcurrentReturn) {
                session.reset();
            }
            const NMetrics::TLabels labels = {
                {"ydb.query.session.pool.name", "/Root/My/DB@" + std::string(endpoint)},
                {"reason", event.Reason},
            };
            WaitUntil([&] {
                auto count = metrics->GetCounter("ydb.query.session.closed", labels);
                return count && count->Get() >= 1;
            });
            if (session) {
                UNIT_ASSERT_VALUES_EQUAL(client->GetActiveSessionCount(), 1);
                UNIT_ASSERT_VALUES_EQUAL(client->GetCurrentPoolSize(), 0);
                if (order == EShutdownOrder::Waiter) {
                    UNIT_ASSERT(!waiter.HasValue());
                }
                session.reset();
            }
            if (order != EShutdownOrder::Waiter) {
                WaitUntil([&] { return client->GetCurrentPoolSize() == 0; });
                UNIT_ASSERT_VALUES_EQUAL(client->GetActiveSessionCount(), 0);
                waiter = client->GetSession();
            }
            UNIT_ASSERT(waiter.Wait(TDuration::Seconds(10)));
            {
                auto result = waiter.ExtractValueSync();
                UNIT_ASSERT(result.IsSuccess());
                UNIT_ASSERT(result.GetSession().GetId() != oldId);
                UNIT_ASSERT_VALUES_EQUAL(client->GetActiveSessionCount(), 1);
            }
            WaitUntil([&] { return service.ClosedSessions.load() == 1; });
            UNIT_ASSERT_VALUES_EQUAL(service.CreateSessionRequests.load(), 2);
            UNIT_ASSERT_VALUES_EQUAL(client->GetCurrentPoolSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(client->GetActiveSessionCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(metrics->GetCounter("ydb.query.session.closed", labels)->Get(), 1);
            if (order == EShutdownOrder::Waiter) {
                WaitUntil([&] { return completions.load() == 1; });
            }
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(QuerySessionShutdown) {
Y_UNIT_TEST(ActiveSession) { TestSessionShutdown(EShutdownOrder::Active); }
Y_UNIT_TEST(IdleSession) { TestSessionShutdown(EShutdownOrder::Idle); }
Y_UNIT_TEST(QueuedWaiter) { TestSessionShutdown(EShutdownOrder::Waiter); }
Y_UNIT_TEST(ConcurrentReturn) { TestSessionShutdown(EShutdownOrder::ConcurrentReturn); }
Y_UNIT_TEST(IdleAttachClosure) { TestSessionShutdown(EShutdownOrder::Idle, true); }
}
