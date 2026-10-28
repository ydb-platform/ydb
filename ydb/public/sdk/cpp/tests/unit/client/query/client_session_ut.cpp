#include "mock_query_service.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/tests/common/fake_metric_registry.h>
#include <library/cpp/testing/common/network.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/system/event.h>

#include <chrono>
#include <optional>
#include <thread>

#include <ydb/public/sdk/cpp/src/client/query/impl/session_state_handler.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/session/session_pool.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>

#include <exception>
#include <stdexcept>

using namespace NYdb;
using namespace NYdb::NQuery;
using NYdb::NTests::StartGrpcServer;
using NYdb::NTests::TMockQueryService;

namespace {

class TMockSessionClient : public ISessionClient {
public:
    void DeleteSession(TKqpSessionCommon*) override {
    }

    void PessimizeNode(std::uint64_t nodeId) override {
        PessimizedNodeId = nodeId;
        ++PessimizeCalls;
    }

    bool ReturnSession(TKqpSessionCommon*) override {
        return true;
    }

    void RecordSessionClosed(std::string_view reason) override {
        LastReason = std::string(reason);
        ++CloseMetrics;
    }

    std::uint64_t PessimizedNodeId = 0;
    int PessimizeCalls = 0;
    std::string LastReason;
    int CloseMetrics = 0;
};

class TMockServerCloseHandler : public IServerCloseHandler {
public:
    void OnCloseSession(const TKqpSessionCommon*, std::shared_ptr<ISessionClient>,
        std::string_view) override
    {
        ++CloseCalls;
    }

    int CloseCalls = 0;
};

std::string MakeSessionIdWithNodeId(std::uint64_t nodeId) {
    NKikimr::NOperationId::TOperationId operationId;
    operationId.SetKind(NKikimr::NOperationId::TOperationId::SESSION_YQL);
    operationId.AddOptionalValue("node_id", ToString(nodeId));
    return operationId.ToString();
}

class TTestKqpSession : public TKqpSessionCommon {
public:
    TTestKqpSession(const std::string& sessionId, const std::string& endpoint)
        : TKqpSessionCommon(sessionId, endpoint, true)
    {
        MarkActive();
    }

    std::shared_ptr<ISessionClient> GetSessionClient() const override {
        return Client;
    }

    std::shared_ptr<ISessionClient> Client;
};

Ydb::Query::SessionState MakeSessionShutdownState() {
    Ydb::Query::SessionState state;
    state.mutable_session_shutdown();
    return state;
}

Ydb::Query::SessionState MakeNodeShutdownState() {
    Ydb::Query::SessionState state;
    state.mutable_node_shutdown();
    return state;
}

template <typename TPredicate>
void WaitUntil(TPredicate predicate) {
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    while (!predicate() && TInstant::Now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    UNIT_ASSERT(predicate());
}

TSession GetSession(TAsyncCreateSessionResult future) {
    UNIT_ASSERT(future.Wait(TDuration::Seconds(10)));
    auto result = future.ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetSession();
}

enum class EShutdownOrder { Active, Idle, Waiter, ConcurrentReturn };

void TestSessionShutdown(EShutdownOrder order, bool closeStream = false) {
    using namespace NSessionPool::NSessionCloseCommands;
    const NSessionPool::TSessionCloseCommand* shutdowns[] = {&SessionShutdown, &NodeShutdown};
    const NSessionPool::TSessionCloseCommand* closures[] = {&AttachClosed, &TransportError};
    for (const auto* command : closeStream ? closures : shutdowns) {
        for (bool deferred : {false, true}) {
            NTesting::InitPortManagerFromEnv();
            const auto endpoint = TStringBuilder() << "127.0.0.1:" << NTesting::GetFreePort();
            TManualEvent shutdown;
            TMockQueryService service;
            service.AttachStatus = grpc::Status(command == &AttachClosed ? grpc::StatusCode::OUT_OF_RANGE
                : command == &TransportError ? grpc::StatusCode::UNAVAILABLE : grpc::StatusCode::OK, "test attach status");
            service.OnAttach = [&](auto* writer) {
                if (shutdown.WaitT(TDuration::Seconds(10)) && !closeStream) {
                    writer->Write(command == &NodeShutdown ? MakeNodeShutdownState() : MakeSessionShutdownState());
                }
            };
            auto server = StartGrpcServer(endpoint, service);
            UNIT_ASSERT(server);
            auto metrics = std::make_shared<NTests::TFakeMetricRegistry>();
            TDriver driver(TDriverConfig().SetEndpoint(endpoint).SetDiscoveryMode(EDiscoveryMode::Off)
                               .SetDatabase("/Root/My/DB").SetMetricRegistry(metrics));
            Y_DEFER { driver.Stop(true); };
            TQueryClient client(driver, TClientSettings().PoolName("shutdown").SessionPoolSettings(
                TSessionPoolSettings().MaxActiveSessions(1).MinPoolSize(1).UseDeferredSessionCreation(deferred)));
            Y_DEFER { shutdown.Signal(); };
            std::optional<TSession> session = GetSession(client.GetSession());
            const auto oldId = session->GetId();
            UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
            if (order == EShutdownOrder::Idle) session.reset();
            UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), order == EShutdownOrder::Idle ? 1 : 0);
            auto waiter = order == EShutdownOrder::Waiter ? client.GetSession() : TAsyncCreateSessionResult{};
            UNIT_ASSERT(!waiter.Initialized() || !waiter.HasValue());
            shutdown.Signal();
            if (order == EShutdownOrder::ConcurrentReturn) session.reset();
            const NMetrics::TLabels labels = {{"ydb.query.session.pool.name", "shutdown"}, {"reason", std::string(command->Reason)}};
            WaitUntil([&] {
                auto count = metrics->GetCounter("ydb.query.session.closed", labels);
                return count && count->Get() >= 1;
            });
            UNIT_ASSERT(!waiter.Initialized() || !waiter.HasValue());
            if (session) {
                UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
                UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), 0);
            }
            session.reset();
            WaitUntil([&] { return service.ClosedSessions.load() == 1; });
            UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), waiter.Initialized() ? 1 : 0);
            {
                auto replacement = GetSession(waiter.Initialized() ? waiter : client.GetSession());
                UNIT_ASSERT(replacement.GetId() != oldId);
                UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(service.CreateSessionRequests.load(), 2);
            UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(metrics->GetCounter("ydb.query.session.closed", labels)->Get(), 1);
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(QueryAttachSessionState) {

Y_UNIT_TEST(SessionShutdownActiveSessionMarksClosing) {
    TTestKqpSession session(MakeSessionIdWithNodeId(42), "host:2136");
    auto client = std::make_shared<TMockSessionClient>();

    UNIT_ASSERT(HandleAttachSessionState(MakeSessionShutdownState(), &session, client)
        == EAttachStreamReadAction::Stop);
    UNIT_ASSERT(session.GetState() == TKqpSessionCommon::S_CLOSING);
    UNIT_ASSERT_VALUES_EQUAL(client->PessimizeCalls, 0);
}

Y_UNIT_TEST(SessionShutdownIdleInPoolDelegatesToCloseHandler) {
    TTestKqpSession session(MakeSessionIdWithNodeId(42), "host:2136");
    auto client = std::make_shared<TMockSessionClient>();
    TMockServerCloseHandler closeHandler;
    session.MarkIdle();
    session.UpdateServerCloseHandler(&closeHandler);

    UNIT_ASSERT(HandleAttachSessionState(MakeSessionShutdownState(), &session, client)
        == EAttachStreamReadAction::Stop);
    UNIT_ASSERT_VALUES_EQUAL(closeHandler.CloseCalls, 1);
    UNIT_ASSERT_VALUES_EQUAL(client->PessimizeCalls, 0);
}

Y_UNIT_TEST(NodeShutdownActiveSessionMarksClosingAndPessimizesNode) {
    TTestKqpSession session(MakeSessionIdWithNodeId(42), "host:2136");
    auto client = std::make_shared<TMockSessionClient>();

    UNIT_ASSERT(HandleAttachSessionState(MakeNodeShutdownState(), &session, client)
        == EAttachStreamReadAction::Stop);
    UNIT_ASSERT(session.GetState() == TKqpSessionCommon::S_CLOSING);
    UNIT_ASSERT_VALUES_EQUAL(client->PessimizeCalls, 1);
    UNIT_ASSERT_VALUES_EQUAL(client->PessimizedNodeId, 42U);
}

Y_UNIT_TEST(NodeShutdownWithZeroNodeIdSkipsPessimization) {
    TTestKqpSession session("", "host:2136");
    auto client = std::make_shared<TMockSessionClient>();

    UNIT_ASSERT(session.GetEndpointKey().GetNodeId() == 0U);
    UNIT_ASSERT(HandleAttachSessionState(MakeNodeShutdownState(), &session, client)
        == EAttachStreamReadAction::Stop);
    UNIT_ASSERT(session.GetState() == TKqpSessionCommon::S_CLOSING);
    UNIT_ASSERT_VALUES_EQUAL(client->PessimizeCalls, 0);
}

Y_UNIT_TEST(EmptySessionStateContinuesReading) {
    TTestKqpSession session(MakeSessionIdWithNodeId(7), "host:2136");
    auto client = std::make_shared<TMockSessionClient>();
    Ydb::Query::SessionState state;

    UNIT_ASSERT(HandleAttachSessionState(state, &session, client)
        == EAttachStreamReadAction::Continue);
    UNIT_ASSERT(session.GetState() == TKqpSessionCommon::S_ACTIVE);
    UNIT_ASSERT_VALUES_EQUAL(client->PessimizeCalls, 0);
}

Y_UNIT_TEST(SessionShutdownNullSessionStopsReading) {
    auto client = std::make_shared<TMockSessionClient>();

    UNIT_ASSERT(HandleAttachSessionState(MakeSessionShutdownState(), nullptr, client)
        == EAttachStreamReadAction::Stop);
    UNIT_ASSERT_VALUES_EQUAL(client->PessimizeCalls, 0);
}

Y_UNIT_TEST(ShutdownPreservesStateAndRejectsPoolReturn) {
    using TPrepare = bool (TKqpSessionCommon::*)();
    for (TPrepare prepare : {TPrepare{}, &TKqpSessionCommon::MarkActive, &TKqpSessionCommon::MarkIdle,
                            &TKqpSessionCommon::MarkBroken, &TKqpSessionCommon::MarkAsClosing}) {
        for (const auto& shutdown : {MakeSessionShutdownState(), MakeNodeShutdownState()}) {
            for (bool withClient : {false, true}) {
                for (bool closedPool : {false, true}) {
                    auto session = std::make_unique<TKqpSessionCommon>(MakeSessionIdWithNodeId(42), "host:2136", true);
                    if (prepare == &TKqpSessionCommon::MarkBroken) session->MarkActive();
                    if (prepare) UNIT_ASSERT((session.get()->*prepare)());
                    const auto initial = session->GetState();
                    auto client = withClient ? std::make_shared<TMockSessionClient>() : nullptr;
                    NSessionPool::TSessionPool pool(1);
                    if (closedPool) pool.Drain([](auto&&) { return true; }, true);
                    auto expectRejected = [&] {
                        const bool returned = pool.ReturnSession(session.get(), false);
                        if (returned) session.release();
                        UNIT_ASSERT(!returned);
                    };
                    if (closedPool || initial != TKqpSessionCommon::S_IDLE) expectRejected();
                    UNIT_ASSERT(HandleAttachSessionState(shutdown, session.get(), client) == EAttachStreamReadAction::Stop);
                    const auto expected = initial == TKqpSessionCommon::S_IDLE || initial == TKqpSessionCommon::S_BROKEN
                        ? initial : TKqpSessionCommon::S_CLOSING;
                    UNIT_ASSERT(session->GetState() == expected);
                    UNIT_ASSERT_VALUES_EQUAL(session->NeedUpdateActiveCounter(),
                        initial == TKqpSessionCommon::S_ACTIVE || initial == TKqpSessionCommon::S_BROKEN);
                    UNIT_ASSERT(!session->CanReturnToPool());
                    expectRejected();
                    // A checkout selected before shutdown may finish, but cannot return to the pool.
                    UNIT_ASSERT_VALUES_EQUAL(session->MarkActive(), initial == TKqpSessionCommon::S_IDLE);
                    HandleAttachSessionState(shutdown, session.get(), client);
                    UNIT_ASSERT(!session->MarkIdle());
                    if (client) UNIT_ASSERT_VALUES_EQUAL(client->CloseMetrics,
                        initial == TKqpSessionCommon::S_BROKEN || initial == TKqpSessionCommon::S_CLOSING ? 0 : 1);
                    UNIT_ASSERT_VALUES_EQUAL(pool.GetCurrentPoolSize(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(pool.GetActiveSessions(), 0);
                }
            }
        }
    }
}

Y_UNIT_TEST(CloseReasonCommandsAreCompleteAndDeduplicated) {
    const std::pair<const NSessionPool::TSessionCloseCommand*, std::string_view> commands[] = {
        {&NSessionPool::NSessionCloseCommands::PoolIdleTimeout, "pool_idle_timeout"},
        {&NSessionPool::NSessionCloseCommands::PoolGracefulShutdown, "pool_graceful_shutdown"},
        {&NSessionPool::NSessionCloseCommands::ClientTimeout, "client_timeout"},
        {&NSessionPool::NSessionCloseCommands::ClientCancelled, "client_cancelled"},
        {&NSessionPool::NSessionCloseCommands::AttachClosed, "attach_closed"},
        {&NSessionPool::NSessionCloseCommands::TransportError, "transport_error"},
        {&NSessionPool::NSessionCloseCommands::NodeShutdown, "node_shutdown"},
        {&NSessionPool::NSessionCloseCommands::SessionShutdown, "session_shutdown"},
        {&NSessionPool::NSessionCloseCommands::BadSession, "bad_session"},
        {&NSessionPool::NSessionCloseCommands::SessionBusy, "session_busy"},
    };
    auto client = std::make_shared<TMockSessionClient>();
    for (const auto& [command, reason] : commands) {
        TTestKqpSession session("", "");
        command->Execute(session, client.get());
        command->Execute(session, client.get());
        UNIT_ASSERT_VALUES_EQUAL(client->LastReason, reason);
        session.MarkBroken();
        command->Execute(session, client.get());
        UNIT_ASSERT(session.GetState() == TKqpSessionCommon::S_BROKEN);
    }
    UNIT_ASSERT_VALUES_EQUAL(client->CloseMetrics, 10);

    TKqpSessionCommon standalone("", "", false);
    NSessionPool::NSessionCloseCommands::BadSession.Execute(standalone, client.get());
    UNIT_ASSERT_VALUES_EQUAL(client->CloseMetrics, 10);
    UNIT_ASSERT(!NSessionPool::NSessionCloseCommands::FromStatus(TStatus(EStatus::SESSION_EXPIRED, {})));
}

}

Y_UNIT_TEST_SUITE(QuerySessionStatusInterception) {

    Y_UNIT_TEST(ExceptionForwardedAndSessionReleasedBeforeCompletion) {
        auto session = std::make_shared<TTestKqpSession>("", "");
        auto client = std::make_shared<TMockSessionClient>();
        session->Client = client;
        std::weak_ptr<TTestKqpSession> weakSession = session;
        auto sourcePromise = NThreading::NewPromise<TStatus>();
        auto result = NSessionPool::InjectSessionStatusInterception<TStatus>(
            session, sourcePromise.GetFuture(), true, TDuration::Seconds(1));
        bool releasedBeforeCompletion = false;
        result.Subscribe([&](const NThreading::TFuture<TStatus>&) {
            releasedBeforeCompletion = session.use_count() == 1;
        });
        sourcePromise.SetException(std::make_exception_ptr(std::runtime_error("interception error")));

        UNIT_ASSERT(releasedBeforeCompletion);
        UNIT_ASSERT(session->GetState() == TKqpSessionCommon::S_BROKEN);
        UNIT_ASSERT(session->NeedUpdateActiveCounter());
        UNIT_ASSERT_VALUES_EQUAL(client->CloseMetrics, 1);
        UNIT_ASSERT_VALUES_EQUAL(client->LastReason, "transport_error");
        session.reset();
        UNIT_ASSERT(weakSession.expired());
        UNIT_ASSERT_EXCEPTION_CONTAINS(result.GetValueSync(), std::runtime_error, "interception error");
    }

} // Y_UNIT_TEST_SUITE(QuerySessionStatusInterception)

Y_UNIT_TEST_SUITE(QuerySessionShutdown) {
Y_UNIT_TEST(ActiveSession) { TestSessionShutdown(EShutdownOrder::Active); }
Y_UNIT_TEST(IdleSession) { TestSessionShutdown(EShutdownOrder::Idle); }
Y_UNIT_TEST(QueuedWaiter) { TestSessionShutdown(EShutdownOrder::Waiter); }
Y_UNIT_TEST(ConcurrentReturn) { TestSessionShutdown(EShutdownOrder::ConcurrentReturn); }
Y_UNIT_TEST(IdleAttachClosure) { TestSessionShutdown(EShutdownOrder::Idle, true); }
}
