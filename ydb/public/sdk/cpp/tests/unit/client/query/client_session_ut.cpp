#include <ydb/public/sdk/cpp/src/client/query/impl/session_state_handler.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/session/session_pool.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>

#include <exception>
#include <future>
#include <stdexcept>
#include <utility>

using namespace NYdb;
using namespace NYdb::NQuery;

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

class TPoolSessionClient : public TMockSessionClient {
public:
    void DeleteSession(TKqpSessionCommon* session) override {
        DeletedSession.reset(session);
    }

    std::unique_ptr<TKqpSessionCommon> DeletedSession;
};

struct TGetSessionResult {
    TKqpSessionCommon* Session = nullptr;
    int Replies = 0;
    int NewSessions = 0;
    int Errors = 0;
};

class TGetSessionProbe : public NSessionPool::IGetSessionCtx {
public:
    explicit TGetSessionProbe(TGetSessionResult& result)
        : Result(result)
    {}

    void ReplySessionToUser(TKqpSessionCommon* session) override {
        Result.Session = session;
        ++Result.Replies;
    }

    void ReplyError(TStatus) override {
        ++Result.Errors;
    }

    void ReplyNewSession() override {
        ++Result.NewSessions;
    }

    void ScheduleOnDeadlineWaiterCleanup() override {}

    TDeadline GetDeadline() const override {
        return TDeadline::Max();
    }

private:
    TGetSessionResult& Result;
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

Y_UNIT_TEST(ShutdownAfterReturnDoesNotLeaveClosingSessionInPool) {
    NSessionPool::TSessionPool pool(1);
    auto session = std::make_unique<TKqpSessionCommon>("", "host:2136", true);
    UNIT_ASSERT(session->MarkActive());
    auto* rawSession = session.get();
    auto client = std::make_shared<TPoolSessionClient>();

    std::promise<void> shutdownReady;
    auto shutdownReadyFuture = shutdownReady.get_future();
    std::promise<void> returnedToPool;
    auto returnedToPoolFuture = returnedToPool.get_future();
    auto shutdown = std::async(std::launch::async, [&] {
        return HandleAttachSessionState(MakeSessionShutdownState(), rawSession, client, [&] {
            shutdownReady.set_value();
            returnedToPoolFuture.get();
        });
    });

    shutdownReadyFuture.get();
    const bool markedIdle = session->MarkIdle();
    const bool returned = pool.ReturnSession(session.get(), false);
    if (returned) {
        session.release();
    }
    // Release the callback before assertions or future destruction, even on failure.
    returnedToPool.set_value();
    const auto action = shutdown.get();

    UNIT_ASSERT(markedIdle);
    UNIT_ASSERT(returned);
    UNIT_ASSERT(action == EAttachStreamReadAction::Stop);
    // The server-closed session must be removed instead of left CLOSING in the pool.
    UNIT_ASSERT_VALUES_EQUAL(pool.GetCurrentPoolSize(), 0);
    UNIT_ASSERT(client->DeletedSession.get() == rawSession);
    UNIT_ASSERT(client->DeletedSession->GetState() == TKqpSessionCommon::S_BROKEN);
    UNIT_ASSERT_VALUES_EQUAL(pool.GetActiveSessions(), 0);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseMetrics, 0);

    TGetSessionResult result;
    pool.GetSession(std::make_unique<TGetSessionProbe>(result));
    UNIT_ASSERT_VALUES_EQUAL(result.Replies, 0);
    UNIT_ASSERT_VALUES_EQUAL(result.NewSessions, 1);
    UNIT_ASSERT_VALUES_EQUAL(result.Errors, 0);
    UNIT_ASSERT_VALUES_EQUAL(pool.GetActiveSessions(), 1);
    pool.DecrementActiveCounter();
}

Y_UNIT_TEST(ShutdownBeforeReturnPreventsPooling) {
    NSessionPool::TSessionPool pool(1);
    auto session = std::make_unique<TKqpSessionCommon>("", "host:2136", true);
    UNIT_ASSERT(session->MarkActive());
    auto client = std::make_shared<TMockSessionClient>();
    UNIT_ASSERT(HandleAttachSessionState(MakeSessionShutdownState(), session.get(), client)
        == EAttachStreamReadAction::Stop);
    UNIT_ASSERT(session->GetState() == TKqpSessionCommon::S_CLOSING);
    UNIT_ASSERT(!session->MarkIdle());
    UNIT_ASSERT(!pool.ReturnSession(session.get(), false));
    UNIT_ASSERT_VALUES_EQUAL(pool.GetCurrentPoolSize(), 0);
    UNIT_ASSERT_VALUES_EQUAL(pool.GetActiveSessions(), 0);
}

Y_UNIT_TEST(RepeatedShutdownPreservesTerminalStateAndCountsOnce) {
    TTestKqpSession session("", "host:2136");
    auto client = std::make_shared<TMockSessionClient>();
    HandleAttachSessionState(MakeSessionShutdownState(), &session, client);
    HandleAttachSessionState(MakeNodeShutdownState(), &session, client);
    UNIT_ASSERT(session.GetState() == TKqpSessionCommon::S_CLOSING);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseMetrics, 1);
    UNIT_ASSERT_VALUES_EQUAL(client->LastReason, "session_shutdown");

    TTestKqpSession broken("", "host:2136");
    broken.MarkBroken();
    HandleAttachSessionState(MakeSessionShutdownState(), &broken, client);
    UNIT_ASSERT(broken.GetState() == TKqpSessionCommon::S_BROKEN);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseMetrics, 1);
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
