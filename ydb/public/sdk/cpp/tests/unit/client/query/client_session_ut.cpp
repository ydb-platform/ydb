#include <ydb/public/sdk/cpp/src/client/query/impl/session_state_handler.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/session/session_pool.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>

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
