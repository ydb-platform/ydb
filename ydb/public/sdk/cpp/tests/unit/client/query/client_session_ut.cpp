#include <ydb/public/sdk/cpp/src/client/query/impl/session_state_handler.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/plain_status/status.h>
#include <ydb/public/sdk/cpp/src/client/impl/session/session_pool.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>

#include <map>
#include <optional>
#include <string_view>
#include <vector>

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
        CloseReasons.emplace_back(reason);
    }

    std::uint64_t PessimizedNodeId = 0;
    int PessimizeCalls = 0;
    std::vector<std::string> CloseReasons;
};

class TDeletingMockSessionClient : public TMockSessionClient {
public:
    explicit TDeletingMockSessionClient(NSessionPool::TSessionPool* pool = nullptr)
        : Pool_(pool)
    {
    }

    void DeleteSession(TKqpSessionCommon* session) override {
        delete session;
    }

    void RecordSessionClosed(std::string_view reason) override {
        if (Pool_) {
            PoolSizeWhenRecorded = Pool_->GetCurrentPoolSize();
        }
        TMockSessionClient::RecordSessionClosed(reason);
    }

    std::optional<std::int64_t> PoolSizeWhenRecorded;

private:
    NSessionPool::TSessionPool* Pool_;
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

void ApplySessionStatus(TKqpSessionCommon& session,
    const std::shared_ptr<ISessionClient>& client,
    TStatus status)
{
    NSessionPool::InjectSessionStatusInterception(
        std::shared_ptr<TKqpSessionCommon>(&session, [](TKqpSessionCommon*) {}),
        NThreading::MakeFuture<TStatus>(std::move(status)),
        false,
        TDuration::Zero(),
        {},
        client
    ).GetValueSync();
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
    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.front(), "session_shutdown");
}

Y_UNIT_TEST(SessionShutdownIdleInPoolRemovesBeforeRecording) {
    NSessionPool::TSessionPool pool(1);
    auto client = std::make_shared<TDeletingMockSessionClient>(&pool);
    auto* session = new TTestKqpSession(MakeSessionIdWithNodeId(42), "host:2136");
    session->MarkIdle();
    UNIT_ASSERT(pool.ReturnSession(session, false));

    UNIT_ASSERT(HandleAttachSessionState(MakeSessionShutdownState(), session, client)
        == EAttachStreamReadAction::Stop);
    UNIT_ASSERT(client->PoolSizeWhenRecorded.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*client->PoolSizeWhenRecorded, 0);
    UNIT_ASSERT_VALUES_EQUAL(pool.GetCurrentPoolSize(), 0);
    UNIT_ASSERT_VALUES_EQUAL(client->PessimizeCalls, 0);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.front(), "session_shutdown");
}

Y_UNIT_TEST(NodeShutdownActiveSessionMarksClosingAndPessimizesNode) {
    TTestKqpSession session(MakeSessionIdWithNodeId(42), "host:2136");
    auto client = std::make_shared<TMockSessionClient>();

    UNIT_ASSERT(HandleAttachSessionState(MakeNodeShutdownState(), &session, client)
        == EAttachStreamReadAction::Stop);
    UNIT_ASSERT(session.GetState() == TKqpSessionCommon::S_CLOSING);
    UNIT_ASSERT_VALUES_EQUAL(client->PessimizeCalls, 1);
    UNIT_ASSERT_VALUES_EQUAL(client->PessimizedNodeId, 42U);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.front(), "node_shutdown");
}

Y_UNIT_TEST(NodeShutdownWithZeroNodeIdSkipsPessimization) {
    TTestKqpSession session("", "host:2136");
    auto client = std::make_shared<TMockSessionClient>();

    UNIT_ASSERT(session.GetEndpointKey().GetNodeId() == 0U);
    UNIT_ASSERT(HandleAttachSessionState(MakeNodeShutdownState(), &session, client)
        == EAttachStreamReadAction::Stop);
    UNIT_ASSERT(session.GetState() == TKqpSessionCommon::S_CLOSING);
    UNIT_ASSERT_VALUES_EQUAL(client->PessimizeCalls, 0);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.front(), "node_shutdown");
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

Y_UNIT_TEST(SessionStatusReasonsMatchContract) {
    const std::vector<std::pair<EStatus, std::string>> cases = {
        {EStatus::CLIENT_DEADLINE_EXCEEDED, "client_timeout"},
        {EStatus::CLIENT_CANCELLED, "client_cancelled"},
        {EStatus::TRANSPORT_UNAVAILABLE, "transport_error"},
        {EStatus::SESSION_BUSY, "session_busy"},
        {EStatus::BAD_SESSION, "bad_session"},
        {EStatus::SESSION_EXPIRED, "bad_session"},
    };

    for (const auto& [status, expectedReason] : cases) {
        TTestKqpSession session(MakeSessionIdWithNodeId(42), "host:2136");
        auto client = std::make_shared<TMockSessionClient>();

        ApplySessionStatus(session, client, TStatus(status, {}));

        UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.front(), expectedReason);
    }
}

Y_UNIT_TEST(SessionCloseHintUsesShutdownReason) {
    std::multimap<std::string, std::string> metadata;
    metadata.emplace(NYdb::YDB_SERVER_HINTS, NYdb::YDB_SESSION_CLOSE);
    TTestKqpSession session(MakeSessionIdWithNodeId(42), "host:2136");
    auto client = std::make_shared<TMockSessionClient>();

    ApplySessionStatus(session, client,
        TStatus(TPlainStatus(EStatus::SUCCESS, {}, {}, std::move(metadata))));

    UNIT_ASSERT(session.GetState() == TKqpSessionCommon::S_CLOSING);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.front(), "session_shutdown");
}

Y_UNIT_TEST(ExcludedTransportStatusesKeepSessionActive) {
    for (const auto status : {EStatus::CLIENT_RESOURCE_EXHAUSTED, EStatus::CLIENT_OUT_OF_RANGE}) {
        TTestKqpSession session(MakeSessionIdWithNodeId(42), "host:2136");
        auto client = std::make_shared<TMockSessionClient>();

        ApplySessionStatus(session, client, TStatus(status, {}));

        UNIT_ASSERT(session.GetState() == TKqpSessionCommon::S_ACTIVE);
        UNIT_ASSERT(client->CloseReasons.empty());
    }
}

Y_UNIT_TEST(FirstTerminalReasonWins) {
    TTestKqpSession session(MakeSessionIdWithNodeId(42), "host:2136");
    auto client = std::make_shared<TMockSessionClient>();

    ApplySessionStatus(session, client, TStatus(EStatus::BAD_SESSION, {}));
    ApplySessionStatus(session, client, TStatus(EStatus::TRANSPORT_UNAVAILABLE, {}));
    HandleAttachSessionState(MakeNodeShutdownState(), &session, client);

    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.front(), "bad_session");
}

Y_UNIT_TEST(PoolIdleRemovalUsesIdleTimeoutReason) {
    NSessionPool::TSessionPool pool(1);
    auto client = std::make_shared<TDeletingMockSessionClient>();
    auto* session = new TTestKqpSession(MakeSessionIdWithNodeId(42), "host:2136");
    session->MarkIdle();
    session->ScheduleTimeToTouchFast(TDuration::Zero(), true);
    UNIT_ASSERT(pool.ReturnSession(session, false));

    auto periodic = pool.CreatePeriodicTask(
        client,
        {},
        [](TKqpSessionCommon*, size_t) {
            return true;
        });

    UNIT_ASSERT(periodic({}, EStatus::SUCCESS));
    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(client->CloseReasons.front(), "pool_idle_timeout");
}

}
