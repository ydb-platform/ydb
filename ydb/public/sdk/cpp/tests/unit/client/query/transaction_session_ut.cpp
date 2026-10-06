#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

using namespace NYdb;

namespace {

    struct TOutcome {
        const char* Name;
        grpc::StatusCode TransportStatus;
        Ydb::StatusIds::StatusCode ServerStatus;
        EStatus ExpectedStatus;
        bool ReuseSession;
        bool CloseHint = false;
    };

    constexpr TOutcome Outcomes[] = {
        {"success", grpc::StatusCode::OK, Ydb::StatusIds::SUCCESS, EStatus::SUCCESS, true},
        {"aborted", grpc::StatusCode::OK, Ydb::StatusIds::ABORTED, EStatus::ABORTED, true},
        {"bad_session", grpc::StatusCode::OK, Ydb::StatusIds::BAD_SESSION, EStatus::BAD_SESSION, false},
        {"session_busy", grpc::StatusCode::OK, Ydb::StatusIds::SESSION_BUSY, EStatus::SESSION_BUSY, false},
        {"cancelled", grpc::StatusCode::CANCELLED, Ydb::StatusIds::SUCCESS, EStatus::CLIENT_CANCELLED, false},
        {"deadline", grpc::StatusCode::DEADLINE_EXCEEDED, Ydb::StatusIds::SUCCESS, EStatus::CLIENT_DEADLINE_EXCEEDED, false},
        {"unavailable", grpc::StatusCode::UNAVAILABLE, Ydb::StatusIds::SUCCESS, EStatus::TRANSPORT_UNAVAILABLE, false},
        {"close_hint", grpc::StatusCode::OK, Ydb::StatusIds::SUCCESS, EStatus::SUCCESS, false, true},
    };

    constexpr const char* MetadataKey = "x-transaction-test";
    constexpr const char* MetadataValue = "preserved";
    constexpr const char* ResponseIssue = "transaction response issue";

    grpc::Status FinishResponse(grpc::ServerContext* context, const TOutcome& outcome) {
        context->AddTrailingMetadata(MetadataKey, MetadataValue);
        if (outcome.CloseHint) {
            context->AddTrailingMetadata("x-ydb-server-hints", "session-close");
        }
        return grpc::Status(outcome.TransportStatus, outcome.Name);
    }

    class TMockQueryService final: public Ydb::Query::V1::QueryService::Service {
    public:
        explicit TMockQueryService(const TOutcome& outcome)
            : Outcome_(outcome)
        {
        }

        grpc::Status CreateSession(grpc::ServerContext*, const Ydb::Query::CreateSessionRequest*,
                                   Ydb::Query::CreateSessionResponse* response) override {
            response->set_status(Ydb::StatusIds::SUCCESS);
            response->set_session_id("query-session-" + std::to_string(++SessionId_));
            response->set_node_id(1);
            return grpc::Status::OK;
        }

        grpc::Status AttachSession(grpc::ServerContext*, const Ydb::Query::AttachSessionRequest*,
                                   grpc::ServerWriter<Ydb::Query::SessionState>* writer) override {
            Ydb::Query::SessionState state;
            state.set_status(Ydb::StatusIds::SUCCESS);
            writer->Write(state);
            // Keep the stream open until teardown, including after client cancellation.
            std::unique_lock lock(AttachMutex_);
            AttachStopped_.wait(lock, [this] { return StopAttach_; });
            return grpc::Status::OK;
        }

        void ReleaseAttachSessions() {
            {
                std::lock_guard lock(AttachMutex_);
                StopAttach_ = true;
            }
            AttachStopped_.notify_all();
        }

        grpc::Status BeginTransaction(grpc::ServerContext*, const Ydb::Query::BeginTransactionRequest*,
                                      Ydb::Query::BeginTransactionResponse* response) override {
            response->set_status(Ydb::StatusIds::SUCCESS);
            response->mutable_tx_meta()->set_id("query-transaction");
            return grpc::Status::OK;
        }

        grpc::Status CommitTransaction(grpc::ServerContext* context, const Ydb::Query::CommitTransactionRequest*,
                                       Ydb::Query::CommitTransactionResponse* response) override {
            response->set_status(Outcome_.ServerStatus);
            response->add_issues()->set_message(ResponseIssue);
            if (Outcome_.ServerStatus == Ydb::StatusIds::SUCCESS) {
                response->mutable_commit_timestamp()->set_plan_step(7);
                response->mutable_commit_timestamp()->set_tx_id(42);
            }
            return FinishResponse(context, Outcome_);
        }

        grpc::Status RollbackTransaction(grpc::ServerContext* context, const Ydb::Query::RollbackTransactionRequest*,
                                         Ydb::Query::RollbackTransactionResponse* response) override {
            response->set_status(Outcome_.ServerStatus);
            response->add_issues()->set_message(ResponseIssue);
            return FinishResponse(context, Outcome_);
        }

    private:
        const TOutcome& Outcome_;
        std::atomic_uint SessionId_ = 0;
        std::mutex AttachMutex_;
        std::condition_variable AttachStopped_;
        bool StopAttach_ = false;
    };

    class TMockTableService final: public Ydb::Table::V1::TableService::Service {
    public:
        explicit TMockTableService(const TOutcome& outcome)
            : Outcome_(outcome)
        {
        }

        grpc::Status CreateSession(grpc::ServerContext*, const Ydb::Table::CreateSessionRequest*,
                                   Ydb::Table::CreateSessionResponse* response) override {
            Ydb::Table::CreateSessionResult result;
            result.set_session_id("table-session-" + std::to_string(++SessionId_));
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
            operation->mutable_result()->PackFrom(result);
            return grpc::Status::OK;
        }

        grpc::Status DeleteSession(grpc::ServerContext*, const Ydb::Table::DeleteSessionRequest*,
                                   Ydb::Table::DeleteSessionResponse* response) override {
            response->mutable_operation()->set_ready(true);
            response->mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
            return grpc::Status::OK;
        }

        grpc::Status BeginTransaction(grpc::ServerContext*, const Ydb::Table::BeginTransactionRequest*,
                                      Ydb::Table::BeginTransactionResponse* response) override {
            Ydb::Table::BeginTransactionResult result;
            result.mutable_tx_meta()->set_id("table-transaction");
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
            operation->mutable_result()->PackFrom(result);
            return grpc::Status::OK;
        }

        grpc::Status CommitTransaction(grpc::ServerContext* context, const Ydb::Table::CommitTransactionRequest*,
                                       Ydb::Table::CommitTransactionResponse* response) override {
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Outcome_.ServerStatus);
            operation->add_issues()->set_message(ResponseIssue);
            Ydb::Table::CommitTransactionResult result;
            result.mutable_query_stats()->set_total_duration_us(123);
            operation->mutable_result()->PackFrom(result);
            return FinishResponse(context, Outcome_);
        }

        grpc::Status RollbackTransaction(grpc::ServerContext* context, const Ydb::Table::RollbackTransactionRequest*,
                                         Ydb::Table::RollbackTransactionResponse* response) override {
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Outcome_.ServerStatus);
            operation->add_issues()->set_message(ResponseIssue);
            return FinishResponse(context, Outcome_);
        }

    private:
        const TOutcome& Outcome_;
        std::atomic_uint SessionId_ = 0;
    };

    class TFixture {
    public:
        explicit TFixture(const TOutcome& outcome)
            : QueryService_(outcome)
            , TableService_(outcome)
            , Port_(NTesting::GetFreePort())
        {
            const auto endpoint = TStringBuilder() << "127.0.0.1:" << Port_;
            Server_ = grpc::ServerBuilder()
                          .AddListeningPort(endpoint, grpc::InsecureServerCredentials())
                          .RegisterService(&QueryService_)
                          .RegisterService(&TableService_)
                          .BuildAndStart();
            UNIT_ASSERT(Server_);
            Driver = std::make_unique<TDriver>(TDriverConfig()
                                                   .SetEndpoint(endpoint)
                                                   .SetDiscoveryMode(EDiscoveryMode::Off)
                                                   .SetDatabase("/Root/My/DB"));
        }

        ~TFixture() {
            QueryService_.ReleaseAttachSessions();
            Driver.reset();
            Server_->Shutdown();
        }

        std::unique_ptr<TDriver> Driver;

    private:
        TMockQueryService QueryService_;
        TMockTableService TableService_;
        NTesting::TPortHolder Port_;
        std::unique_ptr<grpc::Server> Server_;
    };

    template <class TResult>
    TResult WaitForResult(NThreading::TFuture<TResult> future) {
        UNIT_ASSERT(future.Wait(TDuration::Seconds(10)));
        return future.ExtractValueSync();
    }

    void CheckCommitPayload(const NQuery::TCommitTransactionResult& result) {
        UNIT_ASSERT(result.GetCommitTimestamp());
        UNIT_ASSERT_VALUES_EQUAL(result.GetCommitTimestamp()->PlanStep, 7);
        UNIT_ASSERT_VALUES_EQUAL(result.GetCommitTimestamp()->TxId, 42);
    }

    void CheckCommitPayload(const NTable::TCommitTransactionResult& result) {
        UNIT_ASSERT(result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(result.GetStats()->GetTotalDurationUs(), 123);
    }

    void CheckStatus(const TStatus& result, const TOutcome& outcome) {
        UNIT_ASSERT_C(result.GetStatus() == outcome.ExpectedStatus, outcome.Name);
        UNIT_ASSERT(!result.GetEndpoint().empty());
        if (outcome.TransportStatus == grpc::StatusCode::OK) {
            const auto& metadata = result.GetResponseMetadata();
            const auto it = metadata.find(MetadataKey);
            UNIT_ASSERT_C(it != metadata.end(), outcome.Name);
            UNIT_ASSERT_VALUES_EQUAL(it->second, MetadataValue);
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), ResponseIssue);
        }
    }

    template <class TClient, class TTxSettings>
    void CheckTransactionSession(const TOutcome& outcome, bool commit) {
        TFixture fixture(outcome);
        using TSettings = typename TClient::TSettings;
        using TPoolSettings = typename TSettings::TSessionPoolSettings;
        TClient client(*fixture.Driver, TSettings().SessionPoolSettings(
                                            TPoolSettings().MaxActiveSessions(1).MinPoolSize(1)));

        std::string originalId;
        typename TClient::TAsyncCreateSessionResult nextSession;
        {
            auto sessionResult = WaitForResult(client.GetSession());
            UNIT_ASSERT(sessionResult.IsSuccess());
            auto session = sessionResult.GetSession();
            originalId = session.GetId();
            auto beginResult = WaitForResult(session.BeginTransaction(TTxSettings::SerializableRW()));
            UNIT_ASSERT(beginResult.IsSuccess());
            auto transaction = beginResult.GetTransaction();
            if (commit) {
                auto result = WaitForResult(transaction.Commit());
                CheckStatus(result, outcome);
                if (result.IsSuccess()) {
                    CheckCommitPayload(result);
                }
            } else {
                CheckStatus(WaitForResult(transaction.Rollback()), outcome);
            }
            UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
            nextSession = client.GetSession();
            UNIT_ASSERT(!nextSession.HasValue());
        }

        // Completion proves that destruction restored the only pool slot, even if
        // the transaction's coroutine retained its session after completing the RPC.
        auto nextResult = WaitForResult(std::move(nextSession));
        UNIT_ASSERT_C(nextResult.IsSuccess(), outcome.Name);
        UNIT_ASSERT_C((nextResult.GetSession().GetId() == originalId) == outcome.ReuseSession, outcome.Name);
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
    }

} // namespace

Y_UNIT_TEST_SUITE(TransactionSessionStatus) {

    Y_UNIT_TEST(QueryCommit) {
        NTesting::InitPortManagerFromEnv();
        for (const auto& outcome : Outcomes) {
            CheckTransactionSession<NQuery::TQueryClient, NQuery::TTxSettings>(outcome, true);
        }
    }

    Y_UNIT_TEST(QueryRollback) {
        NTesting::InitPortManagerFromEnv();
        for (const auto& outcome : Outcomes) {
            CheckTransactionSession<NQuery::TQueryClient, NQuery::TTxSettings>(outcome, false);
        }
    }

    Y_UNIT_TEST(TableCommit) {
        NTesting::InitPortManagerFromEnv();
        for (const auto& outcome : Outcomes) {
            CheckTransactionSession<NTable::TTableClient, NTable::TTxSettings>(outcome, true);
        }
    }

    Y_UNIT_TEST(TableRollback) {
        NTesting::InitPortManagerFromEnv();
        for (const auto& outcome : Outcomes) {
            CheckTransactionSession<NTable::TTableClient, NTable::TTxSettings>(outcome, false);
        }
    }

} // Y_UNIT_TEST_SUITE(TransactionSessionStatus)
