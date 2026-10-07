#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <atomic>
#include <thread>

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

constexpr TDuration kShortDeadline = TDuration::MilliSeconds(50);
constexpr TDuration kSlowAttach = TDuration::MilliSeconds(300);

class TDelayedMockQueryService : public Ydb::Query::V1::QueryService::Service {
public:
    TDuration AttachDelay = TDuration::Zero();
    std::atomic_uint CreateSessionRequests = 0;
    Ydb::StatusIds::StatusCode TxStatus = Ydb::StatusIds::SUCCESS;
    grpc::StatusCode TxRpcStatus = grpc::StatusCode::OK;

    grpc::Status CreateSession(
        grpc::ServerContext*,
        const Ydb::Query::CreateSessionRequest*,
        Ydb::Query::CreateSessionResponse* response) override
    {
        ++CreateSessionRequests;
        response->set_status(Ydb::StatusIds::SUCCESS);
        response->set_session_id("fake-query-session-id");
        response->set_node_id(1);
        return grpc::Status::OK;
    }

    grpc::Status AttachSession(
        grpc::ServerContext* context,
        const Ydb::Query::AttachSessionRequest*,
        grpc::ServerWriter<Ydb::Query::SessionState>* writer) override
    {
        if (AttachDelay != TDuration::Zero()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(AttachDelay.MilliSeconds()));
        }
        Ydb::Query::SessionState state;
        state.set_status(Ydb::StatusIds::SUCCESS);
        writer->Write(state);
        while (!context->IsCancelled()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return grpc::Status::OK;
    }

    grpc::Status BeginTransaction(grpc::ServerContext*, const Ydb::Query::BeginTransactionRequest*,
                                  Ydb::Query::BeginTransactionResponse* response) override {
        response->set_status(Ydb::StatusIds::SUCCESS);
        response->mutable_tx_meta()->set_id("fake-query-transaction-id");
        return grpc::Status::OK;
    }

    grpc::Status CommitTransaction(grpc::ServerContext*, const Ydb::Query::CommitTransactionRequest*,
                                   Ydb::Query::CommitTransactionResponse* response) override {
        response->set_status(TxStatus);
        return grpc::Status(TxRpcStatus, "mock transaction status");
    }

    grpc::Status RollbackTransaction(grpc::ServerContext*, const Ydb::Query::RollbackTransactionRequest*,
                                     Ydb::Query::RollbackTransactionResponse* response) override {
        response->set_status(TxStatus);
        return grpc::Status(TxRpcStatus, "mock transaction status");
    }

};

template <class TService>
std::unique_ptr<grpc::Server> StartGrpcServer(const std::string& address, TService& service) {
    return grpc::ServerBuilder()
        .AddListeningPort(TString{address}, grpc::InsecureServerCredentials())
        .RegisterService(&service)
        .BuildAndStart();
}

TCreateSessionSettings ShortDeadlineSettings() {
    return TCreateSessionSettings()
        .ClientTimeout(kShortDeadline)
        .Deadline(TDeadline::AfterDuration(kShortDeadline));
}

std::unique_ptr<TQueryClient> MakeClient(TDriver& driver, bool deferred) {
    return std::make_unique<TQueryClient>(
        driver,
        TClientSettings().SessionPoolSettings(
            TSessionPoolSettings().UseDeferredSessionCreation(deferred)));
}

} // namespace

Y_UNIT_TEST_SUITE(DeferredGetSession) {

Y_UNIT_TEST(TimeoutThenPoolWarmup) {
    NTesting::InitPortManagerFromEnv();
    const auto port = NTesting::GetFreePort();
    const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;

    TDelayedMockQueryService service;
    service.AttachDelay = kSlowAttach;
    auto server = StartGrpcServer(endpoint, service);

    TDriver driver(
        TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/My/DB"));
    auto client = MakeClient(driver, /*deferred=*/true);

    const auto result = client->GetSession(ShortDeadlineSettings()).ExtractValueSync();
    UNIT_ASSERT(!result.IsSuccess());
    UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::CLIENT_DEADLINE_EXCEEDED);

    for (int i = 0; i < 40; ++i) {
        if (client->GetCurrentPoolSize() == 1 && client->GetActiveSessionCount() == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    UNIT_ASSERT_EQUAL(client->GetCurrentPoolSize(), 1);
    UNIT_ASSERT_EQUAL(client->GetActiveSessionCount(), 0);

    client.reset();
    driver.Stop(true);
}

Y_UNIT_TEST(DisabledWaitsForAttach) {
    NTesting::InitPortManagerFromEnv();
    const auto port = NTesting::GetFreePort();
    const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;

    TDelayedMockQueryService service;
    service.AttachDelay = kSlowAttach;
    auto server = StartGrpcServer(endpoint, service);

    TDriver driver(
        TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/My/DB"));
    auto client = MakeClient(driver, /*deferred=*/false);

    const auto started = TInstant::Now();
    const auto result = client->GetSession(ShortDeadlineSettings()).ExtractValueSync();
    UNIT_ASSERT(result.IsSuccess());
    UNIT_ASSERT(!result.GetSession().GetId().empty());
    UNIT_ASSERT_GE(TInstant::Now() - started, kSlowAttach);
    UNIT_ASSERT_EQUAL(client->GetActiveSessionCount(), 1);

    client.reset();
    driver.Stop(true);
}

Y_UNIT_TEST(TransactionSessionStatus) {
    NTesting::InitPortManagerFromEnv();
    for (bool commit : {false, true}) {
        for (EStatus status : {EStatus::SUCCESS, EStatus::ABORTED, EStatus::BAD_SESSION, EStatus::CLIENT_DEADLINE_EXCEEDED}) {
            const auto port = NTesting::GetFreePort();
            const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;
            TDelayedMockQueryService service;
            if (status == EStatus::CLIENT_DEADLINE_EXCEEDED) {
                service.TxRpcStatus = grpc::StatusCode::DEADLINE_EXCEEDED;
            } else {
                service.TxStatus = static_cast<Ydb::StatusIds::StatusCode>(status);
            }
            auto server = StartGrpcServer(endpoint, service);
            TDriver driver(TDriverConfig()
                               .SetEndpoint(endpoint)
                               .SetDiscoveryMode(EDiscoveryMode::Off)
                               .SetDatabase("/Root/My/DB"));
            auto client = std::make_unique<TQueryClient>(driver, TClientSettings().SessionPoolSettings(
                                                                     TSessionPoolSettings().MaxActiveSessions(1).MinPoolSize(1)));

            TAsyncCreateSessionResult nextSession;
            {
                auto sessionFuture = client->GetSession();
                UNIT_ASSERT(sessionFuture.Wait(TDuration::Seconds(10)));
                auto sessionResult = sessionFuture.ExtractValueSync();
                UNIT_ASSERT(sessionResult.IsSuccess());
                auto session = sessionResult.GetSession();
                auto beginFuture = session.BeginTransaction(TTxSettings::SerializableRW());
                UNIT_ASSERT(beginFuture.Wait(TDuration::Seconds(10)));
                auto beginResult = beginFuture.ExtractValueSync();
                UNIT_ASSERT(beginResult.IsSuccess());
                auto transaction = beginResult.GetTransaction();
                auto operation = commit ? transaction.Commit().Apply([](auto future) {
                    return TStatus(future.ExtractValue());
                })
                                        : transaction.Rollback();
                UNIT_ASSERT(operation.Wait(TDuration::Seconds(10)));
                UNIT_ASSERT_VALUES_EQUAL(operation.ExtractValueSync().GetStatus(), status);
                nextSession = client->GetSession();
                UNIT_ASSERT(!nextSession.HasValue());
            }
            UNIT_ASSERT(nextSession.Wait(TDuration::Seconds(10)));
            {
                auto result = nextSession.ExtractValueSync();
                UNIT_ASSERT(result.IsSuccess());
                UNIT_ASSERT_VALUES_EQUAL(client->GetActiveSessionCount(), 1);
                const bool reuse = status == EStatus::SUCCESS || status == EStatus::ABORTED;
                UNIT_ASSERT_VALUES_EQUAL(service.CreateSessionRequests.load(), reuse ? 1u : 2u);
            }
            client.reset();
            driver.Stop(true);
        }
    }
}

}
