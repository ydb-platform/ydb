#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/common/network.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <thread>
#include <utility>

using namespace NYdb;

namespace {
    /**
     * The mock for the table service in the YDB public API.
     */
    class TMockTableService : public Ydb::Table::V1::TableService::Service {
    public:
        virtual grpc::Status CreateSession(
            grpc::ServerContext* /* context */,
            const Ydb::Table::CreateSessionRequest* request,
            Ydb::Table::CreateSessionResponse* response
        ) override {
            std::cerr << "CreateSession():" << std::endl
                << request->DebugString()
                << std::endl;

            // Complete the request successfully with a fake session ID
            //
            // NOTE: This method needs to be mocked to allow the test code to create
            //       a new API session. The test code must call CreateSession()
            //       before calling any other methods, like CreateTable() or AlterTable().
            //       And CreateSession() must see a successful response from the server
            //       in order to create a valid session.
            ++CreateSessionRequests;
            Ydb::Table::CreateSessionResult result;
            result.set_session_id("fake-session-id");

            auto op = response->mutable_operation();
            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);
            op->mutable_result()->PackFrom(result);

            return grpc::Status::OK;
        }

        virtual grpc::Status CreateTable(
            grpc::ServerContext* /* context */,
            const Ydb::Table::CreateTableRequest* request,
            Ydb::Table::CreateTableResponse* response
        ) override {
            std::cerr << "CreateTable():" << std::endl
                << request->DebugString()
                << std::endl;

            //

            if (CreateTableStarted) {
                CreateTableStarted->set_value();
                ContinueCreateTable.wait();
            }

            auto op = response->mutable_operation();

            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);

            // Save the CreateTable request to allow the test to verify it
            LastCreateTableRequest = Ydb::Table::CreateTableRequest(*request);
            return grpc::Status::OK;
        }

        virtual grpc::Status DeleteSession(
            grpc::ServerContext* /* context */,
            const Ydb::Table::DeleteSessionRequest* request,
            Ydb::Table::DeleteSessionResponse* response
        ) override {
            std::cerr << "DeleteSession():" << std::endl
                << request->DebugString()
                << std::endl;

            ++DeleteSessionRequests;

            auto op = response->mutable_operation();
            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);

            return grpc::Status::OK;
        }

        virtual grpc::Status AlterTable(
            grpc::ServerContext* /* context */,
            const Ydb::Table::AlterTableRequest* request,
            Ydb::Table::AlterTableResponse* response
        ) override {
            std::cerr << "AlterTable():" << std::endl
                << request->DebugString()
                << std::endl;

            //

            auto op = response->mutable_operation();

            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);

            // Save the AlterTable request to allow the test to verify it
            LastAlterTableRequest = Ydb::Table::AlterTableRequest(*request);
            return grpc::Status::OK;
        }

        grpc::Status BeginTransaction(grpc::ServerContext*, const Ydb::Table::BeginTransactionRequest*,
                                      Ydb::Table::BeginTransactionResponse* response) override {
            Ydb::Table::BeginTransactionResult result;
            result.mutable_tx_meta()->set_id("fake-transaction-id");
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
            operation->mutable_result()->PackFrom(result);
            return grpc::Status::OK;
        }

        template <class TResponse>
        grpc::Status FinishTransaction(TResponse* response) {
            response->mutable_operation()->set_ready(true);
            response->mutable_operation()->set_status(TransactionStatus);
            return grpc::Status(TransactionGrpcStatus, "transaction response");
        }

        grpc::Status CommitTransaction(grpc::ServerContext*, const Ydb::Table::CommitTransactionRequest*,
                                       Ydb::Table::CommitTransactionResponse* response) override {
            return FinishTransaction(response);
        }

        grpc::Status RollbackTransaction(grpc::ServerContext*, const Ydb::Table::RollbackTransactionRequest*,
                                         Ydb::Table::RollbackTransactionResponse* response) override {
            return FinishTransaction(response);
        }
        Ydb::StatusIds::StatusCode TransactionStatus = Ydb::StatusIds::SUCCESS;
        grpc::StatusCode TransactionGrpcStatus = grpc::StatusCode::OK;
        std::atomic_uint CreateSessionRequests = 0;
        std::optional<Ydb::Table::CreateTableRequest> LastCreateTableRequest;
        std::optional<Ydb::Table::AlterTableRequest> LastAlterTableRequest;
        std::atomic_uint DeleteSessionRequests = 0;
        std::promise<void>* CreateTableStarted = nullptr;
        std::shared_future<void> ContinueCreateTable;
    };

    /**
     * Start the local GRPC server for the given API service.
     *
     * @tparam TService The type of the API service
     *
     * @param[in] address The address/port to listen to
     * @param[in] service The API service to start
     *
     * @return The corresponding GRPC server
     */
    template<class TService>
    std::unique_ptr<grpc::Server> StartGrpcServer(const std::string& address, TService& service) {
        return grpc::ServerBuilder()
            .AddListeningPort(TString{address}, grpc::InsecureServerCredentials())
            .RegisterService(&service)
            .BuildAndStart();
    }

    template<class TPredicate>
    bool WaitUntil(TPredicate&& predicate, std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return predicate();
    }

    /**
     * Configure and start a local GRPC server with the mocked table API service.
     *
     * @param[in] tableService The table service to start
     * @param[out] grpcServer Receives the corresponding GRPC server
     * @param[out] driver Receives the connection pool to the server
     * @param[out] tableClient Receives the API client for the table API service
     * @param[out] tableSession Receives the client session for the table API service
     */
    void StartServerWithTableService(
        TMockTableService& tableService,
        std::unique_ptr<grpc::Server>& grpcServer,
        std::unique_ptr<TDriver>& driver,
        std::unique_ptr<NTable::TTableClient>& tableClient,
        std::unique_ptr<NTable::TSession>& tableSession
    ) {
        // Start the local GRPC service for the given table API service
        NTesting::InitPortManagerFromEnv();
        const auto tablePortHolder = NTesting::GetFreePort();
        const ui16 tablePort = static_cast<ui16>(tablePortHolder);

        grpcServer = StartGrpcServer(
            TStringBuilder() << "127.0.0.1:" << tablePort,
            tableService
        );

        // Start the connection pool and create the API client for the table API service
        driver = std::make_unique<TDriver>(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << tablePort)
                .SetDiscoveryMode(EDiscoveryMode::Off)
                .SetDatabase("/Root/My/DB")
        );

        // Create a new session
        tableClient = std::make_unique<NTable::TTableClient>(*driver);

        auto sessionFuture = tableClient->CreateSession();
        ASSERT_TRUE(sessionFuture.Wait(TDuration::Seconds(10)));

        auto sessionResult = sessionFuture.ExtractValueSync();
        ASSERT_TRUE(sessionResult.IsSuccess());

        tableSession = std::make_unique<NTable::TSession>(sessionResult.GetSession());
    }

} // namespace <anonymous>

TEST(TableTest, FulltextSuperLemmerAnalyzerRoundTrip) {
    NTable::TFulltextIndexSettings settings;
    NTable::TFulltextIndexSettings::TColumnAnalyzers column;
    column.Column = "Text";
    column.Analyzers = NTable::TFulltextIndexSettings::TAnalyzers::SuperLemmer("russian");
    settings.Columns.push_back(column);

    Ydb::Table::FulltextIndexSettings proto;
    settings.SerializeTo(proto);
    ASSERT_EQ(proto.columns_size(), 1);
    ASSERT_TRUE(proto.columns(0).has_analyzers());
    ASSERT_TRUE(proto.columns(0).analyzers().use_filter_superlemmer());

    const auto restored = NTable::TFulltextIndexSettings::FromProto(proto);
    ASSERT_EQ(restored.Columns.size(), 1);
    ASSERT_TRUE(restored.Columns[0].Analyzers.has_value());
    const auto& analyzers = *restored.Columns[0].Analyzers;
    ASSERT_EQ(analyzers.Language.value_or(""), "russian");
    ASSERT_TRUE(analyzers.UseFilterLowercase.value_or(false));
    ASSERT_TRUE(analyzers.UseFilterStopwords.value_or(false));
    ASSERT_TRUE(analyzers.UseFilterSuperLemmer.value_or(false));
    ASSERT_NE(ToString(restored).find("use_filter_superlemmer: true"), TString::npos);
}

TEST(TableTest, TransactionSessionStatus) {
    NTesting::InitPortManagerFromEnv();
    for (const bool commit : {false, true}) {
        for (const auto status : {EStatus::SUCCESS, EStatus::ABORTED, EStatus::BAD_SESSION, EStatus::CLIENT_DEADLINE_EXCEEDED}) {
            SCOPED_TRACE(::testing::Message() << "commit=" << commit << ", status=" << static_cast<int>(status));
            TMockTableService service;
            service.TransactionStatus = status == EStatus::CLIENT_DEADLINE_EXCEEDED
                                            ? Ydb::StatusIds::SUCCESS
                                            : static_cast<Ydb::StatusIds::StatusCode>(status);
            service.TransactionGrpcStatus = status == EStatus::CLIENT_DEADLINE_EXCEEDED
                                                ? grpc::StatusCode::DEADLINE_EXCEEDED
                                                : grpc::StatusCode::OK;
            const auto port = NTesting::GetFreePort();
            const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;
            auto server = StartGrpcServer(endpoint, service);
            ASSERT_TRUE(server);
            TDriver driver(TDriverConfig().SetEndpoint(endpoint).SetDiscoveryMode(EDiscoveryMode::Off).SetDatabase("/Root/My/DB"));
            NTable::TTableClient client(driver, NTable::TClientSettings().SessionPoolSettings(
                                                    NTable::TSessionPoolSettings().MaxActiveSessions(1).MinPoolSize(1)));
            NTable::TAsyncCreateSessionResult nextSession;
            {
                auto sessionFuture = client.GetSession();
                ASSERT_TRUE(sessionFuture.Wait(TDuration::Seconds(10)));
                auto sessionResult = sessionFuture.ExtractValueSync();
                ASSERT_TRUE(sessionResult.IsSuccess());
                auto session = sessionResult.GetSession();
                auto beginFuture = session.BeginTransaction(NTable::TTxSettings::SerializableRW());
                ASSERT_TRUE(beginFuture.Wait(TDuration::Seconds(10)));
                auto beginResult = beginFuture.ExtractValueSync();
                ASSERT_TRUE(beginResult.IsSuccess());
                auto transaction = beginResult.GetTransaction();
                auto result = commit ? transaction.Commit().Apply([](auto future) {
                    return TStatus(future.ExtractValue());
                })
                                     : transaction.Rollback();
                ASSERT_TRUE(result.Wait(TDuration::Seconds(10)));
                ASSERT_EQ(result.ExtractValueSync().GetStatus(), status);
                ASSERT_EQ(client.GetActiveSessionCount(), 1);
                nextSession = client.GetSession();
                ASSERT_FALSE(nextSession.IsReady());
            }
            ASSERT_TRUE(nextSession.Wait(TDuration::Seconds(10)));
            auto nextResult = nextSession.ExtractValueSync();
            ASSERT_TRUE(nextResult.IsSuccess());
            ASSERT_EQ(client.GetActiveSessionCount(), 1);
            ASSERT_EQ(service.CreateSessionRequests.load(), status == EStatus::SUCCESS || status == EStatus::ABORTED ? 1u : 2u);
        }
    }
}

TEST(TableTest, SessionHandleDestructionSendsDeleteSession) {
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driver;
    std::unique_ptr<NTable::TTableClient> tableClient;
    std::unique_ptr<NTable::TSession> tableSession;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driver,
        tableClient,
        tableSession
    );

    tableSession.reset();
    ASSERT_TRUE(WaitUntil([&] {
        return tableService.DeleteSessionRequests.load() == 1u;
    }));

    tableClient.reset();
    driver.reset();
}

TEST(TableTest, ClientDestructorSendsDeleteSessionForPooledSessions) {
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driver;
    std::unique_ptr<NTable::TTableClient> tableClient;
    std::unique_ptr<NTable::TSession> tableSession;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driver,
        tableClient,
        tableSession
    );

    tableSession.reset();
    ASSERT_TRUE(WaitUntil([&] {
        return tableService.DeleteSessionRequests.load() == 1u;
    }));
    tableService.DeleteSessionRequests.store(0);

    {
        auto pooledSessionResult = tableClient->GetSession().ExtractValueSync();
        ASSERT_TRUE(pooledSessionResult.IsSuccess());
        auto pooledSession = pooledSessionResult.GetSession();
    }

    tableClient.reset();
    ASSERT_TRUE(WaitUntil([&] {
        return tableService.DeleteSessionRequests.load() == 1u;
    }));

    driver.reset();
}

TEST(TableTest, ExplicitStopClosesPooledSessions) {
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driver;
    std::unique_ptr<NTable::TTableClient> tableClient;
    std::unique_ptr<NTable::TSession> tableSession;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driver,
        tableClient,
        tableSession
    );

    tableSession.reset();
    ASSERT_TRUE(WaitUntil([&] {
        return tableService.DeleteSessionRequests.load() == 1u;
    }));
    tableService.DeleteSessionRequests.store(0);

    {
        auto pooledSessionResult = tableClient->GetSession().ExtractValueSync();
        ASSERT_TRUE(pooledSessionResult.IsSuccess());
        auto pooledSession = pooledSessionResult.GetSession();
    }

    ASSERT_EQ(tableService.DeleteSessionRequests.load(), 0u);

    ASSERT_TRUE(tableClient->Stop().Wait(TDuration::Seconds(10)));
    ASSERT_EQ(tableService.DeleteSessionRequests.load(), 1u);
}

TEST(TableTest, CheckedOutPooledSessionClosesRemotelyAfterExplicitStop) {
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driver;
    std::unique_ptr<NTable::TTableClient> tableClient;
    std::unique_ptr<NTable::TSession> tableSession;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driver,
        tableClient,
        tableSession
    );

    tableSession.reset();
    ASSERT_TRUE(WaitUntil([&] {
        return tableService.DeleteSessionRequests.load() == 1u;
    }));
    tableService.DeleteSessionRequests.store(0);

    {
        auto pooledSessionResult = tableClient->GetSession().ExtractValueSync();
        ASSERT_TRUE(pooledSessionResult.IsSuccess());
        auto pooledSession = pooledSessionResult.GetSession();

        ASSERT_TRUE(tableClient->Stop().Wait(TDuration::Seconds(10)));
        ASSERT_EQ(tableService.DeleteSessionRequests.load(), 0u);
    }

    ASSERT_TRUE(WaitUntil([&] {
        return tableService.DeleteSessionRequests.load() == 1u;
    }));
}

TEST(TableTest, DriverStopFromResponseCallbackRunsStopNotifications) {
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driver;
    std::unique_ptr<NTable::TTableClient> tableClient;
    std::unique_ptr<NTable::TSession> tableSession;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driver,
        tableClient,
        tableSession
    );

    {
        auto pooledSessionResult = tableClient->GetSession().ExtractValueSync();
        ASSERT_TRUE(pooledSessionResult.IsSuccess());
        auto pooledSession = pooledSessionResult.GetSession();
    }

    std::promise<void> createTableStarted;
    auto createTableStartedFuture = createTableStarted.get_future();
    std::promise<void> continueCreateTable;
    tableService.CreateTableStarted = &createTableStarted;
    tableService.ContinueCreateTable = continueCreateTable.get_future().share();

    std::promise<void> callbackDone;
    auto callbackDoneFuture = callbackDone.get_future();
    std::atomic_bool success = false;

    auto requestFuture = tableSession->CreateTable(
        "/Root/My/DB/driver_stop_from_callback",
        NTable::TTableBuilder().Build()
    );

    ASSERT_EQ(createTableStartedFuture.wait_for(std::chrono::seconds(10)), std::future_status::ready);

    requestFuture.Subscribe([&](const NThreading::TFuture<TStatus>& future) mutable {
        success.store(future.GetValue().IsSuccess());
        driver->Stop(true);
        callbackDone.set_value();
    });

    continueCreateTable.set_value();

    ASSERT_EQ(callbackDoneFuture.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    ASSERT_TRUE(success.load());

    ASSERT_TRUE(WaitUntil([&] {
        return tableService.DeleteSessionRequests.load() >= 1u;
    }));
    ASSERT_TRUE(WaitUntil([&] {
        auto stoppedSessionResult = tableClient->CreateSession().ExtractValueSync();
        return stoppedSessionResult.GetStatus() == EStatus::CLIENT_CANCELLED;
    }));
}

TEST(TableTest, DriverStopDoesNotAffectOtherDriver) {
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driverB;
    std::unique_ptr<NTable::TTableClient> tableClientB;
    std::unique_ptr<NTable::TSession> tableSessionB;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driverB,
        tableClientB,
        tableSessionB
    );

    TDriver driverA(driverB->GetConfig());
    NTable::TTableClient tableClientA(driverA);

    std::promise<void> createTableStarted;
    auto createTableStartedFuture = createTableStarted.get_future();
    std::promise<void> continueCreateTable;
    tableService.CreateTableStarted = &createTableStarted;
    tableService.ContinueCreateTable = continueCreateTable.get_future().share();

    auto requestB = tableSessionB->CreateTable(
        "/Root/My/DB/driver_scope_isolation",
        NTable::TTableBuilder().Build()
    );
    ASSERT_EQ(createTableStartedFuture.wait_for(std::chrono::seconds(10)), std::future_status::ready);

    driverA.Stop(true);

    auto stoppedResult = tableClientA.CreateSession().ExtractValueSync();
    ASSERT_EQ(stoppedResult.GetStatus(), EStatus::CLIENT_CANCELLED);
    ASSERT_FALSE(requestB.Wait(TDuration::MilliSeconds(100)));

    continueCreateTable.set_value();
    ASSERT_TRUE(requestB.Wait(TDuration::Seconds(10)));
    ASSERT_TRUE(requestB.ExtractValueSync().IsSuccess());

    auto secondResultB = tableClientB->CreateSession().ExtractValueSync();
    ASSERT_TRUE(secondResultB.IsSuccess());
}

TEST(TableTest, DropLastOwnersFromResponseCallbackDoesNotDeadlock) {
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driver;
    std::unique_ptr<NTable::TTableClient> tableClient;
    std::unique_ptr<NTable::TSession> tableSession;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driver,
        tableClient,
        tableSession
    );

    std::weak_ptr<TGRpcConnectionsImpl> connections = CreateInternalInterface(*driver);

    {
        auto pooledSessionResult = tableClient->GetSession().ExtractValueSync();
        ASSERT_TRUE(pooledSessionResult.IsSuccess());
        auto pooledSession = pooledSessionResult.GetSession();
    }

    std::promise<void> createTableStarted;
    auto createTableStartedFuture = createTableStarted.get_future();
    std::promise<void> continueCreateTable;
    tableService.CreateTableStarted = &createTableStarted;
    tableService.ContinueCreateTable = continueCreateTable.get_future().share();

    std::promise<void> callbackDone;
    auto callbackDoneFuture = callbackDone.get_future();
    std::atomic_bool success = false;

    auto requestFuture = tableSession->CreateTable(
        "/Root/My/DB/drop_owners",
        NTable::TTableBuilder().Build()
    );

    ASSERT_EQ(createTableStartedFuture.wait_for(std::chrono::seconds(10)), std::future_status::ready);

    requestFuture.Subscribe([&](const NThreading::TFuture<TStatus>& future) mutable {
        success.store(future.GetValue().IsSuccess());
        tableSession.reset();
        tableClient.reset();
        driver.reset();
        callbackDone.set_value();
    });

    continueCreateTable.set_value();

    ASSERT_EQ(callbackDoneFuture.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    ASSERT_TRUE(success.load());
    ASSERT_TRUE(WaitUntil([&] {
        return connections.expired();
    }));
    ASSERT_EQ(tableService.DeleteSessionRequests.load(), 2u);
}

TEST(TableTest, DriverStopFromResponseCallbackThenDropOwnersDoesNotDeadlock) {
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driver;
    std::unique_ptr<NTable::TTableClient> tableClient;
    std::unique_ptr<NTable::TSession> tableSession;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driver,
        tableClient,
        tableSession
    );

    std::weak_ptr<TGRpcConnectionsImpl> connections = CreateInternalInterface(*driver);

    {
        auto pooledSessionResult = tableClient->GetSession().ExtractValueSync();
        ASSERT_TRUE(pooledSessionResult.IsSuccess());
        auto pooledSession = pooledSessionResult.GetSession();
    }

    std::promise<void> createTableStarted;
    auto createTableStartedFuture = createTableStarted.get_future();
    std::promise<void> continueCreateTable;
    tableService.CreateTableStarted = &createTableStarted;
    tableService.ContinueCreateTable = continueCreateTable.get_future().share();

    std::promise<void> callbackDone;
    auto callbackDoneFuture = callbackDone.get_future();
    std::atomic_bool success = false;

    auto requestFuture = tableSession->CreateTable(
        "/Root/My/DB/driver_stop_drop_owners",
        NTable::TTableBuilder().Build()
    );

    ASSERT_EQ(createTableStartedFuture.wait_for(std::chrono::seconds(10)), std::future_status::ready);

    requestFuture.Subscribe([&](const NThreading::TFuture<TStatus>& future) mutable {
        success.store(future.GetValue().IsSuccess());
        driver->Stop(true);
        tableSession.reset();
        tableClient.reset();
        driver.reset();
        callbackDone.set_value();
    });

    continueCreateTable.set_value();

    ASSERT_EQ(callbackDoneFuture.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    ASSERT_TRUE(success.load());
    ASSERT_TRUE(WaitUntil([&] {
        return connections.expired();
    }));
}

/**
 * Verify that the SDK creates the CREATE TABLE request correctly,
 * when no metrics configuration is provided.
 */
TEST(TableTest, CreateTableNoMetricsSettings) {
    // Start the mocked table API service
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driver;
    std::unique_ptr<NTable::TTableClient> tableClient;
    std::unique_ptr<NTable::TSession> tableSession;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driver,
        tableClient,
        tableSession
    );

    // Call the CreateTable() API without any metrics configuration
    auto requestFuture = tableSession->CreateTable(
        "/Root/My/DB/test_table",
        NTable::TTableBuilder()
            .Build()
    );

    ASSERT_TRUE(requestFuture.Wait(TDuration::Seconds(10)));

    auto result = requestFuture.ExtractValueSync();
    ASSERT_TRUE(result.IsSuccess());

    // Make sure the metrics configuration was not set in the CreateTable request
    ASSERT_TRUE(tableService.LastCreateTableRequest.has_value());
    ASSERT_TRUE(!tableService.LastCreateTableRequest->has_metrics_settings());
}

/**
 * Verify that the SDK creates the CREATE TABLE request correctly,
 * when the metrics configuration is provided.
 */
TEST(TableTest, CreateTableWithMetricsSettings) {
    // Start the mocked table API service
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driver;
    std::unique_ptr<NTable::TTableClient> tableClient;
    std::unique_ptr<NTable::TSession> tableSession;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driver,
        tableClient,
        tableSession
    );

    // Call the CreateTable() API with the metrics configuration configured
    // to every allowed metrics level
    const auto verifyMetricsLevelFunc = [&](
        const TString& metricsLevelName,
        NTable::TMetricsSettings::EMetricsLevel metricsLevel,
        Ydb::Table::MetricsSettings::MetricsLevel protoMetricsLevel
    ) {
        SCOPED_TRACE(testing::Message() << "Metrics level: " << metricsLevelName);

        auto requestFuture = tableSession->CreateTable(
            "/Root/My/DB/test_table",
            NTable::TTableBuilder()
                .SetMetricsSettings(metricsLevel)
                .Build()
        );

        ASSERT_TRUE(requestFuture.Wait(TDuration::Seconds(10)));

        auto result = requestFuture.ExtractValueSync();
        ASSERT_TRUE(result.IsSuccess());

        // Make sure the metrics configuration is set in the CreateTable request
        ASSERT_TRUE(tableService.LastCreateTableRequest.has_value());
        ASSERT_TRUE(tableService.LastCreateTableRequest->has_metrics_settings());

        ASSERT_EQ(
            tableService.LastCreateTableRequest->metrics_settings().metrics_level(),
            protoMetricsLevel
        );
    };

    verifyMetricsLevelFunc(
        "UNSPECIFIED",
        NTable::TMetricsSettings::EMetricsLevel::Unspecified,
        Ydb::Table::MetricsSettings::METRICS_LEVEL_UNSPECIFIED
    );

    verifyMetricsLevelFunc(
        "DISABLED",
        NTable::TMetricsSettings::EMetricsLevel::Disabled,
        Ydb::Table::MetricsSettings::METRICS_LEVEL_DISABLED
    );

    verifyMetricsLevelFunc(
        "DATABASE",
        NTable::TMetricsSettings::EMetricsLevel::Database,
        Ydb::Table::MetricsSettings::METRICS_LEVEL_DATABASE
    );

    verifyMetricsLevelFunc(
        "TABLE",
        NTable::TMetricsSettings::EMetricsLevel::Table,
        Ydb::Table::MetricsSettings::METRICS_LEVEL_TABLE
    );

    verifyMetricsLevelFunc(
        "PARTITION",
        NTable::TMetricsSettings::EMetricsLevel::Partition,
        Ydb::Table::MetricsSettings::METRICS_LEVEL_PARTITION
    );
}

/**
 * Verify that the SDK creates the ALTER TABLE request correctly,
 * when no metrics configuration is provided.
 */
TEST(TableTest, AlterTableNoMetricsSettings) {
    // Start the mocked table API service
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driver;
    std::unique_ptr<NTable::TTableClient> tableClient;
    std::unique_ptr<NTable::TSession> tableSession;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driver,
        tableClient,
        tableSession
    );

    // Call the AlterTable() API without any metrics configuration
    auto requestFuture = tableSession->AlterTable(
        "/Root/My/DB/test_table",
        NTable::TAlterTableSettings()
    );

    ASSERT_TRUE(requestFuture.Wait(TDuration::Seconds(10)));

    auto result = requestFuture.ExtractValueSync();
    ASSERT_TRUE(result.IsSuccess());

    // Make sure the metrics configuration was not set in the AlterTable request
    ASSERT_TRUE(tableService.LastAlterTableRequest.has_value());

    ASSERT_EQ(
        tableService.LastAlterTableRequest->metrics_settings_action_case(),
        Ydb::Table::AlterTableRequest::METRICS_SETTINGS_ACTION_NOT_SET
    );

    ASSERT_TRUE(!tableService.LastAlterTableRequest->has_set_metrics_settings());
    ASSERT_TRUE(!tableService.LastAlterTableRequest->has_drop_metrics_settings());
}

/**
 * Verify that the SDK creates the ALTER TABLE request correctly,
 * when the metrics configuration is explicitly dropped.
 */
TEST(TableTest, AlterTableDroppedMetricsSettings) {
    // Start the mocked table API service
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driver;
    std::unique_ptr<NTable::TTableClient> tableClient;
    std::unique_ptr<NTable::TSession> tableSession;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driver,
        tableClient,
        tableSession
    );

    // Call the AlterTable() API with the metrics configuration dropped
    auto requestFuture = tableSession->AlterTable(
        "/Root/My/DB/test_table",
        NTable::TAlterTableSettings()
            .BeginAlterMetricsSettings()
            .Drop()
            .EndAlterMetricsSettings()
    );

    ASSERT_TRUE(requestFuture.Wait(TDuration::Seconds(10)));

    auto result = requestFuture.ExtractValueSync();
    ASSERT_TRUE(result.IsSuccess());

    // Make sure the metrics configuration was not set in the AlterTable request
    ASSERT_TRUE(tableService.LastAlterTableRequest.has_value());

    ASSERT_EQ(
        tableService.LastAlterTableRequest->metrics_settings_action_case(),
        Ydb::Table::AlterTableRequest::kDropMetricsSettings
    );

    ASSERT_TRUE(!tableService.LastAlterTableRequest->has_set_metrics_settings());
    ASSERT_TRUE(tableService.LastAlterTableRequest->has_drop_metrics_settings());
}

/**
 * Verify proto round-trip for equi-height histogram multi-column statistics.
 */
TEST(TableTest, MultiColumnStatisticsEqHeightHistogramRoundTrip) {
    NTable::TMultiColumnStatisticsDescription desc(
        "h1",
        {"a", "b"},
        {NTable::EMultiColumnStatisticsType::EqHeightHistogram});

    Ydb::Table::TableMultiColumnStatistics proto;
    desc.SerializeTo(proto);
    ASSERT_EQ(proto.name(), "h1");
    ASSERT_EQ(proto.columns_size(), 2);
    ASSERT_EQ(proto.types_size(), 1);
    ASSERT_EQ(proto.types(0), Ydb::Table::TableMultiColumnStatistics::EQ_HEIGHT_HISTOGRAM);

    auto roundTrip = TProtoAccessor::FromProto(proto);
    ASSERT_EQ(roundTrip.GetName(), "h1");
    ASSERT_EQ(roundTrip.GetColumns().size(), 2u);
    ASSERT_EQ(roundTrip.GetTypes().size(), 1u);
    ASSERT_EQ(roundTrip.GetTypes()[0], NTable::EMultiColumnStatisticsType::EqHeightHistogram);
}

/**
 * Verify that the SDK creates the ALTER TABLE request correctly,
 * when the metrics configuration is explicitly set.
 */
TEST(TableTest, AlterTableSetMetricsSettings) {
    // Start the mocked table API service
    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    std::unique_ptr<TDriver> driver;
    std::unique_ptr<NTable::TTableClient> tableClient;
    std::unique_ptr<NTable::TSession> tableSession;

    StartServerWithTableService(
        tableService,
        grpcServer,
        driver,
        tableClient,
        tableSession
    );

    // Call the AlterTable() API with the metrics configuration set explicitly
    // to every allowed metrics level
    const auto verifyMetricsLevelFunc = [&](
        const TString& metricsLevelName,
        NTable::TMetricsSettings::EMetricsLevel metricsLevel,
        Ydb::Table::MetricsSettings::MetricsLevel protoMetricsLevel
    ) {
        SCOPED_TRACE(testing::Message() << "Metrics level: " << metricsLevelName);

        auto requestFuture = tableSession->AlterTable(
            "/Root/My/DB/test_table",
            NTable::TAlterTableSettings()
                .BeginAlterMetricsSettings()
                .Set(metricsLevel)
                .EndAlterMetricsSettings()
        );

        ASSERT_TRUE(requestFuture.Wait(TDuration::Seconds(10)));

        auto result = requestFuture.ExtractValueSync();
        ASSERT_TRUE(result.IsSuccess());

        // Make sure the metrics configuration was set in the AlterTable request
        ASSERT_TRUE(tableService.LastAlterTableRequest.has_value());

        ASSERT_EQ(
            tableService.LastAlterTableRequest->metrics_settings_action_case(),
            Ydb::Table::AlterTableRequest::kSetMetricsSettings
        );

        ASSERT_EQ(
            tableService.LastAlterTableRequest->set_metrics_settings().metrics_level(),
            protoMetricsLevel
        );

        ASSERT_TRUE(tableService.LastAlterTableRequest->has_set_metrics_settings());
        ASSERT_TRUE(!tableService.LastAlterTableRequest->has_drop_metrics_settings());
    };

    verifyMetricsLevelFunc(
        "UNSPECIFIED",
        NTable::TMetricsSettings::EMetricsLevel::Unspecified,
        Ydb::Table::MetricsSettings::METRICS_LEVEL_UNSPECIFIED
    );

    verifyMetricsLevelFunc(
        "DISABLED",
        NTable::TMetricsSettings::EMetricsLevel::Disabled,
        Ydb::Table::MetricsSettings::METRICS_LEVEL_DISABLED
    );

    verifyMetricsLevelFunc(
        "DATABASE",
        NTable::TMetricsSettings::EMetricsLevel::Database,
        Ydb::Table::MetricsSettings::METRICS_LEVEL_DATABASE
    );

    verifyMetricsLevelFunc(
        "TABLE",
        NTable::TMetricsSettings::EMetricsLevel::Table,
        Ydb::Table::MetricsSettings::METRICS_LEVEL_TABLE
    );

    verifyMetricsLevelFunc(
        "PARTITION",
        NTable::TMetricsSettings::EMetricsLevel::Partition,
        Ydb::Table::MetricsSettings::METRICS_LEVEL_PARTITION
    );
}

TEST(TtlTierSettings, ObjectKeyPrefixRoundTrip) {
    using namespace NYdb::NTable;
    for (auto&& prefix : {std::optional<std::string>(), std::make_optional(std::string()),
                          std::make_optional(std::string("archive//2026:09/"))}) {
        const TTtlTierSettings original(TDateTypeColumnModeSettings("ts", TDuration::Days(1)),
                                        TTtlEvictToExternalStorageAction("/Root/eds", prefix));
        Ydb::Table::TtlTier proto;
        original.SerializeTo(proto);
        ASSERT_EQ(proto.evict_to_external_storage().has_object_key_prefix(), prefix.has_value());
        const auto restored = TTtlTierSettings::FromProto(proto);
        ASSERT_TRUE(restored);
        const auto& action = std::get<TTtlEvictToExternalStorageAction>(restored->GetAction());
        EXPECT_EQ(action.GetStorage(), "/Root/eds");
        EXPECT_EQ(action.GetObjectKeyPrefix(), prefix);
    }

    Ydb::Table::EvictionToExternalStorageSettings proto;
    proto.set_object_key_prefix("previous");
    TTtlEvictToExternalStorageAction("/Root/eds").SerializeTo(proto);
    EXPECT_FALSE(proto.has_object_key_prefix());
}

TEST(VectorIndexSettings, IndexTypeNumericCompatibility) {
    using NTable::EIndexType;
    // Index types are public API: adding HNSW must preserve existing values.
    const std::pair<EIndexType, int> existing[] = {
        {EIndexType::GlobalSync, 0},
        {EIndexType::GlobalAsync, 1},
        {EIndexType::GlobalUnique, 2},
        {EIndexType::GlobalVectorKMeansTree, 3},
        {EIndexType::GlobalFulltextPlain, 4},
        {EIndexType::GlobalFulltextRelevance, 5},
        {EIndexType::GlobalJson, 6},
        {EIndexType::LocalBloomFilter, 7},
        {EIndexType::LocalBloomNgramFilter, 8},
        {EIndexType::LocalMinMax, 9},
    };
    for (const auto& [type, value] : existing) {
        EXPECT_EQ(static_cast<int>(type), value);
    }
    EXPECT_EQ(static_cast<int>(EIndexType::GlobalHnsw), 10);
    EXPECT_EQ(static_cast<int>(EIndexType::Unknown), std::numeric_limits<int>::max());
}

TEST(VectorIndexSettings, HnswDefaultsAndAbsoluteDeltaRows) {
    Ydb::Table::VectorIndexSettings proto;
    const auto defaults = NTable::TVectorIndexSettings::FromProto(proto);
    EXPECT_EQ(defaults.MinRows, 10000u);
    EXPECT_EQ(defaults.M, 16u);
    EXPECT_EQ(defaults.EfConstruction, 200u);
    EXPECT_EQ(defaults.DeltaRows, 10000u);
    auto settings = defaults;
    settings.MinRows = 0;
    settings.M = 24;
    settings.EfConstruction = 400;
    settings.DeltaRows = 1ULL << 40;
    settings.SerializeTo(proto);
    EXPECT_TRUE(proto.has_min_rows());
    EXPECT_EQ(proto.min_rows(), 0u);
    EXPECT_EQ(proto.m(), 24u);
    EXPECT_EQ(proto.ef_construction(), 400u);
    EXPECT_EQ(proto.delta_rows(), 1ULL << 40);
    const auto restored = NTable::TVectorIndexSettings::FromProto(proto);
    EXPECT_EQ(restored.MinRows, settings.MinRows);
    EXPECT_EQ(restored.M, settings.M);
    EXPECT_EQ(restored.EfConstruction, settings.EfConstruction);
    EXPECT_EQ(restored.DeltaRows, settings.DeltaRows);
    settings.DeltaRows = 0;
    proto.Clear();
    settings.SerializeTo(proto);
    EXPECT_TRUE(proto.has_delta_rows());
    EXPECT_EQ(NTable::TVectorIndexSettings::FromProto(proto).DeltaRows, 0u);
}

TEST(VectorIndexSettings, HnswWireFieldNumbers) {
    Ydb::Table::VectorIndexSettings proto;
    // min_rows/M/ef_construction/delta_rows use tags 4/5/6/7.
    // Unknown tag 8 does not affect the supported settings.
    ASSERT_TRUE(proto.ParseFromString(std::string("\x20\x01\x28\x18\x30\xc8\x01\x38\x32\x40\x05", 11)));
    const auto settings = NTable::TVectorIndexSettings::FromProto(proto);
    EXPECT_EQ(settings.MinRows, 1u);
    EXPECT_EQ(settings.M, 24u);
    EXPECT_EQ(settings.EfConstruction, 200u);
    EXPECT_EQ(settings.DeltaRows, 50u);
    EXPECT_TRUE(proto.has_delta_rows());
}

TEST(VectorIndexSettings, HnswIndexFactoriesAndProtoRoundTrip) {
    for (const bool prefixed : {false, true}) {
        Ydb::Table::KMeansTreeSettings tree;
        tree.set_levels(1);
        tree.set_clusters(1);
        auto* vector = tree.mutable_settings();
        vector->set_metric(Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT);
        vector->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        vector->set_vector_dimension(200);
        vector->set_min_rows(0);
        vector->set_m(24);
        vector->set_ef_construction(300);
        vector->set_delta_rows(1ULL << 40);
        const auto settings = NTable::TKMeansTreeSettings::FromProto(tree);
        const auto factory = prefixed
            ? NTable::TIndexDescription::CreatePrefixedHnswIndex("vidx", {"scope", "emb"}, settings)
            : NTable::TIndexDescription::CreateHnswIndex("vidx", "emb", settings);
        Ydb::Table::TableIndex request;
        factory.SerializeTo(request);
        ASSERT_EQ(request.type_case(), Ydb::Table::TableIndex::kGlobalHnswIndex);
        EXPECT_EQ(request.index_columns_size(), prefixed ? 2 : 1);

        auto check = [&](const NTable::TIndexDescription& index) {
            EXPECT_EQ(index.GetIndexType(), NTable::EIndexType::GlobalHnsw);
            Ydb::Table::TableIndex serialized;
            index.SerializeTo(serialized);
            ASSERT_EQ(serialized.type_case(), Ydb::Table::TableIndex::kGlobalHnswIndex);
            const auto& actual = serialized.global_hnsw_index().vector_settings();
            EXPECT_EQ(actual.levels(), 1u);
            EXPECT_EQ(actual.clusters(), 1u);
            EXPECT_EQ(actual.settings().SerializeAsString(), vector->SerializeAsString());
        };
        check(TProtoAccessor::FromProto(request));
        Ydb::Table::TableIndexDescription description;
        description.set_name(request.name());
        *description.mutable_index_columns() = request.index_columns();
        *description.mutable_global_hnsw_index() = request.global_hnsw_index();
        check(TProtoAccessor::FromProto(description));
    }
}

TEST(VectorIndexSettings, LegacyMissingIndexKindIsPreserved) {
    Ydb::Table::TableIndexDescription description;
    description.set_name("legacy_index");
    EXPECT_EQ(TProtoAccessor::FromProto(description).GetIndexType(), NTable::EIndexType::GlobalSync);
}

TEST(VectorIndexSettings, KMeansIndexKeepsLegacyWireSettings) {
    NTable::TKMeansTreeSettings settings;
    settings.Settings.Metric = NTable::TVectorIndexSettings::EMetric::InnerProduct;
    settings.Settings.VectorType = NTable::TVectorIndexSettings::EVectorType::Float;
    settings.Settings.VectorDimension = 200;
    settings.Settings.MinRows = 0;
    settings.Settings.M = 24;
    settings.Settings.EfConstruction = 300;
    settings.Settings.DeltaRows = 0;
    settings.Levels = 1;
    settings.Clusters = 2;
    const auto index = NTable::TIndexDescription::CreateVectorIndex("vidx", "emb", settings);
    Ydb::Table::TableIndex request;
    index.SerializeTo(request);
    ASSERT_EQ(request.type_case(), Ydb::Table::TableIndex::kGlobalVectorKmeansTreeIndex);
    const auto& vector = request.global_vector_kmeans_tree_index().vector_settings().settings();
    EXPECT_FALSE(vector.has_min_rows());
    EXPECT_FALSE(vector.has_m());
    EXPECT_FALSE(vector.has_ef_construction());
    EXPECT_FALSE(vector.has_delta_rows());
    EXPECT_EQ(vector.vector_dimension(), 200u);

    request.Clear();
    NTable::TIndexDescription("unconfigured", NTable::EIndexType::GlobalVectorKMeansTree, {"emb"})
        .SerializeTo(request);
    EXPECT_FALSE(request.global_vector_kmeans_tree_index().vector_settings().has_settings());
}

TEST(VectorIndexSettings, DefaultHnswSettingsClearPreviousOverrides) {
    NTable::TVectorIndexSettings settings;
    settings.MinRows = 0;
    settings.M = 24;
    settings.EfConstruction = 300;
    settings.DeltaRows = 0;
    Ydb::Table::VectorIndexSettings proto;
    settings.SerializeTo(proto);
    ASSERT_TRUE(proto.has_min_rows());
    ASSERT_TRUE(proto.has_m());
    ASSERT_TRUE(proto.has_ef_construction());
    ASSERT_TRUE(proto.has_delta_rows());

    NTable::TVectorIndexSettings().SerializeTo(proto);
    EXPECT_FALSE(proto.has_min_rows());
    EXPECT_FALSE(proto.has_m());
    EXPECT_FALSE(proto.has_ef_construction());
    EXPECT_FALSE(proto.has_delta_rows());
    const auto restored = NTable::TVectorIndexSettings::FromProto(proto);
    EXPECT_EQ(restored.MinRows, 10000u);
    EXPECT_EQ(restored.M, 16u);
    EXPECT_EQ(restored.EfConstruction, 200u);
    EXPECT_EQ(restored.DeltaRows, 10000u);
}

TEST(VectorIndexSettings, HnswIndexDefaultsHavePresence) {
    NTable::TKMeansTreeSettings settings;
    settings.Settings.Metric = NTable::TVectorIndexSettings::EMetric::InnerProduct;
    settings.Settings.VectorType = NTable::TVectorIndexSettings::EVectorType::Float;
    settings.Settings.VectorDimension = 200;
    settings.Levels = 1;
    settings.Clusters = 1;
    const auto index = NTable::TIndexDescription::CreateHnswIndex("vidx", "emb", settings);
    Ydb::Table::TableIndex request;
    index.SerializeTo(request);
    const auto& vector = request.global_hnsw_index().vector_settings().settings();
    ASSERT_TRUE(vector.has_min_rows());
    ASSERT_TRUE(vector.has_m());
    ASSERT_TRUE(vector.has_ef_construction());
    ASSERT_TRUE(vector.has_delta_rows());
    EXPECT_EQ(vector.min_rows(), 10000u);
    EXPECT_EQ(vector.m(), 16u);
    EXPECT_EQ(vector.ef_construction(), 200u);
    EXPECT_EQ(vector.delta_rows(), 10000u);
}
