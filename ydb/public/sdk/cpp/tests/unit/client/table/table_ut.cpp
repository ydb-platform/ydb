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
#include <thread>

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
            Ydb::Table::DeleteSessionResponse* response) override {
            std::cerr << "DeleteSession():" << std::endl
                      << request->DebugString()
                      << std::endl;

            ++DeleteSessionRequests;

            if (DeleteSessionStarted) {
                DeleteSessionStarted->set_value();
                ContinueDeleteSession.wait();
            }

            auto op = response->mutable_operation();
            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);

            if (DeleteSessionFinished) {
                DeleteSessionFinished->set_value();
            }

            ++DeleteSessionCompleted;

            return grpc::Status::OK;
        }

        virtual grpc::Status AlterTable(
            grpc::ServerContext* /* context */,
            const Ydb::Table::AlterTableRequest* request,
            Ydb::Table::AlterTableResponse* response) override {
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

        std::optional<Ydb::Table::CreateTableRequest> LastCreateTableRequest;
        std::optional<Ydb::Table::AlterTableRequest> LastAlterTableRequest;
        std::atomic_uint DeleteSessionRequests = 0;
        std::atomic_uint DeleteSessionCompleted = 0;
        std::shared_ptr<std::promise<void>> CreateTableStarted;
        std::shared_future<void> ContinueCreateTable;
        std::shared_ptr<std::promise<void>> DeleteSessionStarted;
        std::shared_ptr<std::promise<void>> DeleteSessionFinished;
        std::shared_future<void> ContinueDeleteSession;
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
        tableSession);

    tableSession.reset();
    ASSERT_TRUE(WaitUntil([&] {
        return tableService.DeleteSessionCompleted.load() == 1u;
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

TEST(TableTest, DriverStopDoesNotWaitForPooledSessionClose) {
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
        tableSession);

    tableSession.reset();
    ASSERT_TRUE(WaitUntil([&] {
        return tableService.DeleteSessionCompleted.load() == 1u;
    }));
    tableService.DeleteSessionRequests.store(0);

    {
        auto pooledSessionResult = tableClient->GetSession().ExtractValueSync();
        ASSERT_TRUE(pooledSessionResult.IsSuccess());
        auto pooledSession = pooledSessionResult.GetSession();
    }

    auto deleteSessionStarted = std::make_shared<std::promise<void>>();
    auto deleteSessionStartedFuture = deleteSessionStarted->get_future();
    auto deleteSessionFinished = std::make_shared<std::promise<void>>();
    auto deleteSessionFinishedFuture = deleteSessionFinished->get_future();
    std::promise<void> continueDeleteSession;
    tableService.DeleteSessionStarted = std::move(deleteSessionStarted);
    tableService.DeleteSessionFinished = std::move(deleteSessionFinished);
    tableService.ContinueDeleteSession = continueDeleteSession.get_future().share();

    auto stopFuture = std::async(std::launch::async, [&] {
        driver->Stop(true);
    });
    const auto deleteStarted = deleteSessionStartedFuture.wait_for(std::chrono::seconds(10));
    const auto stopReturned = stopFuture.wait_for(std::chrono::seconds(10));
    continueDeleteSession.set_value();
    EXPECT_EQ(deleteStarted, std::future_status::ready);
    EXPECT_EQ(stopReturned, std::future_status::ready);
    ASSERT_EQ(stopFuture.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    stopFuture.get();
    ASSERT_EQ(deleteSessionFinishedFuture.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    ASSERT_EQ(tableService.DeleteSessionRequests.load(), 1u);
}

TEST(TableTest, CheckedOutPooledSessionClosesRemotelyAfterDriverStop) {
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
        tableSession);

    tableSession.reset();
    ASSERT_TRUE(WaitUntil([&] {
        return tableService.DeleteSessionRequests.load() == 1u;
    }));
    tableService.DeleteSessionRequests.store(0);

    {
        auto pooledSessionResult = tableClient->GetSession().ExtractValueSync();
        ASSERT_TRUE(pooledSessionResult.IsSuccess());
        auto pooledSession = pooledSessionResult.GetSession();

        driver->Stop(false);
        ASSERT_EQ(tableService.DeleteSessionRequests.load(), 0u);
    }

    ASSERT_TRUE(WaitUntil([&] {
        return tableService.DeleteSessionRequests.load() == 1u;
    }));
}

TEST(TableTest, AsyncDriverStopLetsInFlightRequestFinish) {
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
        tableSession);

    auto createTableStarted = std::make_shared<std::promise<void>>();
    auto createTableStartedFuture = createTableStarted->get_future();
    std::promise<void> continueCreateTable;
    tableService.CreateTableStarted = std::move(createTableStarted);
    tableService.ContinueCreateTable = continueCreateTable.get_future().share();

    auto requestFuture = tableSession->CreateTable(
        "/Root/My/DB/in_flight_during_driver_stop",
        NTable::TTableBuilder().Build());
    const auto createStarted = createTableStartedFuture.wait_for(std::chrono::seconds(10));
    if (createStarted != std::future_status::ready) {
        continueCreateTable.set_value();
    }
    ASSERT_EQ(createStarted, std::future_status::ready);

    auto stopFuture = std::async(std::launch::async, [&] {
        driver->Stop(false);
    });
    const auto stopReturned = stopFuture.wait_for(std::chrono::seconds(10));
    if (stopReturned != std::future_status::ready) {
        continueCreateTable.set_value();
    }
    ASSERT_EQ(stopReturned, std::future_status::ready);
    stopFuture.get();

    auto stoppedSessionResult = tableClient->GetSession();
    const bool stoppedSessionReady = stoppedSessionResult.Wait(TDuration::Seconds(10));
    continueCreateTable.set_value();
    ASSERT_TRUE(stoppedSessionReady);
    ASSERT_EQ(stoppedSessionResult.GetValue().GetStatus(), EStatus::CLIENT_CANCELLED);
    ASSERT_TRUE(requestFuture.Wait(TDuration::Seconds(10)));
    ASSERT_TRUE(requestFuture.ExtractValueSync().IsSuccess());
    driver->Stop(true);
}

TEST(TableTest, RetryAcceptedBeforeDriverStopCompletes) {
    TDriver driver(TDriverConfig()
                       .SetEndpoint("localhost:1")
                       .SetDiscoveryMode(EDiscoveryMode::Off));
    NTable::TTableClient tableClient(driver);
    auto attempts = std::make_shared<std::atomic_size_t>(0);
    TDriver callbackDriver = driver;

    auto result = tableClient.RetryOperation(
        [attempts, callbackDriver](NTable::TTableClient&) mutable -> TAsyncStatus {
            if (++*attempts == 1) {
                callbackDriver.Stop(false);
                return NThreading::MakeFuture(
                    TStatus(EStatus::UNAVAILABLE, NIssue::TIssues{}));
            }
            return NThreading::MakeFuture(
                TStatus(EStatus::CLIENT_CANCELLED, NIssue::TIssues{}));
        },
        NRetry::TRetryOperationSettings().MaxRetries(1));

    ASSERT_TRUE(result.Wait(TDuration::Seconds(10)));
    ASSERT_EQ(result.GetValue().GetStatus(), EStatus::CLIENT_CANCELLED);
    ASSERT_EQ(attempts->load(), 2u);
}

TEST(TableTest, DriverStopFromResponseCallbackIsNonBlocking) {
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
        tableSession);

    {
        auto pooledSessionResult = tableClient->GetSession().ExtractValueSync();
        ASSERT_TRUE(pooledSessionResult.IsSuccess());
        auto pooledSession = pooledSessionResult.GetSession();
    }

    auto createTableStarted = std::make_shared<std::promise<void>>();
    auto createTableStartedFuture = createTableStarted->get_future();
    std::promise<void> continueCreateTable;
    tableService.CreateTableStarted = std::move(createTableStarted);
    tableService.ContinueCreateTable = continueCreateTable.get_future().share();

    auto callbackDone = std::make_shared<std::promise<void>>();
    auto callbackDoneFuture = callbackDone->get_future();
    auto success = std::make_shared<std::atomic_bool>(false);
    TDriver callbackDriver = *driver;

    auto requestFuture = tableSession->CreateTable(
        "/Root/My/DB/driver_stop_from_callback",
        NTable::TTableBuilder().Build());

    const auto createStarted = createTableStartedFuture.wait_for(std::chrono::seconds(10));
    if (createStarted != std::future_status::ready) {
        continueCreateTable.set_value();
    }
    ASSERT_EQ(createStarted, std::future_status::ready);

    requestFuture.Subscribe([callbackDone, success, callbackDriver](const NThreading::TFuture<TStatus>& future) mutable {
        success->store(future.GetValue().IsSuccess());
        callbackDriver.Stop(true);
        callbackDone->set_value();
    });

    continueCreateTable.set_value();

    ASSERT_EQ(callbackDoneFuture.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    ASSERT_TRUE(success->load());
    ASSERT_TRUE(WaitUntil([&] {
        return tableService.DeleteSessionRequests.load() >= 1u;
    }));
    auto stoppedSessionResult = tableClient->GetSession();
    ASSERT_TRUE(stoppedSessionResult.Wait(TDuration::Seconds(10)));
    ASSERT_EQ(stoppedSessionResult.GetValue().GetStatus(), EStatus::CLIENT_CANCELLED);
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
        tableSessionB);

    TDriver driverA(driverB->GetConfig());
    NTable::TTableClient tableClientA(driverA);

    auto createTableStarted = std::make_shared<std::promise<void>>();
    auto createTableStartedFuture = createTableStarted->get_future();
    std::promise<void> continueCreateTable;
    tableService.CreateTableStarted = std::move(createTableStarted);
    tableService.ContinueCreateTable = continueCreateTable.get_future().share();

    auto requestB = tableSessionB->CreateTable(
        "/Root/My/DB/driver_scope_isolation",
        NTable::TTableBuilder().Build());
    const auto createStarted = createTableStartedFuture.wait_for(std::chrono::seconds(10));
    if (createStarted != std::future_status::ready) {
        continueCreateTable.set_value();
    }
    ASSERT_EQ(createStarted, std::future_status::ready);

    driverA.Stop(true);

    auto stoppedResult = tableClientA.CreateSession();
    const bool stoppedResultReady = stoppedResult.Wait(TDuration::Seconds(10));
    const bool requestCompletedBeforeRelease = requestB.Wait(TDuration::MilliSeconds(100));
    continueCreateTable.set_value();
    ASSERT_TRUE(stoppedResultReady);
    ASSERT_EQ(stoppedResult.GetValue().GetStatus(), EStatus::CLIENT_CANCELLED);
    ASSERT_FALSE(requestCompletedBeforeRelease);
    ASSERT_TRUE(requestB.Wait(TDuration::Seconds(10)));
    ASSERT_TRUE(requestB.ExtractValueSync().IsSuccess());

    auto secondResultB = tableClientB->CreateSession().ExtractValueSync();
    ASSERT_TRUE(secondResultB.IsSuccess());
}

TEST(TableTest, AsyncDriverStopFromResponseCallbackThenDropsOwners) {
    struct TOwners {
        std::unique_ptr<TDriver> Driver;
        std::unique_ptr<NTable::TTableClient> TableClient;
        std::unique_ptr<NTable::TSession> TableSession;
    };

    TMockTableService tableService;
    std::unique_ptr<grpc::Server> grpcServer;
    auto owners = std::make_shared<TOwners>();

    StartServerWithTableService(
        tableService,
        grpcServer,
        owners->Driver,
        owners->TableClient,
        owners->TableSession);

    std::weak_ptr<TGRpcConnectionsImpl> connections = CreateInternalInterface(*owners->Driver);

    {
        auto pooledSessionResult = owners->TableClient->GetSession().ExtractValueSync();
        ASSERT_TRUE(pooledSessionResult.IsSuccess());
        auto pooledSession = pooledSessionResult.GetSession();
    }

    auto createTableStarted = std::make_shared<std::promise<void>>();
    auto createTableStartedFuture = createTableStarted->get_future();
    std::promise<void> continueCreateTable;
    tableService.CreateTableStarted = std::move(createTableStarted);
    tableService.ContinueCreateTable = continueCreateTable.get_future().share();

    auto callbackDone = std::make_shared<std::promise<void>>();
    auto callbackDoneFuture = callbackDone->get_future();
    auto success = std::make_shared<std::atomic_bool>(false);

    auto requestFuture = owners->TableSession->CreateTable(
        "/Root/My/DB/driver_stop_drop_owners",
        NTable::TTableBuilder().Build());

    const auto createStarted = createTableStartedFuture.wait_for(std::chrono::seconds(10));
    if (createStarted != std::future_status::ready) {
        continueCreateTable.set_value();
    }
    ASSERT_EQ(createStarted, std::future_status::ready);

    requestFuture.Subscribe([owners, callbackDone, success](const NThreading::TFuture<TStatus>& future) mutable {
        success->store(future.GetValue().IsSuccess());
        owners->Driver->Stop(false);
        owners->TableSession.reset();
        owners->TableClient.reset();
        owners->Driver.reset();
        callbackDone->set_value();
    });

    continueCreateTable.set_value();

    ASSERT_EQ(callbackDoneFuture.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    ASSERT_TRUE(success->load());
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
