#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/common/network.h>

#include <util/string/builder.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

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
