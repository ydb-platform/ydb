#include "ydb_common_ut.h"
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(YdbSdkSessions) {
    Y_UNIT_TEST(TestSessionPool) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        const TString location = TStringBuilder() << "localhost:" << grpc;

        auto driver = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        NYdb::NTable::TTableClient client(driver);
        int count = 10;

        THashSet<TString> sids;
        while (count--) {
            auto sessionResponse = client.GetSession().ExtractValueSync();
            UNIT_ASSERT_EQUAL(sessionResponse.IsTransportError(), false);
            auto session = sessionResponse.GetSession();
            sids.insert(session.GetId());
            auto result = session.ExecuteDataQuery("SELECT 42;",
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(result.GetEndpoint(), location);
        }
        // All requests used one session
        UNIT_ASSERT_VALUES_EQUAL(sids.size(), 1);
        // No more session captured by client
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);

        driver.Stop(true);
    }

    Y_UNIT_TEST(TestMultipleSessions) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto driver = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        NYdb::NTable::TTableClient client(driver);
        int count = 10;

        TVector<TSession> sids;
        TVector<TAsyncDataQueryResult> results;
        while (count--) {
            auto sessionResponse = client.GetSession().ExtractValueSync();
            UNIT_ASSERT_EQUAL(sessionResponse.IsTransportError(), false);
            auto session = sessionResponse.GetSession();
            sids.push_back(session);
            results.push_back(session.ExecuteDataQuery("SELECT 42;",
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()));
        }

        NThreading::WaitExceptionOrAll(results).Wait();
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 10);

        for (auto& result : results) {
            UNIT_ASSERT_EQUAL(result.GetValue().GetStatus(), EStatus::SUCCESS);
        }
        sids.clear();
        results.clear();

        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);

        driver.Stop(true);
    }

    Y_UNIT_TEST(TestActiveSessionCountAfterBadSession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto driver = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        NYdb::NTable::TTableClient client(driver);
        int count = 10;

        TVector<TSession> sids;
        TVector<TAsyncDataQueryResult> results;
        while (count--) {
            auto sessionResponse = client.GetSession().ExtractValueSync();
            UNIT_ASSERT_EQUAL(sessionResponse.IsTransportError(), false);
            auto session = sessionResponse.GetSession();
            sids.push_back(session);
            if (count == 0) {
                // Force BAD session server response for ExecuteDataQuery
                UNIT_ASSERT_EQUAL(session.Close().GetValueSync().GetStatus(), EStatus::SUCCESS);
                results.push_back(session.ExecuteDataQuery("SELECT 42;",
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()));
            } else {
                results.push_back(session.ExecuteDataQuery("SELECT 42;",
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()));
            }
        }

        NThreading::WaitExceptionOrAll(results).Wait();
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 10);

        for (size_t i = 0; i < results.size(); i++) {
            if (i == 9) {
                UNIT_ASSERT_EQUAL(results[i].GetValue().GetStatus(), EStatus::BAD_SESSION);
            } else {
                UNIT_ASSERT_EQUAL(results[i].GetValue().GetStatus(), EStatus::SUCCESS);
            }
        }
        sids.clear();
        results.clear();

        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);

        driver.Stop(true);
    }

    Y_UNIT_TEST(TestActiveSessionCountAfterTransportError) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto driver = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        NYdb::NTable::TTableClient client(driver);
        int count = 100;

        {
            auto sessionResponse = client.GetSession().ExtractValueSync();
            UNIT_ASSERT(sessionResponse.IsSuccess());
            auto session = sessionResponse.GetSession();
            auto result = session.ExecuteSchemeQuery(R"___(
                CREATE TABLE `Root/Test` (
                    Key Uint32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )___").ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
        }

        while (count--) {
            auto sessionResponse = client.GetSession().ExtractValueSync();
            UNIT_ASSERT_EQUAL(sessionResponse.IsTransportError(), false);
            auto session = sessionResponse.GetSession();

            // Assume 10us is too small to execute query and get response
            auto res = session.ExecuteDataQuery("SELECT COUNT(*) FROM `Root/Test`;",
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                NYdb::NTable::TExecDataQuerySettings().ClientTimeout(TDuration::MicroSeconds(10))).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
        }

        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        driver.Stop(true);
    }

    Y_UNIT_TEST(MultiThreadSync) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto driver = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        NYdb::NTable::TTableClient client(driver);
        const int nThreads = 10;
        const int nRequests = 1000;
        auto job = [client]() mutable {
            for (int i = 0; i < nRequests; i++) {
                auto sessionResponse = client.GetSession().ExtractValueSync();
                UNIT_ASSERT_EQUAL(sessionResponse.GetStatus(), EStatus::SUCCESS);
            }
        };
        IThreadFactory* pool = SystemThreadFactory();

        TVector<TAutoPtr<IThreadFactory::IThread>> threads;
        threads.resize(nThreads);
        for (int i = 0; i < nThreads; i++) {
            threads[i] = pool->Run(job);
        }
        for (int i = 0; i < nThreads; i++) {
            threads[i]->Join();
        }
        UNIT_ASSERT_EQUAL(client.GetActiveSessionCount(), 0);
        driver.Stop(true);
    }

    void EnsureCanExecQuery(NYdb::NTable::TSession session) {
        auto execStatus = session.ExecuteDataQuery("SELECT 42;",
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync().GetStatus();
        UNIT_ASSERT_EQUAL(execStatus, EStatus::SUCCESS);
    }

    void EnsureCanExecQuery(NYdb::NQuery::TSession session) {
        auto execStatus = session.ExecuteQuery("SELECT 42;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync().GetStatus();
        UNIT_ASSERT_EQUAL(execStatus, EStatus::SUCCESS);
    }

    template<typename TClient>
    void DoMultiThreadSessionPoolLimitSync() {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto driver = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        const int maxActiveSessions = 45;
        TClient client(driver,
            typename TClient::TSettings()
                .SessionPoolSettings(
                    typename TClient::TSettings::TSessionPoolSettings().MaxActiveSessions(maxActiveSessions)));

        constexpr int nThreads = 100;
        NYdb::EStatus statuses[nThreads];
        TVector<TMaybe<typename TClient::TSession>> sessions;
        sessions.resize(nThreads);
        TAtomic t = 0;
        auto job = [client, &t, &statuses, &sessions]() mutable {
            auto sessionResponse = client.GetSession().ExtractValueSync();
            int i = AtomicIncrement(t);
            statuses[--i] = sessionResponse.GetStatus();
            if (statuses[i] == EStatus::SUCCESS) {
                EnsureCanExecQuery(sessionResponse.GetSession());
                sessions[i] = sessionResponse.GetSession();
            }
        };
        IThreadFactory* pool = SystemThreadFactory();

        TVector<TAutoPtr<IThreadFactory::IThread>> threads;
        threads.resize(nThreads);
        for (int i = 0; i < nThreads; i++) {
            threads[i] = pool->Run(job);
        }
        for (int i = 0; i < nThreads; i++) {
            threads[i]->Join();
        }

        sessions.clear();

        int successCount = 0;
        int exhaustedCount = 0;
        for (int i = 0; i < nThreads; i++) {
            switch (statuses[i]) {
                case EStatus::SUCCESS:
                    successCount++;
                    break;
                case EStatus::CLIENT_RESOURCE_EXHAUSTED:
                    exhaustedCount++;
                    break;
                default:
                    UNIT_ASSERT(false);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(successCount, maxActiveSessions);
        UNIT_ASSERT_VALUES_EQUAL(exhaustedCount, nThreads - maxActiveSessions);
        driver.Stop(true);
    }

    Y_UNIT_TEST(MultiThreadSessionPoolLimitSyncTableClient) {
        DoMultiThreadSessionPoolLimitSync<NYdb::NTable::TTableClient>();
    }

    Y_UNIT_TEST(MultiThreadSessionPoolLimitSyncQueryClient) {
        DoMultiThreadSessionPoolLimitSync<NYdb::NQuery::TQueryClient>();
    }

    NYdb::NTable::TAsyncDataQueryResult ExecQueryAsync(NYdb::NTable::TSession session, const TString q) {
        return session.ExecuteDataQuery(q,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx());
    }

    NYdb::NQuery::TAsyncExecuteQueryResult ExecQueryAsync(NYdb::NQuery::TSession session, const TString q) {
        return session.ExecuteQuery(q,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx());
    }

    template<typename T>
    void DoMultiThreadMultipleRequestsOnSharedSessions() {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto driver = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        const int maxActiveSessions = 10;
        typename T::TClient client(driver,
            typename T::TClient::TSettings()
                .SessionPoolSettings(
                    typename T::TClient::TSettings::TSessionPoolSettings().MaxActiveSessions(maxActiveSessions)));

        constexpr int nThreads = 20;
        constexpr int nRequests = 50;
        std::array<TVector<typename T::TResult>, nThreads> results;
        TAtomic t = 0;
        TAtomic validSessions = 0;
        auto job = [client, &t, &results, &validSessions]() mutable {
            auto sessionResponse = client.GetSession().ExtractValueSync();

            int i = AtomicIncrement(t);
            TVector<typename T::TResult>& r = results[--i];

            if (sessionResponse.GetStatus() != EStatus::SUCCESS) {
                return;
            }
            AtomicIncrement(validSessions);

            for (int i = 0; i < nRequests; i++) {
                r.push_back(ExecQueryAsync(sessionResponse.GetSession(), "SELECT 42;"));
            }
        };
        IThreadFactory* pool = SystemThreadFactory();

        TVector<TAutoPtr<IThreadFactory::IThread>> threads;
        threads.resize(nThreads);
        for (int i = 0; i < nThreads; i++) {
            threads[i] = pool->Run(job);
        }
        for (int i = 0; i < nThreads; i++) {
            threads[i]->Join();
        }

        for (auto& r : results) {
            NThreading::WaitExceptionOrAll(r).Wait();
        }
        for (auto& r : results) {
            if (!r.empty()) {
                for (auto& asyncStatus : r) {
                    auto res = asyncStatus.GetValue();
                    if (!res.IsSuccess()) {
                        UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::SESSION_BUSY);
                    }
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), maxActiveSessions);
        auto curExpectedActive = maxActiveSessions;
        auto empty = 0;
        for (auto& r : results) {
            if (!r.empty()) {
                r.clear();
                UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), --curExpectedActive);
            } else {
                empty++;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(empty, nThreads - maxActiveSessions);
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        driver.Stop(true);
    }

    Y_UNIT_TEST(MultiThreadMultipleRequestsOnSharedSessionsTableClient) {
        struct TTypeHelper {
            using TClient = NYdb::NTable::TTableClient;
            using TResult = NYdb::NTable::TAsyncDataQueryResult;
        };
        DoMultiThreadMultipleRequestsOnSharedSessions<TTypeHelper>();
    }

    // Enable after interactive tx support
    //Y_UNIT_TEST(MultiThreadMultipleRequestsOnSharedSessionsQueryClient) {
    //    struct TTypeHelper {
    //        using TClient = NYdb::NQuery::TQueryClient;
    //        using TResult = NYdb::NQuery::TAsyncExecuteQueryResult;
    //    };
    //    DoMultiThreadMultipleRequestsOnSharedSessions<TTypeHelper>();
    //}

    Y_UNIT_TEST(SessionsServerLimit) {
        NKikimrConfig::TAppConfig appConfig;
        auto& tableServiceConfig = *appConfig.MutableTableServiceConfig();
        tableServiceConfig.SetSessionsLimitPerNode(2);

        TKikimrWithGrpcAndRootSchema server(appConfig);

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
        NYdb::NTable::TTableClient client(driver);
        auto sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
        auto session1 = sessionResult.GetSession();

        sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
        auto session2 = sessionResult.GetSession();

        sessionResult = client.CreateSession().ExtractValueSync();
        sessionResult.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::OVERLOADED);

        auto status = session1.Close().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(status.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(status.GetStatus(), EStatus::SUCCESS);

        auto result = session2.ExecuteDataQuery(R"___(
            SELECT 1;
        )___", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);

        sessionResult = client.CreateSession().ExtractValueSync();
        sessionResult.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
    }

    Y_UNIT_TEST(SessionsServerLimitWithSessionPool) {
        NKikimrConfig::TAppConfig appConfig;
        auto& tableServiceConfig = *appConfig.MutableTableServiceConfig();
        tableServiceConfig.SetSessionsLimitPerNode(2);

        TKikimrWithGrpcAndRootSchema server(appConfig);

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
        NYdb::NTable::TTableClient client(driver);
        auto sessionResult1 = client.GetSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult1.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
        auto session1 = sessionResult1.GetSession();

        auto sessionResult2 = client.GetSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult2.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 2);
        auto session2 = sessionResult2.GetSession();

        {
            auto sessionResult3 = client.GetSession().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(sessionResult3.GetStatus(), EStatus::OVERLOADED);
            UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 3);
        }
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 2);

        auto status = session1.Close().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(status.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL(status.GetStatus(), EStatus::SUCCESS);

        // Close doesnt free session from user perspective,
        // the value of ActiveSessionsCounter will be same after Close() call.
        // Probably we want to chenge this contract
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 2);

        auto result = session2.ExecuteDataQuery(R"___(
            SELECT 1;
        )___", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        sessionResult1 = client.GetSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult1.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(sessionResult1.GetSession().GetId().empty(), false);
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 3);

        auto sessionResult3 = client.GetSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult3.GetStatus(), EStatus::OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 4);

        auto tmp = client.GetSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 5);
        sessionResult1 = tmp; // here we reset previous created session object,
                              // so perform close rpc call implicitly and delete it
        UNIT_ASSERT_VALUES_EQUAL(sessionResult1.GetStatus(), EStatus::OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 4);
    }

    Y_UNIT_TEST(CloseSessionAfterDriverDtorWithoutSessionPool) {
        NKikimrConfig::TAppConfig appConfig;

        TKikimrWithGrpcAndRootSchema server(appConfig);

        TVector<TString> sessionIds;
        int iterations = 50;

        while (iterations--) {
            NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
            NYdb::NTable::TTableClient client(driver);
            auto sessionResult = client.CreateSession().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
            UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
            auto session1 = sessionResult.GetSession();
            sessionIds.push_back(session1.GetId());
        }

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(server.GetPort()), grpc::InsecureChannelCredentials());
        auto stub = Ydb::Table::V1::TableService::NewStub(channel);
        for (const auto& sessionId : sessionIds) {
            grpc::ClientContext context;
            Ydb::Table::KeepAliveRequest request;
            request.set_session_id(sessionId);
            Ydb::Table::KeepAliveResponse response;
            auto status = stub->KeepAlive(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::BAD_SESSION);
        }
    }

    Y_UNIT_TEST(CloseSessionWithSessionPoolExplicit) {
        NKikimrConfig::TAppConfig appConfig;

        TKikimrWithGrpcAndRootSchema server(appConfig);

        TVector<TString> sessionIds;
        int iterations = 100;

        while (iterations--) {
            NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
            NYdb::NTable::TTableClient client(driver);
            //TODO: remove this scope after session tracker implementation
            {
                auto sessionResult = client.GetSession().ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
                UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
                auto session1 = sessionResult.GetSession();
                sessionIds.push_back(session1.GetId());

                sessionResult = client.GetSession().ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 2);
                UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
                // Here previous created session will be returnet to session pool
                session1 = sessionResult.GetSession();
                UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
                sessionIds.push_back(session1.GetId());
            }

            if (RandomNumber<ui32>(10) == 5) {
                client.Stop().Apply([client](NThreading::TFuture<void> future){
                    UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
                    return future;
                }).Wait();
            } else {
                client.Stop().Wait();
            }

            if (iterations & 4) {
                driver.Stop(true);
            }
        }

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(server.GetPort()), grpc::InsecureChannelCredentials());
        auto stub = Ydb::Table::V1::TableService::NewStub(channel);
        for (const auto& sessionId : sessionIds) {
            grpc::ClientContext context;
            Ydb::Table::KeepAliveRequest request;
            request.set_session_id(sessionId);
            Ydb::Table::KeepAliveResponse response;
            auto status = stub->KeepAlive(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::BAD_SESSION);
        }
    }

    Y_UNIT_TEST(CloseSessionWithSessionPoolExplicitDriverStopOnly) {
        NKikimrConfig::TAppConfig appConfig;

        TKikimrWithGrpcAndRootSchema server(appConfig);

        TVector<TString> sessionIds;
        int iterations = 100;

        while (iterations--) {
            NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
            NYdb::NTable::TTableClient client(driver);
            //TODO: remove this scope after session tracker implementation
            {
                auto sessionResult = client.GetSession().ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
                UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
                auto session1 = sessionResult.GetSession();
                sessionIds.push_back(session1.GetId());

                sessionResult = client.GetSession().ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 2);
                UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
                // Here previous created session will be returnet to session pool
                session1 = sessionResult.GetSession();
                UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
                sessionIds.push_back(session1.GetId());
            }
            driver.Stop(true);
        }

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(server.GetPort()), grpc::InsecureChannelCredentials());
        auto stub = Ydb::Table::V1::TableService::NewStub(channel);
        for (const auto& sessionId : sessionIds) {
            grpc::ClientContext context;
            Ydb::Table::KeepAliveRequest request;
            request.set_session_id(sessionId);
            Ydb::Table::KeepAliveResponse response;
            auto status = stub->KeepAlive(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::BAD_SESSION);
        }
    }

    Y_UNIT_TEST(CloseSessionWithSessionPoolFromDtors) {
        NKikimrConfig::TAppConfig appConfig;

        TKikimrWithGrpcAndRootSchema server(appConfig);

        TVector<TString> sessionIds;
        int iterations = 100;

        while (iterations--) {
            NYdb::TDriver driver(TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
            NYdb::NTable::TTableClient client(driver);
            //TODO: remove this scope after session tracker implementation
            {
                auto sessionResult = client.GetSession().ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
                UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
                auto session1 = sessionResult.GetSession();
                sessionIds.push_back(session1.GetId());

                sessionResult = client.GetSession().ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 2);
                UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
                // Here previous created session will be returnet to session pool
                session1 = sessionResult.GetSession();
                UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
                sessionIds.push_back(session1.GetId());
            }
        }

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(server.GetPort()), grpc::InsecureChannelCredentials());
        auto stub = Ydb::Table::V1::TableService::NewStub(channel);
        for (const auto& sessionId : sessionIds) {
            grpc::ClientContext context;
            Ydb::Table::KeepAliveRequest request;
            request.set_session_id(sessionId);
            Ydb::Table::KeepAliveResponse response;
            auto status = stub->KeepAlive(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::BAD_SESSION);
        }
    }
}
