#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <grpcpp/security/credentials.h>
#include <grpcpp/create_channel.h>

#include <random>
#include <thread>

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

std::string GetTablePath() {
    return std::string(std::getenv("YDB_DATABASE")) + "/" + std::string(std::getenv("YDB_TEST_ROOT")) + "/sessions_test_table";
}

void CreateTestTable(NYdb::TDriver& driver) {
    NYdb::NTable::TTableClient client(driver);
    auto sessionResult = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(sessionResult.IsSuccess());
    auto session = sessionResult.GetSession();
    auto result = session.ExecuteSchemeQuery(std::format(R"___(
        CREATE TABLE `{}` (
            Key Uint32,
            Value String,
            PRIMARY KEY (Key)
        );
    )___", GetTablePath())).ExtractValueSync();
    ASSERT_TRUE(result.IsSuccess());
    ASSERT_EQ(client.GetActiveSessionCount(), 1);
}

void WarmPoolCreateSession(NYdb::NQuery::TQueryClient& client, std::string& sessionId) {
    auto sessionResponse = client.GetSession().ExtractValueSync();
    ASSERT_TRUE(sessionResponse.IsSuccess());
    auto session = sessionResponse.GetSession();
    sessionId = session.GetId();
    auto res = session.ExecuteQuery("SELECT * FROM `Root/Test`", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

    ASSERT_EQ(res.GetStatus(), EStatus::SUCCESS) << res.GetIssues().ToString();

    TResultSetParser resultSet(res.GetResultSetParser(0));
    ASSERT_EQ(resultSet.ColumnsCount(), 2u);
}

void WaitForSessionsInPool(NYdb::NQuery::TQueryClient& client, std::int64_t expected) {
    int attempt = 10;
    while (attempt--) {
        if (client.GetCurrentPoolSize() == expected) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_EQ(client.GetCurrentPoolSize(), expected);
}

}

void CheckDelete(const NYdbGrpc::TGRpcClientConfig& clientConfig, const std::string& id, int expected, bool& allDoneOk) {
    NYdbGrpc::TGRpcClientLow clientLow;
    auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Query::V1::QueryService>(clientConfig);

    Ydb::Query::DeleteSessionRequest request;
    request.set_session_id(id);

    NYdbGrpc::TResponseCallback<Ydb::Query::DeleteSessionResponse> responseCb =
        [&allDoneOk, expected](NYdbGrpc::TGrpcStatus&& grpcStatus, Ydb::Query::DeleteSessionResponse&& response) -> void {
            ASSERT_FALSE(grpcStatus.InternalError);
            ASSERT_EQ(grpcStatus.GRpcStatusCode, 0) << grpcStatus.Msg + " " + grpcStatus.Details;
            allDoneOk &= (response.status() == expected);
            if (!allDoneOk) {
                std::cerr << "Expected status: " << expected << ", got response: " << response.DebugString() << std::endl;
            }
    };

    connection->DoRequest(request, std::move(responseCb), &Ydb::Query::V1::QueryService::Stub::AsyncDeleteSession);
}

TEST(YdbSdkSessions, TestSessionPool) {
    const std::string location = std::getenv("YDB_ENDPOINT");

    auto driver = NYdb::TDriver(
        TDriverConfig()
            .SetEndpoint(location));

    NYdb::NTable::TTableClient client(driver);
    int count = 10;

    std::unordered_set<std::string> sids;
    while (count--) {
        auto sessionResponse = client.GetSession().ExtractValueSync();
        ASSERT_FALSE(sessionResponse.IsTransportError());
        auto session = sessionResponse.GetSession();
        sids.insert(session.GetId());
        auto result = session.ExecuteDataQuery("SELECT 42;",
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        ASSERT_EQ(result.GetStatus(), EStatus::SUCCESS);
        ASSERT_EQ(result.GetEndpoint(), location);
    }
    // All requests used one session
    ASSERT_EQ(sids.size(), 1u);
    // No more session captured by client
    ASSERT_EQ(client.GetActiveSessionCount(), 0u);

    driver.Stop(true);
}

TEST(YdbSdkSessions, TestMultipleSessions) {
    std::string location = std::getenv("YDB_ENDPOINT");

    auto driver = NYdb::TDriver(
        TDriverConfig()
            .SetEndpoint(location));

    NYdb::NTable::TTableClient client(driver);
    int count = 10;

    std::vector<TSession> sids;
    std::vector<TAsyncDataQueryResult> results;
    while (count--) {
        auto sessionResponse = client.GetSession().ExtractValueSync();
        ASSERT_FALSE(sessionResponse.IsTransportError());
        auto session = sessionResponse.GetSession();
        sids.push_back(session);
        results.push_back(session.ExecuteDataQuery("SELECT 42;",
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()));
    }

    NThreading::WaitExceptionOrAll(results).Wait();
    ASSERT_EQ(client.GetActiveSessionCount(), 10);

    for (auto& result : results) {
        ASSERT_EQ(result.GetValue().GetStatus(), EStatus::SUCCESS);
    }
    sids.clear();
    results.clear();

    ASSERT_EQ(client.GetActiveSessionCount(), 0);

    driver.Stop(true);
}

TEST(YdbSdkSessions, TestActiveSessionCountAfterBadSession) {
    std::string location = std::getenv("YDB_ENDPOINT");

    auto driver = NYdb::TDriver(
        TDriverConfig()
            .SetEndpoint(location));

    NYdb::NTable::TTableClient client(driver);
    int count = 10;

    std::vector<TSession> sids;
    std::vector<TAsyncDataQueryResult> results;
    while (count--) {
        auto sessionResponse = client.GetSession().ExtractValueSync();
        ASSERT_FALSE(sessionResponse.IsTransportError());
        auto session = sessionResponse.GetSession();
        sids.push_back(session);
        if (count == 0) {
            // Force BAD session server response for ExecuteDataQuery
            ASSERT_EQ(session.Close().GetValueSync().GetStatus(), EStatus::SUCCESS);
            results.push_back(session.ExecuteDataQuery("SELECT 42;",
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()));
        } else {
            results.push_back(session.ExecuteDataQuery("SELECT 42;",
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()));
        }
    }

    NThreading::WaitExceptionOrAll(results).Wait();
    ASSERT_EQ(client.GetActiveSessionCount(), 10);

    for (size_t i = 0; i < results.size(); i++) {
        if (i == 9) {
            ASSERT_EQ(results[i].GetValue().GetStatus(), EStatus::BAD_SESSION);
        } else {
            ASSERT_EQ(results[i].GetValue().GetStatus(), EStatus::SUCCESS);
        }
    }
    sids.clear();
    results.clear();

    ASSERT_EQ(client.GetActiveSessionCount(), 0);

    driver.Stop(true);
}

TEST(YdbSdkSessions, TestSdkFreeSessionAfterBadSessionQueryService) {
    GTEST_SKIP() << "Test is failing right now";
    std::string location = std::getenv("YDB_ENDPOINT");
    auto clientConfig = NYdbGrpc::TGRpcClientConfig(location);

    auto driver = NYdb::TDriver(
        TDriverConfig()
            .SetEndpoint(location));

    CreateTestTable(driver);

    NYdb::NQuery::TQueryClient client(driver);
    std::string sessionId;
    WarmPoolCreateSession(client, sessionId);
    WaitForSessionsInPool(client, 1);

    bool allDoneOk = true;
    CheckDelete(clientConfig, sessionId, Ydb::StatusIds::SUCCESS, allDoneOk);
    ASSERT_TRUE(allDoneOk);

    {
        auto sessionResponse = client.GetSession().ExtractValueSync();
        ASSERT_TRUE(sessionResponse.IsSuccess());
        auto session = sessionResponse.GetSession();
        ASSERT_EQ(session.GetId(), sessionId);

        auto res = session.ExecuteQuery(std::format("SELECT * FROM `{}`", GetTablePath()), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

        ASSERT_EQ(res.GetStatus(), EStatus::BAD_SESSION) << res.GetIssues().ToString();
    }

    WaitForSessionsInPool(client, 0);

    {
        auto sessionResponse = client.GetSession().ExtractValueSync();
        ASSERT_TRUE(sessionResponse.IsSuccess());
        auto session = sessionResponse.GetSession();
        ASSERT_NE(session.GetId(), sessionId);
        auto res = session.ExecuteQuery(std::format("SELECT * FROM `{}`", GetTablePath()), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

        ASSERT_EQ(res.GetStatus(), EStatus::SUCCESS) << res.GetIssues().ToString();
    }

    WaitForSessionsInPool(client, 1);

    driver.Stop(true);
}

TEST(YdbSdkSessions, TestSdkFreeSessionAfterBadSessionQueryServiceStreamCall) {
    GTEST_SKIP() << "Test is failing right now";
    std::string location = std::getenv("YDB_ENDPOINT");
    auto clientConfig = NYdbGrpc::TGRpcClientConfig(location);

    auto driver = NYdb::TDriver(
        TDriverConfig()
            .SetEndpoint(location));

    CreateTestTable(driver);

    NYdb::NQuery::TQueryClient client(driver);
    std::string sessionId;
    WarmPoolCreateSession(client, sessionId);
    WaitForSessionsInPool(client, 1);

    bool allDoneOk = true;
    CheckDelete(clientConfig, sessionId, Ydb::StatusIds::SUCCESS, allDoneOk);
    ASSERT_TRUE(allDoneOk);

    {
        auto sessionResponse = client.GetSession().ExtractValueSync();
        ASSERT_TRUE(sessionResponse.IsSuccess());
        auto session = sessionResponse.GetSession();
        ASSERT_EQ(session.GetId(), sessionId);

        auto it = session.StreamExecuteQuery(std::format("SELECT * FROM `{}`", GetTablePath()), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

        ASSERT_EQ(it.GetStatus(), EStatus::SUCCESS) << it.GetIssues().ToString();

        auto res = it.ReadNext().GetValueSync();
        ASSERT_EQ(res.GetStatus(), EStatus::BAD_SESSION) << res.GetIssues().ToString();
    }

    WaitForSessionsInPool(client, 0);

    {
        auto sessionResponse = client.GetSession().ExtractValueSync();
        ASSERT_TRUE(sessionResponse.IsSuccess());
        auto session = sessionResponse.GetSession();
        ASSERT_NE(session.GetId(), sessionId);

        auto res = session.ExecuteQuery(std::format("SELECT * FROM `{}`", GetTablePath()), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

        ASSERT_EQ(res.GetStatus(), EStatus::SUCCESS) << res.GetIssues().ToString();
    }

    WaitForSessionsInPool(client, 1);

    driver.Stop(true);
}

TEST(YdbSdkSessions, TestActiveSessionCountAfterTransportError) {
    std::string location = std::getenv("YDB_ENDPOINT");

    auto driver = NYdb::TDriver(
        TDriverConfig()
            .SetEndpoint(location));

    NYdb::NTable::TTableClient client(driver);
    int count = 100;

    {
        auto sessionResponse = client.GetSession().ExtractValueSync();
        ASSERT_TRUE(sessionResponse.IsSuccess());
        auto session = sessionResponse.GetSession();
        auto result = session.ExecuteSchemeQuery(std::format(R"___(
            CREATE TABLE `{}` (
                Key Uint32,
                Value String,
                PRIMARY KEY (Key)
            );
        )___", GetTablePath())).ExtractValueSync();
        ASSERT_TRUE(result.IsSuccess());
        ASSERT_EQ(client.GetActiveSessionCount(), 1);
    }

    while (count--) {
        auto sessionResponse = client.GetSession().ExtractValueSync();
        ASSERT_FALSE(sessionResponse.IsTransportError());
        auto session = sessionResponse.GetSession();

        // Assume 10us is too small to execute query and get response
        auto res = session.ExecuteDataQuery(std::format("SELECT COUNT(*) FROM `{}`;", GetTablePath()),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
            NYdb::NTable::TExecDataQuerySettings().ClientTimeout(TDuration::MicroSeconds(10))).GetValueSync();
        ASSERT_EQ(client.GetActiveSessionCount(), 1);
    }

    ASSERT_EQ(client.GetActiveSessionCount(), 0);
    driver.Stop(true);
}

TEST(YdbSdkSessions, MultiThreadSync) {
    std::string location = std::getenv("YDB_ENDPOINT");

    auto driver = NYdb::TDriver(
        TDriverConfig()
            .SetEndpoint(location));

    NYdb::NTable::TTableClient client(driver);
    const int nThreads = 10;
    const int nRequests = 1000;
    auto job = [client]() mutable {
        for (int i = 0; i < nRequests; i++) {
            auto sessionResponse = client.GetSession().ExtractValueSync();
            ASSERT_EQ(sessionResponse.GetStatus(), EStatus::SUCCESS);
        }
    };
    std::vector<std::thread> threads;
    for (int i = 0; i < nThreads; i++) {
        threads.emplace_back(job);
    }
    for (auto& thread : threads) {
        thread.join();
    }
    ASSERT_EQ(client.GetActiveSessionCount(), 0);
    driver.Stop(true);
}

void EnsureCanExecQuery(NYdb::NTable::TSession session) {
    auto execStatus = session.ExecuteDataQuery("SELECT 42;",
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync().GetStatus();
    ASSERT_EQ(execStatus, EStatus::SUCCESS);
}

void EnsureCanExecQuery(NYdb::NQuery::TSession session) {
    auto execStatus = session.ExecuteQuery("SELECT 42;",
        NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync().GetStatus();
    ASSERT_EQ(execStatus, EStatus::SUCCESS);
}

template<typename TClient>
void DoMultiThreadSessionPoolLimitSync() {
    std::string location = std::getenv("YDB_ENDPOINT");

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
    std::vector<std::optional<typename TClient::TSession>> sessions;
    sessions.resize(nThreads);
    std::atomic<int> t = 0;
    auto job = [client, &t, &statuses, &sessions]() mutable {
        auto sessionResponse = client.GetSession().ExtractValueSync();
        int i = ++t;
        statuses[--i] = sessionResponse.GetStatus();
        if (statuses[i] == EStatus::SUCCESS) {
            EnsureCanExecQuery(sessionResponse.GetSession());
            sessions[i] = sessionResponse.GetSession();
        }
    };

    std::vector<std::thread> threads;
    threads.resize(nThreads);
    for (int i = 0; i < nThreads; i++) {
        threads[i] = std::thread(job);
    }
    for (int i = 0; i < nThreads; i++) {
        threads[i].join();
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
                FAIL() << "Unexpected status code: " << static_cast<size_t>(statuses[i]);
        }
    }

    ASSERT_EQ(client.GetActiveSessionCount(), 0);
    ASSERT_EQ(successCount, maxActiveSessions);
    ASSERT_EQ(exhaustedCount, nThreads - maxActiveSessions);
    driver.Stop(true);
}

TEST(YdbSdkSessions, MultiThreadSessionPoolLimitSyncTableClient) {
    DoMultiThreadSessionPoolLimitSync<NYdb::NTable::TTableClient>();
}

TEST(YdbSdkSessions, MultiThreadSessionPoolLimitSyncQueryClient) {
    DoMultiThreadSessionPoolLimitSync<NYdb::NQuery::TQueryClient>();
}

NYdb::NTable::TAsyncDataQueryResult ExecQueryAsync(NYdb::NTable::TSession session, const std::string q) {
    return session.ExecuteDataQuery(q,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx());
}

NYdb::NQuery::TAsyncExecuteQueryResult ExecQueryAsync(NYdb::NQuery::TSession session, const std::string q) {
    return session.ExecuteQuery(q,
        NYdb::NQuery::TTxControl::BeginTx().CommitTx());
}

template<typename T>
void DoMultiThreadMultipleRequestsOnSharedSessions() {
    std::string location = std::getenv("YDB_ENDPOINT");

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
    std::array<std::vector<typename T::TResult>, nThreads> results;
    std::atomic<int> t = 0;
    std::atomic<int> validSessions = 0;
    auto job = [client, &t, &results, &validSessions]() mutable {
        auto sessionResponse = client.GetSession().ExtractValueSync();

        int i = ++t;
        std::vector<typename T::TResult>& r = results[--i];

        if (sessionResponse.GetStatus() != EStatus::SUCCESS) {
            return;
        }
        validSessions.fetch_add(1);

        for (int i = 0; i < nRequests; i++) {
            r.push_back(ExecQueryAsync(sessionResponse.GetSession(), "SELECT 42;"));
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < nThreads; i++) {
        threads.emplace_back(job);
    }
    for (auto& thread : threads) {
        thread.join();
    }

    for (auto& r : results) {
        NThreading::WaitExceptionOrAll(r).Wait();
    }
    for (auto& r : results) {
        if (!r.empty()) {
            for (auto& asyncStatus : r) {
                auto res = asyncStatus.GetValue();
                if (!res.IsSuccess()) {
                    ASSERT_EQ(res.GetStatus(), EStatus::SESSION_BUSY);
                }
            }
        }
    }
    ASSERT_EQ(client.GetActiveSessionCount(), maxActiveSessions);
    auto curExpectedActive = maxActiveSessions;
    auto empty = 0;
    for (auto& r : results) {
        if (!r.empty()) {
            r.clear();
            ASSERT_EQ(client.GetActiveSessionCount(), --curExpectedActive);
        } else {
            empty++;
        }
    }
    ASSERT_EQ(empty, nThreads - maxActiveSessions);
    ASSERT_EQ(client.GetActiveSessionCount(), 0);
    driver.Stop(true);
}

TEST(YdbSdkSessions, MultiThreadMultipleRequestsOnSharedSessionsTableClient) {
    struct TTypeHelper {
        using TClient = NYdb::NTable::TTableClient;
        using TResult = NYdb::NTable::TAsyncDataQueryResult;
    };
    DoMultiThreadMultipleRequestsOnSharedSessions<TTypeHelper>();
}

TEST(YdbSdkSessions, MultiThreadMultipleRequestsOnSharedSessionsQueryClient) {
    GTEST_SKIP() << "Enable after interactive tx support";
    struct TTypeHelper {
        using TClient = NYdb::NQuery::TQueryClient;
        using TResult = NYdb::NQuery::TAsyncExecuteQueryResult;
    };
    DoMultiThreadMultipleRequestsOnSharedSessions<TTypeHelper>();
}

TEST(YdbSdkSessions, SessionsServerLimit) {
    GTEST_SKIP() << "Enable after accepting a pull request with merging configs";

    NYdb::TDriver driver(TDriverConfig().SetEndpoint(std::getenv("YDB_ENDPOINT")));
    NYdb::NTable::TTableClient client(driver);
    auto sessionResult = client.CreateSession().ExtractValueSync();
    ASSERT_EQ(client.GetActiveSessionCount(), 0);
    ASSERT_EQ(sessionResult.GetStatus(), EStatus::SUCCESS);
    auto session1 = sessionResult.GetSession();

    sessionResult = client.CreateSession().ExtractValueSync();
    ASSERT_EQ(client.GetActiveSessionCount(), 0);
    ASSERT_EQ(sessionResult.GetStatus(), EStatus::SUCCESS);
    auto session2 = sessionResult.GetSession();

    sessionResult = client.CreateSession().ExtractValueSync();
    sessionResult.GetIssues().PrintTo(Cerr);
    ASSERT_EQ(client.GetActiveSessionCount(), 0);
    ASSERT_EQ(sessionResult.GetStatus(), EStatus::OVERLOADED);

    auto status = session1.Close().ExtractValueSync();
    ASSERT_EQ(status.IsTransportError(), false);
    ASSERT_EQ(client.GetActiveSessionCount(), 0);
    ASSERT_EQ(status.GetStatus(), EStatus::SUCCESS);

    auto result = session2.ExecuteDataQuery(R"___(
        SELECT 1;
    )___", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    ASSERT_EQ(client.GetActiveSessionCount(), 0);
    ASSERT_EQ(result.GetStatus(), EStatus::SUCCESS);

    sessionResult = client.CreateSession().ExtractValueSync();
    ASSERT_EQ(sessionResult.GetStatus(), EStatus::SUCCESS);

    sessionResult = client.CreateSession().ExtractValueSync();
    sessionResult.GetIssues().PrintTo(Cerr);
    ASSERT_EQ(sessionResult.GetStatus(), EStatus::OVERLOADED);
    ASSERT_EQ(client.GetActiveSessionCount(), 0);
}

TEST(YdbSdkSessions, SessionsServerLimitWithSessionPool) {
    GTEST_SKIP() << "Enable after accepting a pull request with merging configs";
    NYdb::TDriver driver(TDriverConfig().SetEndpoint(std::getenv("YDB_ENDPOINT")));
    NYdb::NTable::TTableClient client(driver);
    auto sessionResult1 = client.GetSession().ExtractValueSync();
    ASSERT_EQ(sessionResult1.GetStatus(), EStatus::SUCCESS);
    ASSERT_EQ(client.GetActiveSessionCount(), 1);
    auto session1 = sessionResult1.GetSession();

    auto sessionResult2 = client.GetSession().ExtractValueSync();
    ASSERT_EQ(sessionResult2.GetStatus(), EStatus::SUCCESS);
    ASSERT_EQ(client.GetActiveSessionCount(), 2);
    auto session2 = sessionResult2.GetSession();

    {
        auto sessionResult3 = client.GetSession().ExtractValueSync();
        ASSERT_EQ(sessionResult3.GetStatus(), EStatus::OVERLOADED);
        ASSERT_EQ(client.GetActiveSessionCount(), 3);
    }
    ASSERT_EQ(client.GetActiveSessionCount(), 2);

    auto status = session1.Close().ExtractValueSync();
    ASSERT_EQ(status.IsTransportError(), false);
    ASSERT_EQ(status.GetStatus(), EStatus::SUCCESS);

    // Close doesnt free session from user perspective,
    // the value of ActiveSessionsCounter will be same after Close() call.
    // Probably we want to chenge this contract
    ASSERT_EQ(client.GetActiveSessionCount(), 2);

    auto result = session2.ExecuteDataQuery(R"___(
        SELECT 1;
    )___", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    ASSERT_EQ(result.GetStatus(), EStatus::SUCCESS);

    sessionResult1 = client.GetSession().ExtractValueSync();
    ASSERT_EQ(sessionResult1.GetStatus(), EStatus::SUCCESS);
    ASSERT_EQ(sessionResult1.GetSession().GetId().empty(), false);
    ASSERT_EQ(client.GetActiveSessionCount(), 3);

    auto sessionResult3 = client.GetSession().ExtractValueSync();
    ASSERT_EQ(sessionResult3.GetStatus(), EStatus::OVERLOADED);
    ASSERT_EQ(client.GetActiveSessionCount(), 4);

    auto tmp = client.GetSession().ExtractValueSync();
    ASSERT_EQ(client.GetActiveSessionCount(), 5);
    sessionResult1 = tmp; // here we reset previous created session object,
                          // so perform close rpc call implicitly and delete it
    ASSERT_EQ(sessionResult1.GetStatus(), EStatus::OVERLOADED);
    ASSERT_EQ(client.GetActiveSessionCount(), 4);
}

TEST(YdbSdkSessions, CloseSessionAfterDriverDtorWithoutSessionPool) {
    std::vector<std::string> sessionIds;
    int iterations = 50;

    while (iterations--) {
        NYdb::TDriver driver(TDriverConfig().SetEndpoint(std::getenv("YDB_ENDPOINT")));
        NYdb::NTable::TTableClient client(driver);
        auto sessionResult = client.CreateSession().ExtractValueSync();
        ASSERT_EQ(client.GetActiveSessionCount(), 0);
        ASSERT_EQ(sessionResult.GetStatus(), EStatus::SUCCESS);
        auto session1 = sessionResult.GetSession();
        sessionIds.push_back(session1.GetId());
    }

    std::shared_ptr<grpc::Channel> channel;
    channel = grpc::CreateChannel(std::getenv("YDB_ENDPOINT"), grpc::InsecureChannelCredentials());
    auto stub = Ydb::Table::V1::TableService::NewStub(channel);
    for (const auto& sessionId : sessionIds) {
        grpc::ClientContext context;
        Ydb::Table::KeepAliveRequest request;
        request.set_session_id(sessionId);
        Ydb::Table::KeepAliveResponse response;
        auto status = stub->KeepAlive(&context, request, &response);
        ASSERT_TRUE(status.ok());
        auto deferred = response.operation();
        ASSERT_TRUE(deferred.ready() == true);
        ASSERT_EQ(deferred.status(), Ydb::StatusIds::BAD_SESSION);
    }
}

TEST(YdbSdkSessions, CloseSessionWithSessionPoolExplicit) {
    std::vector<std::string> sessionIds;
    int iterations = 100;

    while (iterations--) {
        NYdb::TDriver driver(TDriverConfig().SetEndpoint(std::getenv("YDB_ENDPOINT")));
        NYdb::NTable::TTableClient client(driver);
        //TODO: remove this scope after session tracker implementation
        {
            auto sessionResult = client.GetSession().ExtractValueSync();
            ASSERT_EQ(client.GetActiveSessionCount(), 1);
            ASSERT_EQ(sessionResult.GetStatus(), EStatus::SUCCESS);
            auto session1 = sessionResult.GetSession();
            sessionIds.push_back(session1.GetId());

            sessionResult = client.GetSession().ExtractValueSync();
            ASSERT_EQ(client.GetActiveSessionCount(), 2);
            ASSERT_EQ(sessionResult.GetStatus(), EStatus::SUCCESS);
            // Here previous created session will be returnet to session pool
            session1 = sessionResult.GetSession();
            ASSERT_EQ(client.GetActiveSessionCount(), 1);
            sessionIds.push_back(session1.GetId());
        }

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 9);
        if (dis(gen) == 5) {
            client.Stop().Apply([client](NThreading::TFuture<void> future) {
                EXPECT_EQ(client.GetActiveSessionCount(), 0);
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
    channel = grpc::CreateChannel(std::getenv("YDB_ENDPOINT"), grpc::InsecureChannelCredentials());
    auto stub = Ydb::Table::V1::TableService::NewStub(channel);
    for (const auto& sessionId : sessionIds) {
        grpc::ClientContext context;
        Ydb::Table::KeepAliveRequest request;
        request.set_session_id(sessionId);
        Ydb::Table::KeepAliveResponse response;
        auto status = stub->KeepAlive(&context, request, &response);
        ASSERT_TRUE(status.ok());
        auto deferred = response.operation();
        ASSERT_TRUE(deferred.ready() == true);
        ASSERT_EQ(deferred.status(), Ydb::StatusIds::BAD_SESSION);
    }
}

TEST(YdbSdkSessions, CloseSessionWithSessionPoolExplicitDriverStopOnly) {
    std::vector<std::string> sessionIds;
    int iterations = 100;

    while (iterations--) {
        NYdb::TDriver driver(TDriverConfig().SetEndpoint(std::getenv("YDB_ENDPOINT")));
        NYdb::NTable::TTableClient client(driver);
        //TODO: remove this scope after session tracker implementation
        {
            auto sessionResult = client.GetSession().ExtractValueSync();
            ASSERT_EQ(client.GetActiveSessionCount(), 1);
            ASSERT_EQ(sessionResult.GetStatus(), EStatus::SUCCESS);
            auto session1 = sessionResult.GetSession();
            sessionIds.push_back(session1.GetId());

            sessionResult = client.GetSession().ExtractValueSync();
            ASSERT_EQ(client.GetActiveSessionCount(), 2);
            ASSERT_EQ(sessionResult.GetStatus(), EStatus::SUCCESS);
            // Here previous created session will be returnet to session pool
            session1 = sessionResult.GetSession();
            ASSERT_EQ(client.GetActiveSessionCount(), 1);
            sessionIds.push_back(session1.GetId());
        }
        driver.Stop(true);
    }

    std::shared_ptr<grpc::ChannelInterface> channel;
    channel = grpc::CreateChannel(std::getenv("YDB_ENDPOINT"), grpc::InsecureChannelCredentials());
    auto stub = Ydb::Table::V1::TableService::NewStub(channel);
    for (const auto& sessionId : sessionIds) {
        grpc::ClientContext context;
        Ydb::Table::KeepAliveRequest request;
        request.set_session_id(sessionId);
        Ydb::Table::KeepAliveResponse response;
        auto status = stub->KeepAlive(&context, request, &response);
        ASSERT_TRUE(status.ok());
        auto deferred = response.operation();
        ASSERT_TRUE(deferred.ready() == true);
        ASSERT_EQ(deferred.status(), Ydb::StatusIds::BAD_SESSION);
    }
}

TEST(YdbSdkSessions, CloseSessionWithSessionPoolFromDtors) {
    std::vector<std::string> sessionIds;
    int iterations = 100;

    while (iterations--) {
        NYdb::TDriver driver(TDriverConfig().SetEndpoint(std::getenv("YDB_ENDPOINT")));
        NYdb::NTable::TTableClient client(driver);
        //TODO: remove this scope after session tracker implementation
        {
            auto sessionResult = client.GetSession().ExtractValueSync();
            ASSERT_EQ(client.GetActiveSessionCount(), 1);
            ASSERT_EQ(sessionResult.GetStatus(), EStatus::SUCCESS);
            auto session1 = sessionResult.GetSession();
            sessionIds.push_back(session1.GetId());

            sessionResult = client.GetSession().ExtractValueSync();
            ASSERT_EQ(client.GetActiveSessionCount(), 2);
            ASSERT_EQ(sessionResult.GetStatus(), EStatus::SUCCESS);
            // Here previous created session will be returnet to session pool
            session1 = sessionResult.GetSession();
            ASSERT_EQ(client.GetActiveSessionCount(), 1);
            sessionIds.push_back(session1.GetId());
        }
    }

    std::shared_ptr<grpc::Channel> channel;
    channel = grpc::CreateChannel(std::getenv("YDB_ENDPOINT"), grpc::InsecureChannelCredentials());
    auto stub = Ydb::Table::V1::TableService::NewStub(channel);
    for (const auto& sessionId : sessionIds) {
        grpc::ClientContext context;
        Ydb::Table::KeepAliveRequest request;
        request.set_session_id(sessionId);
        Ydb::Table::KeepAliveResponse response;
        auto status = stub->KeepAlive(&context, request, &response);
        ASSERT_TRUE(status.ok());
        auto deferred = response.operation();
        ASSERT_TRUE(deferred.ready() == true);
        ASSERT_EQ(deferred.status(), Ydb::StatusIds::BAD_SESSION);
    }
}
