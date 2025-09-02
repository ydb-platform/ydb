#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <random>
#include <thread>

using namespace NYdb;
using namespace NYdb::NTable;

class YdbSdkSessionsPool : public ::testing::TestWithParam<std::uint32_t> {
protected:
    void SetUp() override {
        std::uint32_t maxActiveSessions = GetParam();
        Driver = std::make_unique<NYdb::TDriver>(TDriverConfig().SetEndpoint(std::getenv("YDB_ENDPOINT")));

        auto clientSettings = TClientSettings().SessionPoolSettings(
            TSessionPoolSettings()
                .MaxActiveSessions(maxActiveSessions)
                .KeepAliveIdleThreshold(TDuration::MilliSeconds(10))
                .CloseIdleThreshold(TDuration::MilliSeconds(10)));
        Client = std::make_unique<NYdb::NTable::TTableClient>(*Driver, clientSettings);
    }

    void TearDown() override {
        Driver->Stop(true);
    }

protected:
    std::unique_ptr<NYdb::TDriver> Driver;
    std::unique_ptr<NYdb::NTable::TTableClient> Client;
};

class YdbSdkSessionsPool1Session : public YdbSdkSessionsPool {};

enum class EAction: std::uint8_t {
    CreateFuture,
    ExtractValue,
    Return
};
using TPlan = std::vector<std::pair<EAction, std::uint32_t>>;


void CheckPlan(TPlan plan) {
    std::unordered_map<std::uint32_t, EAction> sessions;
    for (const auto& [action, sessionId]: plan) {
        if (action == EAction::CreateFuture) {
            ASSERT_FALSE(sessions.contains(sessionId));
        } else {
            ASSERT_TRUE(sessions.contains(sessionId));
            switch (sessions.at(sessionId)) {
                case EAction::CreateFuture: {
                    ASSERT_EQ(action, EAction::ExtractValue);
                    break;
                }
                case EAction::ExtractValue: {
                    ASSERT_EQ(action, EAction::Return);
                    break;
                }
                default: {
                    ASSERT_TRUE(false);
                }
            }
        }
        sessions[sessionId] = action;
    }
}

void RunPlan(const TPlan& plan, NYdb::NTable::TTableClient& client) {
    std::unordered_map<std::uint32_t, NThreading::TFuture<NYdb::NTable::TCreateSessionResult>> sessionFutures;
    std::unordered_map<std::uint32_t, NYdb::NTable::TCreateSessionResult> sessions;

    std::uint32_t requestedSessions = 0;

    for (const auto& [action, sessionId]: plan) {
        switch (action) {
            case EAction::CreateFuture: {
                sessionFutures.emplace(sessionId, client.GetSession());
                ++requestedSessions;
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                if (requestedSessions > client.GetActiveSessionsLimit()) {
                    ASSERT_EQ(client.GetActiveSessionCount(), client.GetActiveSessionsLimit());
                }
                ASSERT_FALSE(sessionFutures.at(sessionId).HasValue());
                break;
            }
            case EAction::ExtractValue: {
                auto it = sessionFutures.find(sessionId);
                auto session = it->second.ExtractValueSync();
                sessionFutures.erase(it);
                sessions.emplace(sessionId, std::move(session));
                break;
            }
            case EAction::Return: {
                sessions.erase(sessionId);
                --requestedSessions;
                break;
            }
        }
        ASSERT_LE(client.GetActiveSessionCount(), client.GetActiveSessionsLimit());
        ASSERT_GE(client.GetActiveSessionCount(), static_cast<std::int64_t>(sessions.size()));
        ASSERT_LE(client.GetActiveSessionCount(), static_cast<std::int64_t>(sessions.size() + sessionFutures.size()));
    }
}

int GetRand(std::mt19937& rng, int min, int max) {
    std::uniform_int_distribution<std::mt19937::result_type> dist(min, max);
    return dist(rng);
}


TPlan GenerateRandomPlan(std::uint32_t numSessions) {
    TPlan plan;
    std::random_device dev;
    std::mt19937 rng(dev());

    for (std::uint32_t i = 0; i < numSessions; ++i) {
        std::uniform_int_distribution<std::mt19937::result_type> dist(0, plan.size());
        std::uint32_t prevPos = 0;
        for (EAction action: {EAction::CreateFuture, EAction::ExtractValue, EAction::Return}) {
            int pos = GetRand(rng, prevPos, plan.size());
            plan.emplace(plan.begin() + pos, std::make_pair(action, i));
            prevPos = pos + 1;
        }
    }
    return plan;
}


TEST_P(YdbSdkSessionsPool1Session, GetSession) {
    ASSERT_EQ(Client->GetActiveSessionsLimit(), 1);
    ASSERT_EQ(Client->GetActiveSessionCount(), 0);
    ASSERT_EQ(Client->GetCurrentPoolSize(), 0);

    {
        auto session = Client->GetSession().ExtractValueSync();

        ASSERT_EQ(session.GetStatus(), EStatus::SUCCESS);
        ASSERT_EQ(Client->GetActiveSessionCount(), 1);
        ASSERT_EQ(Client->GetCurrentPoolSize(), 0);
    }

    ASSERT_EQ(Client->GetActiveSessionCount(), 0);
    ASSERT_EQ(Client->GetCurrentPoolSize(), 1);
}

void TestWaitQueue(NYdb::NTable::TTableClient& client, std::uint32_t activeSessionsLimit) {
    std::vector<NThreading::TFuture<NYdb::NTable::TCreateSessionResult>> sessionFutures;
    std::vector<NYdb::NTable::TCreateSessionResult> sessions;

    // exhaust the pool
    for (std::uint32_t i = 0; i < activeSessionsLimit; ++i) {
        sessions.emplace_back(client.GetSession().ExtractValueSync());
    }
    ASSERT_EQ(client.GetActiveSessionCount(), activeSessionsLimit);

    // next should be in the wait queue
    for (std::uint32_t i = 0; i < activeSessionsLimit * 10; ++i) {
        sessionFutures.emplace_back(client.GetSession());
    }
    ASSERT_EQ(client.GetActiveSessionCount(), activeSessionsLimit);

    // next should be a fake session
    {
        auto brokenSession = client.GetSession().ExtractValueSync();
        ASSERT_FALSE(brokenSession.IsSuccess());
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    for (auto& sessionFuture: sessionFutures) {
        ASSERT_FALSE(sessionFuture.HasValue());
    }

    for (auto& sessionFuture: sessionFutures) {
        sessions.erase(sessions.begin());
        sessions.emplace_back(sessionFuture.ExtractValueSync());
    }
    ASSERT_EQ(client.GetActiveSessionCount(), activeSessionsLimit);
}

TEST_P(YdbSdkSessionsPool, WaitQueue) {
    TestWaitQueue(*Client, GetParam());
}

TEST_P(YdbSdkSessionsPool1Session, RunSmallPlan) {    
    TPlan plan{
        {EAction::CreateFuture, 1},
        {EAction::ExtractValue, 1},
        {EAction::CreateFuture, 2},
        {EAction::Return, 1},
        {EAction::ExtractValue, 2},
        {EAction::Return, 2}
    };
    CheckPlan(plan);
    RunPlan(plan, *Client);

    ASSERT_EQ(Client->GetActiveSessionCount(), 0);
    ASSERT_EQ(Client->GetCurrentPoolSize(), 1);
}

TEST_P(YdbSdkSessionsPool1Session, CustomPlan) {
    TPlan plan{
        {EAction::CreateFuture, 1}
    };
    CheckPlan(plan);
    RunPlan(plan, *Client);

    std::this_thread::sleep_for(std::chrono::milliseconds(10000));

    ASSERT_EQ(Client->GetActiveSessionCount(), 0);
}

std::uint32_t RunStressTestSync(std::uint32_t n, std::uint32_t activeSessionsLimit, NYdb::NTable::TTableClient& client) {
    std::vector<NThreading::TFuture<NYdb::NTable::TCreateSessionResult>> sessionFutures;
    std::vector<NYdb::NTable::TCreateSessionResult> sessions;
    std::mt19937 rng(0);
    std::uint32_t successCount = 0;

    for (std::uint32_t i = 0; i < activeSessionsLimit * 12; ++i) {
        sessionFutures.emplace_back(client.GetSession());
    }

    for (std::uint32_t i = 0; i < n; ++i) {
        switch (static_cast<EAction>(GetRand(rng, 0, 2))) {
            case EAction::CreateFuture: {
                sessionFutures.emplace_back(client.GetSession());
                break;
            }
            case EAction::ExtractValue: {
                if (sessionFutures.empty()) {
                    break;
                }
                auto ind = GetRand(rng, 0, sessionFutures.size() - 1);
                auto sessionFuture = sessionFutures[ind];
                if (sessionFuture.HasValue()) {
                    auto session = sessionFuture.ExtractValueSync();
                    if (session.IsSuccess()) {
                        ++successCount;
                    }
                    sessions.emplace_back(std::move(session));
                    sessionFutures.erase(sessionFutures.begin() + ind);
                    break;
                }
                break;
            }
            case EAction::Return: {
                if (sessions.empty()) {
                    break;
                }
                auto ind = GetRand(rng, 0, sessions.size() - 1);
                sessions.erase(sessions.begin() + ind);
                break;
            }
        }
    }
    return successCount;
}

TEST_P(YdbSdkSessionsPool, StressTestSync) {
    std::uint32_t activeSessionsLimit = GetParam();

    RunStressTestSync(1000, activeSessionsLimit, *Client);

    std::this_thread::sleep_for(std::chrono::milliseconds(10000));

    ASSERT_EQ(Client->GetActiveSessionCount(), 0);
    ASSERT_EQ(Client->GetCurrentPoolSize(), activeSessionsLimit);
}

std::uint32_t RunStressTestAsync(std::uint32_t n, std::uint32_t nThreads, NYdb::NTable::TTableClient& client) {
    std::atomic<std::uint32_t> successCount(0);
    std::atomic<std::uint32_t> jobIndex(0);

    auto job = [&client, &successCount, &jobIndex, n]() mutable {
        std::mt19937 rng(++jobIndex);
        for (std::uint32_t i = 0; i < n; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(GetRand(rng, 1, 100)));
            auto sessionFuture = client.GetSession();
            std::this_thread::sleep_for(std::chrono::milliseconds(GetRand(rng, 1, 100)));
            auto session = sessionFuture.ExtractValueSync();
            std::this_thread::sleep_for(std::chrono::milliseconds(GetRand(rng, 1, 100)));
            successCount += session.IsSuccess();
        }
    };

    std::vector<std::thread> threads;
    for (std::uint32_t i = 0; i < nThreads; i++) {
        threads.emplace_back(job);
    }
    for (auto& thread: threads) {
        thread.join();
    }

    return successCount;
}

TEST_P(YdbSdkSessionsPool, StressTestAsync) {
    std::uint32_t activeSessionsLimit = GetParam();
    std::uint32_t iterations = (activeSessionsLimit == 1) ? 100 : 1000;

    RunStressTestAsync(iterations, 10, *Client);

    std::this_thread::sleep_for(std::chrono::milliseconds(10000));

    ASSERT_EQ(Client->GetActiveSessionCount(), 0);
    ASSERT_EQ(Client->GetCurrentPoolSize(), activeSessionsLimit);
}

void TestPeriodicTask(std::uint32_t activeSessionsLimit, NYdb::NTable::TTableClient& client) {
    std::vector<NThreading::TFuture<NYdb::NTable::TCreateSessionResult>> sessionFutures;
    std::vector<NYdb::NTable::TCreateSessionResult> sessions;

    for (std::uint32_t i = 0; i < activeSessionsLimit; ++i) {
        sessions.emplace_back(client.GetSession().ExtractValueSync());
        ASSERT_TRUE(sessions.back().IsSuccess());
    }

    for (std::uint32_t i = 0; i < activeSessionsLimit; ++i) {
        sessionFutures.emplace_back(client.GetSession());
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    for (auto& sessionFuture : sessionFutures) {
        ASSERT_FALSE(sessionFuture.HasValue());
    }

    // Wait for wait session timeout
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));

    for (auto& sessionFuture : sessionFutures) {
        ASSERT_TRUE(sessionFuture.HasValue());
        ASSERT_FALSE(sessionFuture.ExtractValueSync().IsSuccess());
    }

    ASSERT_EQ(client.GetActiveSessionCount(), activeSessionsLimit);

    sessionFutures.clear();
    sessions.clear();

    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    ASSERT_EQ(client.GetActiveSessionCount(), 0);
    ASSERT_EQ(client.GetCurrentPoolSize(), activeSessionsLimit);
}

TEST_P(YdbSdkSessionsPool, PeriodicTask) {
    TestPeriodicTask(GetParam(), *Client);
}

TEST_P(YdbSdkSessionsPool1Session, FailTest) {
    // This test reproduces bug from KIKIMR-18063
    auto sessionFromPool = Client->GetSession().ExtractValueSync();
    auto futureInWaitPool = Client->GetSession();

    {
        auto standaloneSessionThatWillBeBroken = Client->CreateSession().ExtractValueSync();
        auto res = standaloneSessionThatWillBeBroken.GetSession().ExecuteDataQuery("SELECT COUNT(*) FROM `Root/Test`;",
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
            NYdb::NTable::TExecDataQuerySettings().ClientTimeout(TDuration::MicroSeconds(10))).GetValueSync();
    }
}

INSTANTIATE_TEST_SUITE_P(, YdbSdkSessionsPool, ::testing::Values(1, 10));

INSTANTIATE_TEST_SUITE_P(, YdbSdkSessionsPool1Session, ::testing::Values(1));
