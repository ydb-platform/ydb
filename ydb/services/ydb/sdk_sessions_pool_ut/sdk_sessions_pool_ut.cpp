#include "ydb_common_ut.h"

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <util/thread/pool.h>

#include <random>
#include <thread>

using namespace NYdb;
using namespace NYdb::NTable;

class TDefaultTestSetup {
public:
    TDefaultTestSetup(ui32 maxActiveSessions)
        : Driver_(NYdb::TDriver(
            TDriverConfig().SetEndpoint(
                TStringBuilder() << "localhost:" << Server_.GetPort()
            )
        ))
        , Client_(
            Driver_,
            TClientSettings().SessionPoolSettings(
                TSessionPoolSettings()
                    .MaxActiveSessions(maxActiveSessions)
                    .KeepAliveIdleThreshold(TDuration::MilliSeconds(10))
                    .CloseIdleThreshold(TDuration::MilliSeconds(10))
            )
        )
    {
    }

    ~TDefaultTestSetup() {
        Driver_.Stop(true);
    }

    NYdb::NTable::TTableClient& GetClient() {
        return Client_;
    }

private:
    TKikimrWithGrpcAndRootSchema Server_;
    NYdb::TDriver Driver_;
    NYdb::NTable::TTableClient Client_;
};


enum class EAction: ui8 {
    CreateFuture,
    ExtractValue,
    Return
};
using TPlan = TVector<std::pair<EAction, ui32>>;


void CheckPlan(TPlan plan) {
    THashMap<ui32, EAction> sessions;
    for (const auto& [action, sessionId]: plan) {
        if (action == EAction::CreateFuture) {
            UNIT_ASSERT(!sessions.contains(sessionId));
        } else {
            UNIT_ASSERT(sessions.contains(sessionId));
            switch (sessions.at(sessionId)) {
                case EAction::CreateFuture: {
                    UNIT_ASSERT(action == EAction::ExtractValue);
                    break;
                }
                case EAction::ExtractValue: {
                    UNIT_ASSERT(action == EAction::Return);
                    break;
                }
                default: {
                    UNIT_ASSERT(false);
                }
            }
        }
        sessions[sessionId] = action;
    }
}

void RunPlan(const TPlan& plan, NYdb::NTable::TTableClient& client) {
    THashMap<ui32, NThreading::TFuture<NYdb::NTable::TCreateSessionResult>> sessionFutures;
    THashMap<ui32, NYdb::NTable::TCreateSessionResult> sessions;

    ui32 requestedSessions = 0;

    for (const auto& [action, sessionId]: plan) {
        switch (action) {
            case EAction::CreateFuture: {
                sessionFutures.emplace(sessionId, client.GetSession());
                ++requestedSessions;
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                if (requestedSessions > client.GetActiveSessionsLimit()) {
                    UNIT_ASSERT(client.GetActiveSessionCount() == client.GetActiveSessionsLimit());
                }
                UNIT_ASSERT(!sessionFutures.at(sessionId).HasValue());
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
        UNIT_ASSERT(client.GetActiveSessionCount() <= client.GetActiveSessionsLimit());
        UNIT_ASSERT(client.GetActiveSessionCount() >= static_cast<i64>(sessions.size()));
        UNIT_ASSERT(client.GetActiveSessionCount() <= static_cast<i64>(sessions.size() + sessionFutures.size()));
    }
}

int GetRand(std::mt19937& rng, int min, int max) {
    std::uniform_int_distribution<std::mt19937::result_type> dist(min, max);
    return dist(rng);
}


TPlan GenerateRandomPlan(ui32 numSessions) {
    TPlan plan;
    std::random_device dev;
    std::mt19937 rng(dev());

    for (ui32 i = 0; i < numSessions; ++i) {
        std::uniform_int_distribution<std::mt19937::result_type> dist(0, plan.size());
        ui32 prevPos = 0;
        for (EAction action: {EAction::CreateFuture, EAction::ExtractValue, EAction::Return}) {
            int pos = GetRand(rng, prevPos, plan.size());
            plan.emplace(plan.begin() + pos, std::make_pair(action, i));
            prevPos = pos + 1;
        }
    }
    return plan;
}


Y_UNIT_TEST_SUITE(YdbSdkSessionsPool) {
    Y_UNIT_TEST(Get1Session) {
        TDefaultTestSetup setup(1);
        auto& client = setup.GetClient();

        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionsLimit(), 1);
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), 0);

        {
            auto session = client.GetSession().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), 1);
    }

    void TestWaitQueue(NYdb::NTable::TTableClient& client, ui32 activeSessionsLimit) {
        std::vector<NThreading::TFuture<NYdb::NTable::TCreateSessionResult>> sessionFutures;
        std::vector<NYdb::NTable::TCreateSessionResult> sessions;

        // exhaust the pool
        for (ui32 i = 0; i < activeSessionsLimit; ++i) {
            sessions.emplace_back(client.GetSession().ExtractValueSync());
        }
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), activeSessionsLimit);

        // next should be in the wait queue
        for (ui32 i = 0; i < activeSessionsLimit * 10; ++i) {
            sessionFutures.emplace_back(client.GetSession());
        }
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), activeSessionsLimit);

        // next should be a fake session
        {
            auto brokenSession = client.GetSession().ExtractValueSync();
            UNIT_ASSERT(!brokenSession.IsSuccess());
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        for (auto& sessionFuture: sessionFutures) {
            UNIT_ASSERT(!sessionFuture.HasValue());
        }

        for (auto& sessionFuture: sessionFutures) {
            sessions.erase(sessions.begin());
            sessions.emplace_back(sessionFuture.ExtractValueSync());
        }
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), activeSessionsLimit);
    }

    Y_UNIT_TEST(WaitQueue1) {
        ui32 activeSessionsLimit = 1;

        TDefaultTestSetup setup(activeSessionsLimit);
        auto& client = setup.GetClient();

        TestWaitQueue(client, activeSessionsLimit);
    }

    Y_UNIT_TEST(WaitQueue10) {
        ui32 activeSessionsLimit = 10;

        TDefaultTestSetup setup(activeSessionsLimit);
        auto& client = setup.GetClient();

        TestWaitQueue(client, activeSessionsLimit);
    }

    Y_UNIT_TEST(RunSmallPlan) {
        TDefaultTestSetup setup(1);
        auto& client = setup.GetClient();

        TPlan plan{
            {EAction::CreateFuture, 1},
            {EAction::ExtractValue, 1},
            {EAction::CreateFuture, 2},
            {EAction::Return, 1},
            {EAction::ExtractValue, 2},
            {EAction::Return, 2}
        };
        CheckPlan(plan);
        RunPlan(plan, client);

        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), 1);
    }

    Y_UNIT_TEST(CustomPlan) {
        TDefaultTestSetup setup(1);
        auto& client = setup.GetClient();

        TPlan plan{
            {EAction::CreateFuture, 1}
        };
        CheckPlan(plan);
        RunPlan(plan, client);

        std::this_thread::sleep_for(std::chrono::milliseconds(10000));

        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
    }

    ui32 RunStressTestSync(ui32 n, ui32 activeSessionsLimit, NYdb::NTable::TTableClient& client) {
        std::vector<NThreading::TFuture<NYdb::NTable::TCreateSessionResult>> sessionFutures;
        std::vector<NYdb::NTable::TCreateSessionResult> sessions;
        std::mt19937 rng(0);
        ui32 successCount = 0;

        for (ui32 i = 0; i < activeSessionsLimit * 12; ++i) {
            sessionFutures.emplace_back(client.GetSession());
        }

        for (ui32 i = 0; i < n; ++i) {
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

    Y_UNIT_TEST(StressTestSync1) {
        ui32 activeSessionsLimit = 1;

        TDefaultTestSetup setup(activeSessionsLimit);
        auto& client = setup.GetClient();

        RunStressTestSync(1000, activeSessionsLimit, client);

        std::this_thread::sleep_for(std::chrono::milliseconds(10000));

        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), activeSessionsLimit);
    }

    Y_UNIT_TEST(StressTestSync10) {
        ui32 activeSessionsLimit = 10;

        TDefaultTestSetup setup(activeSessionsLimit);
        auto& client = setup.GetClient();

        RunStressTestSync(1000, activeSessionsLimit, client);

        std::this_thread::sleep_for(std::chrono::milliseconds(10000));

        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), activeSessionsLimit);
    }

    ui32 RunStressTestAsync(ui32 n, ui32 nThreads, NYdb::NTable::TTableClient& client) {
        std::atomic<ui32> successCount(0);
        std::atomic<ui32> jobIndex(0);

        auto job = [&client, &successCount, &jobIndex, n]() mutable {
            std::mt19937 rng(++jobIndex);
            for (ui32 i = 0; i < n; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(GetRand(rng, 1, 100)));
                auto sessionFuture = client.GetSession();
                std::this_thread::sleep_for(std::chrono::milliseconds(GetRand(rng, 1, 100)));
                auto session = sessionFuture.ExtractValueSync();
                std::this_thread::sleep_for(std::chrono::milliseconds(GetRand(rng, 1, 100)));
                successCount += session.IsSuccess();
            }
        };

        IThreadFactory* pool = SystemThreadFactory();
        TVector<TAutoPtr<IThreadFactory::IThread>> threads;
        threads.resize(nThreads);
        for (ui32 i = 0; i < nThreads; i++) {
            threads[i] = pool->Run(job);
        }
        for (ui32 i = 0; i < nThreads; i++) {
            threads[i]->Join();
        }

        return successCount;
    }

    Y_UNIT_TEST(StressTestAsync1) {
        ui32 activeSessionsLimit = 1;

        TDefaultTestSetup setup(activeSessionsLimit);
        auto& client = setup.GetClient();

        RunStressTestAsync(100, 10, client);

        std::this_thread::sleep_for(std::chrono::milliseconds(10000));

        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), activeSessionsLimit);
    }

    Y_UNIT_TEST(StressTestAsync10) {
        ui32 activeSessionsLimit = 10;

        TDefaultTestSetup setup(activeSessionsLimit);
        auto& client = setup.GetClient();

        RunStressTestAsync(1000, 10, client);

        std::this_thread::sleep_for(std::chrono::milliseconds(10000));

        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), activeSessionsLimit);
    }

    void TestPeriodicTask(ui32 activeSessionsLimit, NYdb::NTable::TTableClient& client) {
        std::vector<NThreading::TFuture<NYdb::NTable::TCreateSessionResult>> sessionFutures;
        std::vector<NYdb::NTable::TCreateSessionResult> sessions;

        for (ui32 i = 0; i < activeSessionsLimit; ++i) {
            sessions.emplace_back(client.GetSession().ExtractValueSync());
            UNIT_ASSERT_VALUES_EQUAL(sessions.back().IsSuccess(), true);
        }

        for (ui32 i = 0; i < activeSessionsLimit; ++i) {
            sessionFutures.emplace_back(client.GetSession());
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        for (auto& sessionFuture : sessionFutures) {
            UNIT_ASSERT(!sessionFuture.HasValue());
        }

        // Wait for wait session timeout
        std::this_thread::sleep_for(std::chrono::milliseconds(10000));

        for (auto& sessionFuture : sessionFutures) {
            UNIT_ASSERT(sessionFuture.HasValue());
            UNIT_ASSERT(!sessionFuture.ExtractValueSync().IsSuccess());
        }

        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), activeSessionsLimit);

        sessionFutures.clear();
        sessions.clear();

        std::this_thread::sleep_for(std::chrono::milliseconds(10000));
        UNIT_ASSERT_VALUES_EQUAL(client.GetActiveSessionCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(client.GetCurrentPoolSize(), activeSessionsLimit);
    }

    Y_UNIT_TEST(PeriodicTask1) {
        ui32 activeSessionsLimit = 1;

        TDefaultTestSetup setup(activeSessionsLimit);
        auto& client = setup.GetClient();
        
        TestPeriodicTask(activeSessionsLimit, client);
    }

    Y_UNIT_TEST(PeriodicTask10) {
        ui32 activeSessionsLimit = 10;

        TDefaultTestSetup setup(activeSessionsLimit);
        auto& client = setup.GetClient();
        
        TestPeriodicTask(activeSessionsLimit, client);
    }

    Y_UNIT_TEST(FailTest) {
        // This test reproduces bug from KIKIMR-18063
        TDefaultTestSetup setup(1);
        auto& client = setup.GetClient();

        auto sessionFromPool = client.GetSession().ExtractValueSync();
        auto futureInWaitPool = client.GetSession();

        {
            auto standaloneSessionThatWillBeBroken = client.CreateSession().ExtractValueSync();
            auto res = standaloneSessionThatWillBeBroken.GetSession().ExecuteDataQuery("SELECT COUNT(*) FROM `Root/Test`;",
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                NYdb::NTable::TExecDataQuerySettings().ClientTimeout(TDuration::MicroSeconds(10))).GetValueSync();
        }
    }
}
