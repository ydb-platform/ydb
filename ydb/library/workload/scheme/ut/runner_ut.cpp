#include <ydb/library/workload/scheme/runner.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

using namespace NYdbWorkload::NScheme;

namespace {

class TClient final : public IClient {
public:
    void PrepareDirectory(const TString&) override { ++Directories; }

    void Create(const TObject& object) override {
        Enter(object, true);
        Y_DEFER { Leave(object, true); };
        if (BeforeCreate) {
            BeforeCreate(object);
        }
        std::lock_guard guard(Mutex);
        Y_ENSURE(!FailCreate, "Uncertain CREATE outcome");
        Y_ENSURE(!Present.contains(object.Path), "Duplicate create");
        Present[object.Path] = false;
        ++Creates;
    }

    EObjectState Inspect(const TObject& object) override {
        if (BeforeInspect) {
            BeforeInspect(object);
        }
        std::lock_guard guard(Mutex);
        ++Inspects;
        Y_ENSURE(object.Path != InvalidPath, "Invalid schema");
        if (!Present.contains(object.Path)) {
            return EObjectState::Missing;
        }
        return Present.at(object.Path) ? EObjectState::WithBatch : EObjectState::WithoutBatch;
    }

    void Alter(const TObject& object, bool add) override {
        Enter(object, false);
        Y_DEFER { Leave(object, false); };
        if (BeforeAlter) {
            BeforeAlter(object);
        }
        std::lock_guard guard(Mutex);
        ++Alters;
        Y_ENSURE(!FailAlter, "Uncertain ALTER outcome");
        Y_ENSURE(Present.at(object.Path) != add, "Wrong ALTER direction");
        Present[object.Path] = add;
        FirstAlterAdds.emplace(object.Path, add);
        if (add) {
            ++Adds;
        } else {
            ++Drops;
        }
        BatchSizes.insert(object.AlterBatchSize);
    }

    void Enter(const TObject& object, bool create) {
        std::unique_lock lock(Mutex);
        Y_ENSURE(!Busy.contains(object.Path), "Concurrent operations on one object");
        Busy.insert(object.Path);
        ++(create ? ActiveCreates : ActiveAlters);
        if (create) {
            ++CreateAttempts;
        }
        MaxCreates = std::max(MaxCreates, ActiveCreates);
        MaxAlters = std::max(MaxAlters, ActiveAlters);
        MaxActive = std::max(MaxActive, Busy.size());
        if (GateSize && MaxActive >= GateSize) {
            GateOpen = true;
            Condition.notify_all();
        }
        if (GateSize) {
            Y_ENSURE(Condition.wait_for(lock, std::chrono::seconds(5), [&] { return GateOpen; }), "Workers did not reach configured concurrency");
        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    void Leave(const TObject& object, bool create) {
        std::lock_guard guard(Mutex);
        Busy.erase(object.Path);
        --(create ? ActiveCreates : ActiveAlters);
        Condition.notify_all();
    }

    std::mutex Mutex;
    std::condition_variable Condition;
    THashMap<TString, bool> Present;
    THashMap<TString, bool> FirstAlterAdds;
    THashSet<TString> Busy;
    THashSet<ui32> BatchSizes;
    size_t GateSize = 0;
    bool GateOpen = false;
    size_t MaxActive = 0;
    size_t ActiveCreates = 0;
    size_t ActiveAlters = 0;
    size_t MaxCreates = 0;
    size_t MaxAlters = 0;
    ui64 CreateAttempts = 0;
    ui64 Directories = 0;
    ui64 Creates = 0;
    ui64 Inspects = 0;
    ui64 Alters = 0;
    ui64 Adds = 0;
    ui64 Drops = 0;
    bool FailAlter = false;
    bool FailCreate = false;
    TString InvalidPath;
    std::function<void(const TObject&)> BeforeCreate;
    std::function<void(const TObject&)> BeforeAlter;
    std::function<void(const TObject&)> BeforeInspect;
};

TConfig Config() {
    return ParseConfig(R"(
path: load
create_concurrency: 3
alter_concurrency: 2
tables: {count: 3, partitions: 8, alter_batch_size: 4}
topics: {count: 2, partitions: 2, alter_batch_size: 3}
)");
}

} // namespace

Y_UNIT_TEST_SUITE(SchemeWorkloadRunner) {
    Y_UNIT_TEST(CreateConcurrencyAndBothTypes) {
        const auto config = Config();
        TClient client;
        client.GateSize = config.CreateConcurrency;
        TRunner runner(config, client);
        runner.Init();
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 5);
        UNIT_ASSERT_VALUES_EQUAL(client.MaxActive, 3);
        UNIT_ASSERT_VALUES_EQUAL(runner.Statistics()[static_cast<size_t>(EOperation::CreateTable)], 3);
        UNIT_ASSERT_VALUES_EQUAL(runner.Statistics()[static_cast<size_t>(EOperation::CreateTopic)], 2);
    }

    Y_UNIT_TEST(AlterConcurrencyAndRestartFromMixedState) {
        const auto config = Config();
        TClient client;
        for (ui64 i = 0; i < config.ObjectCount(); ++i) {
            client.Present[config.Object(i).Path] = i % 2;
        }
        client.GateSize = config.AlterConcurrency;
        TRunner runner(config, client);
        runner.Run(TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(client.MaxActive, 2);
        UNIT_ASSERT_VALUES_EQUAL(client.Inspects, 5);
        UNIT_ASSERT_GT(client.Adds, 0);
        UNIT_ASSERT_GT(client.Drops, 0);
        UNIT_ASSERT(client.BatchSizes.contains(3));
        UNIT_ASSERT(client.BatchSizes.contains(4));
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 0);
        for (ui64 i = 0; i < config.ObjectCount(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(client.FirstAlterAdds.at(config.Object(i).Path), !(i % 2));
        }
        const ui64 before = client.Alters;
        TRunner(config, client).Run(TDuration::MilliSeconds(100));
        UNIT_ASSERT_GT(client.Alters, before);
        UNIT_ASSERT_VALUES_EQUAL(client.Inspects, 10);
    }

    Y_UNIT_TEST(DoesNotExceedObjectCount) {
        const auto config = ParseConfig("path: load\ncreate_concurrency: 50\nalter_concurrency: 50\ntables: {count: 1, partitions: 1}");
        TClient client;
        TRunner runner(config, client);
        runner.Init();
        runner.Run(TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(client.MaxActive, 1);
    }

    Y_UNIT_TEST(StopsOnUnknownOutcomeWithoutRetryOrStateAdvance) {
        auto config = Config();
        config.AlterConcurrency = 1;
        TClient client;
        TRunner runner(config, client);
        runner.Init();
        client.FailAlter = true;
        UNIT_ASSERT_EXCEPTION_CONTAINS(runner.Run(TDuration::Seconds(10)), yexception, "Uncertain ALTER outcome");
        UNIT_ASSERT_VALUES_EQUAL(client.Alters, 1);
        UNIT_ASSERT_VALUES_EQUAL(client.Adds, 0);
        UNIT_ASSERT_VALUES_EQUAL(runner.Statistics()[static_cast<size_t>(EOperation::AddColumns)], 0);
        UNIT_ASSERT(client.Busy.empty());
    }

    Y_UNIT_TEST(ValidatesAllObjectsBeforeCreatingOrAltering) {
        const auto config = Config();
        TClient client;
        client.Present[config.Object(0).Path] = false;
        client.Present[config.Object(4).Path] = true;
        client.InvalidPath = config.Object(4).Path;
        TRunner runner(config, client);
        UNIT_ASSERT_EXCEPTION_CONTAINS(runner.Run(TDuration::Seconds(1)), yexception, "Invalid schema");
        UNIT_ASSERT_VALUES_EQUAL(client.Alters, 0);
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 0);
    }

    Y_UNIT_TEST(InterruptBeforeStarting) {
        const auto config = Config();
        TClient client;
        TRunner runner(config, client, [] { return true; });
        UNIT_ASSERT_EXCEPTION(runner.Init(), yexception);
        UNIT_ASSERT_EXCEPTION(runner.Run(TDuration::Seconds(1)), yexception);
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 0);
        UNIT_ASSERT_VALUES_EQUAL(client.Alters, 0);
        UNIT_ASSERT_VALUES_EQUAL(client.Directories, 0);
    }

    Y_UNIT_TEST(InitPreparesOnlyInitialCounts) {
        auto config = Config();
        config.Tables->InitialCount = 1;
        config.Topics->InitialCount = 1;
        TClient client;
        TRunner(config, client).Init();
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 2);
        UNIT_ASSERT(client.Present.contains("load/table_0"));
        UNIT_ASSERT(client.Present.contains("load/topic_0"));
        TRunner runner(config, client);
        runner.Run(TDuration::MilliSeconds(500));
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 5);
        UNIT_ASSERT_VALUES_EQUAL(runner.Statistics()[static_cast<size_t>(EOperation::CreateTable)], 2);
        UNIT_ASSERT_VALUES_EQUAL(runner.Statistics()[static_cast<size_t>(EOperation::CreateTopic)], 1);
    }

    Y_UNIT_TEST(ZeroInitialCountsPrepareDirectoryOnly) {
        auto config = Config();
        config.Tables->InitialCount = 0;
        config.Topics->InitialCount = 0;
        TClient client;
        TRunner(config, client).Init();
        UNIT_ASSERT_VALUES_EQUAL(client.Directories, 1);
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 0);
        UNIT_ASSERT_VALUES_EQUAL(client.Inspects, 0);
    }

    Y_UNIT_TEST(RunUsesActualObjectsIncludingHoles) {
        const auto config = Config();
        TClient client;
        // The configured initial count is the full target, but the real set is incomplete.
        client.Present[config.Object(1).Path] = true;
        client.Present[config.Object(4).Path] = false;
        TRunner runner(config, client);
        runner.Run(TDuration::MilliSeconds(500));
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 3);
        UNIT_ASSERT_VALUES_EQUAL(client.Present.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(client.FirstAlterAdds.at(config.Object(1).Path), false);
        UNIT_ASSERT_VALUES_EQUAL(client.FirstAlterAdds.at(config.Object(4).Path), true);
    }

    Y_UNIT_TEST(CreatesAndAltersHaveIndependentSharedLimits) {
        const auto config = Config();
        TClient client;
        client.Present[config.Object(0).Path] = false;
        client.Present[config.Object(3).Path] = true;
        // Both operation types must occupy all their slots together to open this gate.
        client.GateSize = config.CreateConcurrency + config.AlterConcurrency;
        TRunner(config, client).Run(TDuration::MilliSeconds(500));
        UNIT_ASSERT_VALUES_EQUAL(client.MaxCreates, 3);
        UNIT_ASSERT_VALUES_EQUAL(client.MaxAlters, 2);
        UNIT_ASSERT_VALUES_EQUAL(client.MaxActive, 5);
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 3);
        UNIT_ASSERT(client.Busy.empty());
    }

    Y_UNIT_TEST(NewObjectsCanBeAlteredBeforeTheRemainingCreatesFinish) {
        auto config = Config();
        config.CreateConcurrency = 1;
        config.AlterConcurrency = 1;
        TClient client;
        client.BeforeCreate = [&](const TObject& object) {
            if (object.Path == config.Object(1).Path) {
                std::unique_lock lock(client.Mutex);
                Y_ENSURE(client.Condition.wait_for(lock, std::chrono::seconds(5), [&] { return client.Alters > 0; }),
                    "ALTER waited for all CREATEs to finish");
            }
        };
        TRunner(config, client).Run(TDuration::MilliSeconds(500));
        UNIT_ASSERT_VALUES_EQUAL(client.Directories, 1);
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 5);
        UNIT_ASSERT_GT(client.Adds, 0);
        UNIT_ASSERT_GT(client.Drops, 0);
        UNIT_ASSERT_VALUES_EQUAL(client.MaxCreates, 1);
        UNIT_ASSERT_VALUES_EQUAL(client.MaxAlters, 1);
    }

    Y_UNIT_TEST(DeadlineStopsNewWorkAndDrainsCreates) {
        auto config = Config();
        config.CreateConcurrency = 1;
        TClient client;
        client.BeforeCreate = [](const TObject&) { std::this_thread::sleep_for(std::chrono::milliseconds(200)); };
        TRunner(config, client).Run(TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 1);
        UNIT_ASSERT_VALUES_EQUAL(client.Alters, 0);
        UNIT_ASSERT(client.Busy.empty());
    }

    Y_UNIT_TEST(DurationStartsAfterInspection) {
        auto config = Config();
        config.AlterConcurrency = 1;
        TClient client;
        client.BeforeInspect = [](const TObject&) { std::this_thread::sleep_for(std::chrono::milliseconds(50)); };
        TRunner(config, client).Run(TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(client.Inspects, 5);
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 5);
        UNIT_ASSERT_GT(client.Alters, 0);
    }

    Y_UNIT_TEST(MixedFailuresStopBothOperationTypesAndDrainRequests) {
        for (bool failCreate : {false, true}) {
            auto config = Config();
            config.CreateConcurrency = 1;
            config.AlterConcurrency = 1;
            TClient client;
            client.Present[config.Object(0).Path] = false;
            client.GateSize = 2;
            client.FailCreate = failCreate;
            client.FailAlter = !failCreate;
            // The successful operation finishes after the other operation has failed.
            auto& slow = failCreate ? client.BeforeAlter : client.BeforeCreate;
            slow = [](const TObject&) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); };
            TRunner runner(config, client);
            UNIT_ASSERT_EXCEPTION_CONTAINS(runner.Run(TDuration::Seconds(5)), yexception,
                failCreate ? "Uncertain CREATE outcome" : "Uncertain ALTER outcome");
            UNIT_ASSERT_VALUES_EQUAL(client.CreateAttempts, 1);
            UNIT_ASSERT_VALUES_EQUAL(client.Alters, 1);
            UNIT_ASSERT_VALUES_EQUAL(client.Creates, failCreate ? 0 : 1);
            UNIT_ASSERT_VALUES_EQUAL(client.Adds, failCreate ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(runner.Statistics()[static_cast<size_t>(EOperation::CreateTable)], failCreate ? 0 : 1);
            UNIT_ASSERT_VALUES_EQUAL(runner.Statistics()[static_cast<size_t>(EOperation::AddColumns)], failCreate ? 1 : 0);
            UNIT_ASSERT(client.Busy.empty());
        }
    }

    Y_UNIT_TEST(CancellationWakesIdleWorkersAndDrainsRequests) {
        auto config = Config();
        config.CreateConcurrency = 1;
        TClient client;
        std::atomic<bool> cancelled = false;
        client.BeforeCreate = [&](const TObject&) {
            cancelled = true;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        };
        TRunner runner(config, client, [&] { return cancelled.load(); });
        UNIT_ASSERT_EXCEPTION_CONTAINS(runner.Run(TDuration::Seconds(5)), yexception, "interrupted");
        UNIT_ASSERT_VALUES_EQUAL(client.CreateAttempts, 1);
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 1);
        UNIT_ASSERT_VALUES_EQUAL(client.Alters, 0);
        UNIT_ASSERT(client.Busy.empty());
    }

    Y_UNIT_TEST(SingleNewObjectWithLargeConcurrency) {
        const auto config = ParseConfig("path: load\ncreate_concurrency: 50\nalter_concurrency: 50\ntopics: {count: 1, partitions: 1}");
        TClient client;
        TRunner(config, client).Run(TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(client.MaxActive, 1);
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 1);
        UNIT_ASSERT_GT(client.Adds, 0);
        UNIT_ASSERT_GT(client.Drops, 0);
    }
}
