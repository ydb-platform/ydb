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
    void MakeDirectory(const TString&) override { }

    void Create(const TObject& object) override {
        Enter(object);
        Y_DEFER { Leave(object); };
        std::lock_guard guard(Mutex);
        Y_ENSURE(!Present.contains(object.Path), "Duplicate create");
        Present[object.Path] = false;
        ++Creates;
    }

    bool Inspect(const TObject& object) override {
        std::lock_guard guard(Mutex);
        ++Inspects;
        Y_ENSURE(Present.contains(object.Path), "Missing object");
        return Present.at(object.Path);
    }

    void Alter(const TObject& object, bool add) override {
        Enter(object);
        Y_DEFER { Leave(object); };
        std::lock_guard guard(Mutex);
        ++Alters;
        Y_ENSURE(!FailAlter, "Uncertain ALTER outcome");
        Y_ENSURE(Present.at(object.Path) != add, "Wrong ALTER direction");
        Present[object.Path] = add;
        if (add) {
            ++Adds;
        } else {
            ++Drops;
        }
        BatchSizes.insert(object.AlterBatchSize);
    }

    void Enter(const TObject& object) {
        std::unique_lock lock(Mutex);
        Y_ENSURE(!Busy.contains(object.Path), "Concurrent operations on one object");
        Busy.insert(object.Path);
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

    void Leave(const TObject& object) {
        std::lock_guard guard(Mutex);
        Busy.erase(object.Path);
    }

    std::mutex Mutex;
    std::condition_variable Condition;
    THashMap<TString, bool> Present;
    THashSet<TString> Busy;
    THashSet<ui32> BatchSizes;
    size_t GateSize = 0;
    bool GateOpen = false;
    size_t MaxActive = 0;
    ui64 Creates = 0;
    ui64 Inspects = 0;
    ui64 Alters = 0;
    ui64 Adds = 0;
    ui64 Drops = 0;
    bool FailAlter = false;
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

    Y_UNIT_TEST(ValidatesAllObjectsBeforeAltering) {
        const auto config = Config();
        TClient client;
        client.Present[config.Object(0).Path] = false;
        TRunner runner(config, client);
        UNIT_ASSERT_EXCEPTION_CONTAINS(runner.Run(TDuration::Seconds(1)), yexception, "Missing object");
        UNIT_ASSERT_VALUES_EQUAL(client.Alters, 0);
    }

    Y_UNIT_TEST(InterruptBeforeStarting) {
        const auto config = Config();
        TClient client;
        TRunner runner(config, client, [] { return true; });
        UNIT_ASSERT_EXCEPTION(runner.Init(), yexception);
        UNIT_ASSERT_EXCEPTION(runner.Run(TDuration::Seconds(1)), yexception);
        UNIT_ASSERT_VALUES_EQUAL(client.Creates, 0);
        UNIT_ASSERT_VALUES_EQUAL(client.Alters, 0);
    }
}
