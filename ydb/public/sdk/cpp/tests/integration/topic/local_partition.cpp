#include "setup/fixture.h"

#include "utils/local_partition.h"
#include "utils/trace.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/logger/stream.h>

#include <format>

namespace NYdb::inline Dev::NTopic::NTests {

struct TYdbPqTestRetryState : IRetryPolicy::IRetryState {
    TYdbPqTestRetryState(std::function<void ()> retryCallback, std::function<void ()> destroyCallback, const TDuration& delay)
        : RetryDone(retryCallback)
        , DestroyDone(destroyCallback)
        , Delay(delay)
    {}

    TMaybe<TDuration> GetNextRetryDelay(NYdb::EStatus) override {
        std::cerr << "Test retry state: get retry delay" << std::endl;
        RetryDone();
        return Delay;
    }

    std::function<void ()> RetryDone;
    std::function<void ()> DestroyDone;
    TDuration Delay;

    ~TYdbPqTestRetryState() {
        DestroyDone();
    }
};

struct TYdbPqNoRetryState : IRetryPolicy::IRetryState {
    std::atomic<bool> DelayCalled = false;
    TMaybe<TDuration> GetNextRetryDelay(NYdb::EStatus) override {
        auto res = DelayCalled.exchange(true);
        EXPECT_FALSE(res);
        return {};
    }
};

struct TYdbPqTestRetryPolicy : IRetryPolicy {
    TYdbPqTestRetryPolicy(const TDuration& delay = TDuration::MilliSeconds(2000))
        : Delay(delay)
    {
        std::cerr << "====TYdbPqTestRetryPolicy()\n";
    }

    IRetryState::TPtr CreateRetryState() const override {
        std::cerr << "====CreateRetryState\n";
        if (OnFatalBreakDown.exchange(false)) {
            return std::make_unique<TYdbPqNoRetryState>();
        }
        if (Initialized_.load())
        {
            std::cerr << "====CreateRetryState Initialized\n";
            auto res = OnBreakDown.exchange(false);
            EXPECT_TRUE(res);
            for (size_t i = 0; i < 100; i++) {
                if (CurrentRetries.load() == 0) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            EXPECT_EQ(CurrentRetries.load(), 0u);
        }
        auto retryCb = [this]() mutable {this->RetryDone();};
        auto destroyCb = [this]() mutable {this->StateDestroyed();};
        return std::make_unique<TYdbPqTestRetryState>(retryCb, destroyCb, Delay);
    }

    void RetryDone() const {
        CurrentRetries.fetch_add(1);
        auto expected = RetriesExpected.load();
        if (expected > 0 && CurrentRetries.load() >= expected) {
            std::lock_guard lock(Lock);
            {
                RetryPromise.SetValue();
            }
            RetriesExpected.store(0);
        }
    }
    void StateDestroyed() const {
        std::cerr << "====StateDestroyed\n";
        auto expected = RepairExpected.exchange(false);
        if (expected) {
            std::lock_guard lock(Lock);
            RepairPromise.SetValue();
        }
    }
    void ExpectBreakDown() {
        // Either TYdbPqTestRetryPolicy() or Initialize() should be called beforehand in order to set OnBreakDown=0
        std::cerr << "====ExpectBreakDown\n";
        for (std::size_t i = 0; i < 100; i++) {
            if (!OnBreakDown.load()) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        EXPECT_FALSE(OnBreakDown.load());
        CurrentRetries.store(0);
        OnBreakDown.store(true);
    }
    void ExpectFatalBreakDown() {
        OnFatalBreakDown.store(true);
    }

    void WaitForRetries(std::uint64_t retryCount, NThreading::TPromise<void>& promise) {
        RetriesExpected.store(retryCount);
        std::lock_guard lock(Lock);
        RetryPromise = promise;
    }

    void WaitForRetriesSync(std::uint64_t retryCount) {
        NThreading::TPromise<void> retriesPromise = NThreading::NewPromise();
        auto retriesFuture = retriesPromise.GetFuture();
        WaitForRetries(retryCount, retriesPromise);
        retriesFuture.Wait();
    }

    void WaitForRepair(NThreading::TPromise<void>& promise) {
        RepairExpected.store(true);
        std::lock_guard lock(Lock);
        RepairPromise = promise;
    }

    void WaitForRepairSync() {
        NThreading::TPromise<void> repairPromise = NThreading::NewPromise();
        auto repairFuture = repairPromise.GetFuture();
        WaitForRepair(repairPromise);
        repairFuture.Wait();
    }

    void Initialize() {
        Initialized_.store(true);
        CurrentRetries.store(0);
    }

private:
    TDuration Delay;
    mutable std::atomic<std::uint64_t> CurrentRetries = 0;
    mutable std::atomic<bool> Initialized_ = false;
    mutable std::atomic<bool> OnBreakDown = false;
    mutable std::atomic<bool> OnFatalBreakDown = false;
    mutable NThreading::TPromise<void> RetryPromise;
    mutable NThreading::TPromise<void> RepairPromise;
    mutable std::atomic<std::uint64_t> RetriesExpected = 0;
    mutable std::atomic<bool> RepairExpected = false;
    mutable std::mutex Lock;
};

class LocalPartition : public TTopicTestFixture {};

TEST_F(LocalPartition, TEST_NAME(Basic)) {
    TMockDiscoveryService discovery;
    discovery.SetGoodEndpoints(*this);
    TDriver driver(CreateConfig(*this, discovery.GetDiscoveryAddr()));
    TTopicClient client(driver);

    WriteMessage(*this, client);
    ReadMessage(*this, client);
}

TEST_F(LocalPartition, TEST_NAME(DescribeBadPartition)) {
    TMockDiscoveryService discovery;

    discovery.SetGoodEndpoints(*this);

    auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();

    // Set non-existing partition
    auto writeSettings = CreateWriteSessionSettings(*this);
    writeSettings.RetryPolicy(retryPolicy);
    writeSettings.PartitionId(1);

    retryPolicy->Initialize();
    retryPolicy->ExpectBreakDown();

    std::cerr << "=== Create write session\n";
    TTopicClient client(TDriver(CreateConfig(*this, discovery.GetDiscoveryAddr())));
    auto writeSession = client.CreateWriteSession(writeSettings);

    std::cerr << "=== Wait for retries\n";
    retryPolicy->WaitForRetriesSync(3);

    std::cerr << "=== Alter partition count\n";
    TAlterTopicSettings alterSettings;
    alterSettings.AlterPartitioningSettings(2, 2);
    auto alterResult = client.AlterTopic(GetTopicPath(), alterSettings).GetValueSync();
    ASSERT_EQ(alterResult.GetStatus(), NYdb::EStatus::SUCCESS) << alterResult.GetIssues().ToString();

    std::cerr << "=== Wait for repair\n";
    retryPolicy->WaitForRepairSync();

    std::cerr << "=== Close write session\n";
    writeSession->Close();
}

TEST_F(LocalPartition, TEST_NAME(DiscoveryServiceBadPort)) {
    TMockDiscoveryService discovery;
    discovery.SetEndpoints(9999, 2, 0);

    auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();

    auto writeSettings = CreateWriteSessionSettings(*this);
    writeSettings.RetryPolicy(retryPolicy);

    retryPolicy->Initialize();
    retryPolicy->ExpectBreakDown();

    std::cerr << "=== Create write session\n";
    TTopicClient client(TDriver(CreateConfig(*this, discovery.GetDiscoveryAddr())));
    auto writeSession = client.CreateWriteSession(writeSettings);

    std::cerr << "=== Wait for retries\n";
    retryPolicy->WaitForRetriesSync(3);

    discovery.SetGoodEndpoints(*this);

    std::cerr << "=== Wait for repair\n";
    retryPolicy->WaitForRepairSync();

    std::cerr << "=== Close write session\n";
    writeSession->Close();
}

TEST_F(LocalPartition, TEST_NAME(DiscoveryServiceBadNodeId)) {
    TMockDiscoveryService discovery;
    discovery.SetEndpoints(9999, GetNodeIds().size(), GetPort());

    auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();

    auto writeSettings = CreateWriteSessionSettings(*this);
    writeSettings.RetryPolicy(retryPolicy);

    retryPolicy->Initialize();
    retryPolicy->ExpectBreakDown();

    std::cerr << "=== Create write session\n";
    TTopicClient client(TDriver(CreateConfig(*this, discovery.GetDiscoveryAddr())));
    auto writeSession = client.CreateWriteSession(writeSettings);

    std::cerr << "=== Wait for retries\n";
    retryPolicy->WaitForRetriesSync(3);

    discovery.SetGoodEndpoints(*this);

    std::cerr << "=== Wait for repair\n";
    retryPolicy->WaitForRepairSync();

    std::cerr << "=== Close write session\n";
    writeSession->Close();
}

TEST_F(LocalPartition, TEST_NAME(DescribeHang)) {
    TMockDiscoveryService discovery;
    discovery.SetGoodEndpoints(*this);

    auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>(std::chrono::days(1));

    auto writeSettings = CreateWriteSessionSettings(*this);
    writeSettings.RetryPolicy(retryPolicy);

    retryPolicy->Initialize();
    retryPolicy->ExpectBreakDown();

    std::cerr << "=== Create write session\n";
    TTopicClient client(TDriver(CreateConfig(*this, discovery.GetDiscoveryAddr())));
    auto writeSession = client.CreateWriteSession(writeSettings);

    std::cerr << "=== Close write session\n";
    writeSession->Close();
}

TEST_F(LocalPartition, TEST_NAME(DiscoveryHang)) {
    TMockDiscoveryService discovery;
    discovery.SetGoodEndpoints(*this);
    discovery.SetDelay(std::chrono::days(1));

    std::cerr << "=== Create write session\n";
    TTopicClient client(TDriver(CreateConfig(*this, discovery.GetDiscoveryAddr())));
    auto writeSession = client.CreateWriteSession(CreateWriteSessionSettings(*this));

    std::cerr << "=== Close write session\n";
    writeSession->Close();
}

TEST_F(LocalPartition, TEST_NAME(WithoutPartition)) {
    // Direct write without partition: happy way.
    TMockDiscoveryService discovery;
    discovery.SetGoodEndpoints(*this);
    auto driverConfig = CreateConfig(*this, discovery.GetDiscoveryAddr());
    auto* tracingBackend = new TTracingBackend();
    std::vector<std::unique_ptr<TLogBackend>> underlyingBackends;
    underlyingBackends.push_back(std::make_unique<TStreamLogBackend>(&Cerr));
    underlyingBackends.push_back(std::unique_ptr<TLogBackend>(tracingBackend));
    driverConfig.SetLog(std::make_unique<TCompositeLogBackend>(std::move(underlyingBackends)));
    TDriver driver(driverConfig);
    TTopicClient client(driver);
    auto sessionSettings = TWriteSessionSettings()
        .Path(GetTopicPath())
        .ProducerId(TEST_MESSAGE_GROUP_ID)
        .MessageGroupId(TEST_MESSAGE_GROUP_ID)
        .DirectWriteToPartition(true);
    auto writeSession = client.CreateSimpleBlockingWriteSession(sessionSettings);
    ASSERT_TRUE(writeSession->Write("message"));
    writeSession->Close();

    auto node0Id = std::to_string(GetNodeIds()[0]);
    TExpectedTrace expected{
        "InitRequest !partition_id !partition_with_generation",
        "InitResponse partition_id=0 session_id",
        "DescribePartitionRequest partition_id=0",
        std::format("DescribePartitionResponse partition_id=0 pl_generation=1 pl_node_id={}", node0Id),
        std::format("PreferredPartitionLocation Generation=1 NodeId={}", node0Id),
        "InitRequest !partition_id pwg_partition_id=0 pwg_generation=1",
        "InitResponse partition_id=0 session_id",
    };
    auto const events = tracingBackend->GetEvents();
    std::cerr << "==== Events count: " << events.size() << std::endl;
    for (auto const& event : events) {
        std::cerr << "==== Event: " << event.Event << std::endl;
        for (auto const& [key, value] : event.KeyValues) {
            std::cerr << "==== " << key << "=" << value << std::endl;
        }
    }
    ASSERT_TRUE(expected.Matches(events));
}

TEST_F(LocalPartition, TEST_NAME(WithoutPartitionDeadNode)) {
    // This test emulates a situation, when InitResponse directs us to an inaccessible node.
    TMockDiscoveryService discovery;
    discovery.SetEndpoints(GetNodeIds()[0], 1, 0);
    auto driverConfig = CreateConfig(*this, discovery.GetDiscoveryAddr());
    auto* tracingBackend = new TTracingBackend();
    std::vector<std::unique_ptr<TLogBackend>> underlyingBackends;
    underlyingBackends.push_back(std::make_unique<TStreamLogBackend>(&Cerr));
    underlyingBackends.push_back(std::unique_ptr<TLogBackend>(tracingBackend));
    driverConfig.SetLog(std::make_unique<TCompositeLogBackend>(std::move(underlyingBackends)));
    TDriver driver(driverConfig);
    TTopicClient client(driver);
    auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();
    auto sessionSettings = TWriteSessionSettings()
        .Path(GetTopicPath())
        .MessageGroupId(TEST_MESSAGE_GROUP_ID)
        .DirectWriteToPartition(true)
        .PartitionId(0)
        .RetryPolicy(retryPolicy);
    retryPolicy->Initialize();
    retryPolicy->ExpectBreakDown();
    auto writeSession = client.CreateSimpleBlockingWriteSession(sessionSettings);

    retryPolicy->WaitForRetriesSync(1);
    discovery.SetGoodEndpoints(*this);
    retryPolicy->WaitForRepairSync();
    ASSERT_TRUE(writeSession->Close());

    auto node0Id = std::to_string(GetNodeIds()[0]);
    TExpectedTrace expected{
        "DescribePartitionRequest partition_id=0",
        "Error status=TRANSPORT_UNAVAILABLE",
        "DescribePartitionRequest partition_id=0",
        std::format("DescribePartitionResponse partition_id=0 pl_generation=1 pl_node_id={}", node0Id),
        std::format("PreferredPartitionLocation Generation=1 NodeId={}", node0Id),
        "InitRequest !partition_id pwg_partition_id=0 pwg_generation=1",
        "InitResponse partition_id=0",
    };
    auto const events = tracingBackend->GetEvents();
    ASSERT_TRUE(expected.Matches(events));
}

}
