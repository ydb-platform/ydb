#include "setup/fixture.h"
#include "utils/trace.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <library/cpp/logger/stream.h>

#include <grpc++/grpc++.h>

#include <format>
#include <thread>

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

class LocalPartition : public TTopicTestFixture {
protected:
    TDriverConfig CreateConfig(const std::string& discoveryAddr) {
        NYdb::TDriverConfig config = MakeDriverConfig();
        config.SetEndpoint(discoveryAddr);
        return config;
    }

    TWriteSessionSettings CreateWriteSessionSettings() {
        return TWriteSessionSettings()
            .Path(GetTopicPath())
            .ProducerId("test-producer")
            .PartitionId(0)
            .DirectWriteToPartition(true);
    }

    TReadSessionSettings CreateReadSessionSettings() {
        return TReadSessionSettings()
            .ConsumerName("test-consumer")
            .AppendTopics(GetTopicPath());
    }

    void WriteMessage(TTopicClient& client) {
        std::cerr << "=== Write message" << std::endl;

        auto writeSession = client.CreateSimpleBlockingWriteSession(CreateWriteSessionSettings());
        EXPECT_TRUE(writeSession->Write("message"));
        writeSession->Close();
    }

    void ReadMessage(TTopicClient& client, ui64 expectedCommitedOffset = 1) {
        std::cerr << "=== Read message" << std::endl;

        auto readSession = client.CreateReadSession(CreateReadSessionSettings());

        std::optional<TReadSessionEvent::TEvent> event = readSession->GetEvent(true);
        EXPECT_TRUE(event);
        auto startPartitionSession = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&event.value());
        EXPECT_TRUE(startPartitionSession) << DebugString(*event);

        startPartitionSession->Confirm();

        event = readSession->GetEvent(true);
        EXPECT_TRUE(event) << DebugString(*event);
        auto dataReceived = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event.value());
        EXPECT_TRUE(dataReceived) << DebugString(*event);

        dataReceived->Commit();

        auto& messages = dataReceived->GetMessages();
        EXPECT_EQ(messages.size(), 1u);
        EXPECT_EQ(messages[0].GetData(), "message");

        event = readSession->GetEvent(true);
        EXPECT_TRUE(event) << DebugString(*event);
        auto commitOffsetAck = std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&event.value());
        EXPECT_TRUE(commitOffsetAck) << DebugString(*event);
        EXPECT_EQ(commitOffsetAck->GetCommittedOffset(), expectedCommitedOffset);
    }
};

class TMockDiscoveryService: public Ydb::Discovery::V1::DiscoveryService::Service {
public:
    TMockDiscoveryService() {
        int discoveryPort = 0;

        grpc::ServerBuilder builder;
        builder.AddListeningPort("0.0.0.0:0", grpc::InsecureServerCredentials(), &discoveryPort);
        builder.RegisterService(this);
        Server = builder.BuildAndStart();

        DiscoveryAddr = "0.0.0.0:" + std::to_string(discoveryPort);
        std::cerr << "==== TMockDiscovery server started on port " << discoveryPort << std::endl;
    }

    void SetGoodEndpoints(TTopicTestFixture& fixture) {
        std::lock_guard lock(Lock);
        std::cerr << "==== TMockDiscovery set good endpoint nodes " << std::endl;

        auto nodeIds = fixture.GetNodeIds();
        SetEndpointsLocked(nodeIds[0], nodeIds.size(), fixture.GetPort());
    }

    // Call this method only after locking the Lock.
    void SetEndpointsLocked(std::uint32_t firstNodeId, std::uint32_t nodeCount, std::uint16_t port) {
        std::cerr << "==== TMockDiscovery add endpoints, firstNodeId " << firstNodeId << ", nodeCount " << nodeCount << ", port " << port << std::endl;

        MockResults.clear_endpoints();
        if (nodeCount > 0) {
            Ydb::Discovery::EndpointInfo* endpoint = MockResults.add_endpoints();
            endpoint->set_address(TStringBuilder() << "localhost");
            endpoint->set_port(port);
            endpoint->set_node_id(firstNodeId);
        }
        if (nodeCount > 1) {
            Ydb::Discovery::EndpointInfo* endpoint = MockResults.add_endpoints();
            endpoint->set_address(TStringBuilder() << "ip6-localhost"); // name should be different
            endpoint->set_port(port);
            endpoint->set_node_id(firstNodeId + 1);
        }
        if (nodeCount > 2) {
            EXPECT_TRUE(false) << "Unsupported count of nodes";
        }
    }

    void SetEndpoints(std::uint32_t firstNodeId, std::uint32_t nodeCount, std::uint16_t port) {
        std::lock_guard lock(Lock);
        SetEndpointsLocked(firstNodeId, nodeCount, port);
    }

    grpc::Status ListEndpoints(grpc::ServerContext* context, const Ydb::Discovery::ListEndpointsRequest* request, Ydb::Discovery::ListEndpointsResponse* response) override {
        std::lock_guard lock(Lock);
        EXPECT_TRUE(context);

        if (Delay != std::chrono::milliseconds::zero()) {
            std::cerr << "==== Delay " << Delay << " before ListEndpoints request" << std::endl;
            auto start = std::chrono::steady_clock::now();
            while (start + Delay < std::chrono::steady_clock::now()) {
                if (context->IsCancelled()) {
                    return grpc::Status::CANCELLED;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        std::cerr << "==== ListEndpoints request: " << request->ShortDebugString() << std::endl;

        auto* op = response->mutable_operation();
        op->set_ready(true);
        op->set_status(Ydb::StatusIds::SUCCESS);
        op->mutable_result()->PackFrom(MockResults);

        std::cerr << "==== ListEndpoints response: " << response->ShortDebugString() << std::endl;

        return grpc::Status::OK;
    }

    std::string GetDiscoveryAddr() const {
        return DiscoveryAddr;
    }

    void SetDelay(std::chrono::milliseconds delay) {
        Delay = delay;
    }

private:
    Ydb::Discovery::ListEndpointsResult MockResults;
    std::string DiscoveryAddr;
    std::unique_ptr<grpc::Server> Server;
    std::mutex Lock;

    std::chrono::milliseconds Delay = {};
};

TEST_F(LocalPartition, Basic) {
    TMockDiscoveryService discovery;
    discovery.SetGoodEndpoints(*this);
    TDriver driver(CreateConfig(discovery.GetDiscoveryAddr()));
    TTopicClient client(driver);

    WriteMessage(client);
    ReadMessage(client);
}

TEST_F(LocalPartition, DescribeBadPartition) {
    TMockDiscoveryService discovery;

    discovery.SetGoodEndpoints(*this);

    auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();

    // Set non-existing partition
    auto writeSettings = CreateWriteSessionSettings();
    writeSettings.RetryPolicy(retryPolicy);
    writeSettings.PartitionId(1);

    retryPolicy->Initialize();
    retryPolicy->ExpectBreakDown();

    std::cerr << "=== Create write session\n";
    TTopicClient client(TDriver(CreateConfig(discovery.GetDiscoveryAddr())));
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

TEST_F(LocalPartition, DiscoveryServiceBadPort) {
    TMockDiscoveryService discovery;
    discovery.SetEndpoints(9999, 2, 0);

    auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();

    auto writeSettings = CreateWriteSessionSettings();
    writeSettings.RetryPolicy(retryPolicy);

    retryPolicy->Initialize();
    retryPolicy->ExpectBreakDown();

    std::cerr << "=== Create write session\n";
    TTopicClient client(TDriver(CreateConfig(discovery.GetDiscoveryAddr())));
    auto writeSession = client.CreateWriteSession(writeSettings);

    std::cerr << "=== Wait for retries\n";
    retryPolicy->WaitForRetriesSync(3);

    discovery.SetGoodEndpoints(*this);

    std::cerr << "=== Wait for repair\n";
    retryPolicy->WaitForRepairSync();

    std::cerr << "=== Close write session\n";
    writeSession->Close();
}

TEST_F(LocalPartition, DiscoveryServiceBadNodeId) {
    TMockDiscoveryService discovery;
    discovery.SetEndpoints(9999, GetNodeIds().size(), GetPort());

    auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();

    auto writeSettings = CreateWriteSessionSettings();
    writeSettings.RetryPolicy(retryPolicy);

    retryPolicy->Initialize();
    retryPolicy->ExpectBreakDown();

    std::cerr << "=== Create write session\n";
    TTopicClient client(TDriver(CreateConfig(discovery.GetDiscoveryAddr())));
    auto writeSession = client.CreateWriteSession(writeSettings);

    std::cerr << "=== Wait for retries\n";
    retryPolicy->WaitForRetriesSync(3);

    discovery.SetGoodEndpoints(*this);

    std::cerr << "=== Wait for repair\n";
    retryPolicy->WaitForRepairSync();

    std::cerr << "=== Close write session\n";
    writeSession->Close();
}

TEST_F(LocalPartition, DescribeHang) {
    TMockDiscoveryService discovery;
    discovery.SetGoodEndpoints(*this);

    auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>(std::chrono::days(1));

    auto writeSettings = CreateWriteSessionSettings();
    writeSettings.RetryPolicy(retryPolicy);

    retryPolicy->Initialize();
    retryPolicy->ExpectBreakDown();

    std::cerr << "=== Create write session\n";
    TTopicClient client(TDriver(CreateConfig(discovery.GetDiscoveryAddr())));
    auto writeSession = client.CreateWriteSession(writeSettings);

    std::cerr << "=== Close write session\n";
    writeSession->Close();
}

TEST_F(LocalPartition, DiscoveryHang) {
    TMockDiscoveryService discovery;
    discovery.SetGoodEndpoints(*this);
    discovery.SetDelay(std::chrono::days(1));

    std::cerr << "=== Create write session\n";
    TTopicClient client(TDriver(CreateConfig(discovery.GetDiscoveryAddr())));
    auto writeSession = client.CreateWriteSession(CreateWriteSessionSettings());

    std::cerr << "=== Close write session\n";
    writeSession->Close();
}

TEST_F(LocalPartition, WithoutPartition) {
    // Direct write without partition: happy way.
    TMockDiscoveryService discovery;
    discovery.SetGoodEndpoints(*this);
    auto driverConfig = CreateConfig(discovery.GetDiscoveryAddr());
    auto* tracingBackend = new TTracingBackend();
    std::vector<std::unique_ptr<TLogBackend>> underlyingBackends;
    underlyingBackends.push_back(std::make_unique<TStreamLogBackend>(&Cerr));
    underlyingBackends.push_back(std::unique_ptr<TLogBackend>(tracingBackend));
    driverConfig.SetLog(std::make_unique<TCompositeLogBackend>(std::move(underlyingBackends)));
    TDriver driver(driverConfig);
    TTopicClient client(driver);
    auto sessionSettings = TWriteSessionSettings()
        .Path(GetTopicPath())
        .ProducerId("test-message-group")
        .MessageGroupId("test-message-group")
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

TEST_F(LocalPartition, WithoutPartitionDeadNode) {
    // This test emulates a situation, when InitResponse directs us to an inaccessible node.
    TMockDiscoveryService discovery;
    discovery.SetEndpoints(GetNodeIds()[0], 1, 0);
    auto driverConfig = CreateConfig(discovery.GetDiscoveryAddr());
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
        .MessageGroupId("test-message-group")
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
