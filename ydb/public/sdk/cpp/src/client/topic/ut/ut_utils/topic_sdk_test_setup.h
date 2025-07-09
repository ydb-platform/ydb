#pragma once

#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>

#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session.h>

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/setup.h>

namespace NYdb::inline Dev::NTopic::NTests {

#define TEST_CASE_NAME (this->Name_)

class TTopicSdkTestSetup : public ITopicTestSetup {
public:
    explicit TTopicSdkTestSetup(const std::string& testCaseName, const NKikimr::Tests::TServerSettings& settings = MakeServerSettings(), bool createTopic = true);

    void CreateTopic(const std::string& name = TEST_TOPIC,
                     const std::string& consumer = TEST_CONSUMER,
                     std::size_t partitionCount = 1,
                     std::optional<std::size_t> maxPartitionCount = std::nullopt,
                     const TDuration retention = TDuration::Hours(1),
                     bool important = false) override;
    void CreateTopicWithAutoscale(const std::string& name = TEST_TOPIC,
                                  const std::string& consumer = TEST_CONSUMER,
                                  std::size_t partitionCount = 1,
                                  std::size_t maxPartitionCount = 100);

    TConsumerDescription DescribeConsumer(const std::string& name = TEST_TOPIC,
                                          const std::string& consumer = TEST_CONSUMER);

    void Write(const std::string& message, std::uint32_t partitionId = 0,
               const std::optional<std::string> producer = std::nullopt,
               std::optional<std::uint64_t> seqNo = std::nullopt);

    struct TReadResult {
        std::shared_ptr<IReadSession> Reader;
        bool Timeout;

        std::vector<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent> StartPartitionSessionEvents;
    };

    TReadResult Read(const std::string& topic, const std::string& consumer,
                     std::function<bool (NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent&)> handler,
                     std::optional<std::size_t> partition = std::nullopt,
                     const TDuration timeout = TDuration::Seconds(5));

    TStatus Commit(const std::string& path, const std::string& consumerName,
                   std::size_t partitionId, std::size_t offset,
                   std::optional<std::string> sessionId = std::nullopt);

    std::string GetEndpoint() const override;
    std::string GetDatabase() const override;

    std::string GetFullTopicPath() const;

    std::vector<std::uint32_t> GetNodeIds() override;
    std::uint16_t GetPort() const override;

    TDriverConfig MakeDriverConfig() const override;

    ::NPersQueue::TTestServer& GetServer();
    NActors::TTestActorRuntime& GetRuntime();
    TLog& GetLog();

    TTopicClient MakeClient() const;
    NYdb::NTable::TTableClient MakeTableClient() const;

    static NKikimr::Tests::TServerSettings MakeServerSettings();

private:
    std::string Database_;

    ::NPersQueue::TTestServer Server_;

    TLog Log_ = CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG);
};

}
