#pragma once

#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>

#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session.h>

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/setup.h>

namespace NYdb::inline Dev::NTopic::NTests {

#define TEST_CASE_NAME (this->Name_)


inline static const std::string TEST_TOPIC = "test-topic";
inline static const std::string TEST_CONSUMER = "test-consumer";

class TTopicSdkTestSetup : public ITopicTestSetup {
public:
    TTopicSdkTestSetup(const std::string& testCaseName, const NKikimr::Tests::TServerSettings& settings = MakeServerSettings(), bool createTopic = true);

    void CreateTopic(const std::optional<std::string>& path = std::nullopt,
                     const std::optional<std::string>& consumer = std::nullopt,
                     std::size_t partitionCount = 1,
                     std::optional<std::size_t> maxPartitionCount = std::nullopt,
                     const TDuration retention = TDuration::Hours(1),
                     bool important = false) override;

    void CreateTopicWithAutoscale(const std::optional<std::string>& path = std::nullopt,
                                  const std::optional<std::string>& consumer = std::nullopt,
                                  std::size_t partitionCount = 1,
                                  std::optional<std::size_t> maxPartitionCount = std::nullopt);

    TTopicDescription DescribeTopic(const std::optional<std::string>& path = std::nullopt);
    TConsumerDescription DescribeConsumer(const std::optional<std::string>& path = std::nullopt,
                                          const std::optional<std::string>& consumer = std::nullopt);

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

    std::string GetEndpoint() const;
    std::string GetTopicPath() const override;
    std::string GetConsumerName() const override;
    std::string GetTopicParent() const;
    std::string GetDatabase() const;

    std::vector<std::uint32_t> GetNodeIds() override;
    std::uint16_t GetPort() const override;

    ::NPersQueue::TTestServer& GetServer();
    NActors::TTestActorRuntime& GetRuntime();
    TLog& GetLog();

    TTopicClient MakeClient() const;
    NYdb::NTable::TTableClient MakeTableClient() const;

    TDriver MakeDriver() const;
    TDriver MakeDriver(const TDriverConfig& config) const;

    TDriverConfig MakeDriverConfig() const override;
    static NKikimr::Tests::TServerSettings MakeServerSettings();

private:
    std::string Database_;
    std::string TopicPath_;
    std::string ConsumerName_;

    ::NPersQueue::TTestServer Server_;

    TLog Log_ = CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG);
};

}
