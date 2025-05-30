#pragma once

#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>

#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session.h>

namespace NYdb::NTopic::NTests {

#define TEST_CASE_NAME (this->Name_)

inline static const std::string TEST_TOPIC = "test-topic";
inline static const TString TEST_CONSUMER = "test-consumer";
inline static const TString TEST_MESSAGE_GROUP_ID = "test-message_group_id";

class TTopicSdkTestSetup {
public:
    TTopicSdkTestSetup(const TString& testCaseName, const NKikimr::Tests::TServerSettings& settings = MakeServerSettings(), bool createTopic = true);

    void CreateTopic(const std::string& path = TEST_TOPIC, const std::string& consumer = TEST_CONSUMER, size_t partitionCount = 1,
                     std::optional<size_t> maxPartitionCount = std::nullopt);
    void CreateTopicWithAutoscale(const std::string& path = TEST_TOPIC, const std::string& consumer = TEST_CONSUMER, size_t partitionCount = 1,
                     size_t maxPartitionCount = 100);

    TTopicDescription DescribeTopic(const std::string& path = TEST_TOPIC);
    TConsumerDescription DescribeConsumer(const std::string& path = TEST_TOPIC, const std::string& consumer = TEST_CONSUMER);

    void Write(const std::string& message, ui32 partitionId = 0, const std::optional<std::string> producer = std::nullopt, std::optional<ui64> seqNo = std::nullopt);

    struct ReadResult {
        std::shared_ptr<IReadSession> Reader;
        bool Timeout;

        std::vector<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent> StartPartitionSessionEvents;
    };
    ReadResult Read(const std::string& topic, const std::string& consumer,
        std::function<bool (NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent&)> handler,
        std::optional<size_t> partition = std::nullopt, const TDuration timeout = TDuration::Seconds(5));
    TStatus Commit(const std::string& path, const std::string& consumerName, size_t partitionId, size_t offset, std::optional<std::string> sessionId = std::nullopt);

    TString GetEndpoint() const;
    TString GetTopicPath(const TString& name = TString{TEST_TOPIC}) const;
    TString GetTopicParent() const;
    TString GetDatabase() const;

    ::NPersQueue::TTestServer& GetServer();
    NActors::TTestActorRuntime& GetRuntime();
    TLog& GetLog();

    TTopicClient MakeClient() const;
    NYdb::NTable::TTableClient MakeTableClient() const;

    TDriver MakeDriver() const;
    TDriver MakeDriver(const TDriverConfig& config) const;

    TDriverConfig MakeDriverConfig() const;
    static NKikimr::Tests::TServerSettings MakeServerSettings();
private:
    TString Database;
    ::NPersQueue::TTestServer Server;

    TLog Log = CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG);
};

}
