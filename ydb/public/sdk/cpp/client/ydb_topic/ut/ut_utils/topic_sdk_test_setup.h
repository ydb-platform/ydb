#pragma once

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/test_server.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/write_session.h>

namespace NYdb::NTopic::NTests {

#define TEST_CASE_NAME (this->Name_)

inline static const TString TEST_TOPIC = "test-topic";
inline static const TString TEST_CONSUMER = "test-consumer";
inline static const TString TEST_MESSAGE_GROUP_ID = "test-message_group_id";

class TTopicSdkTestSetup {
public:
    TTopicSdkTestSetup(const TString& testCaseName, const NKikimr::Tests::TServerSettings& settings = MakeServerSettings(), bool createTopic = true);

    void CreateTopic(const TString& path = TEST_TOPIC, const TString& consumer = TEST_CONSUMER, size_t partitionCount = 1,
                     std::optional<size_t> maxPartitionCount = std::nullopt);
    void CreateTopicWithAutoscale(const TString& path = TEST_TOPIC, const TString& consumer = TEST_CONSUMER, size_t partitionCount = 1,
                     size_t maxPartitionCount = 100);

    TString GetEndpoint() const;
    TString GetTopicPath(const TString& name = TEST_TOPIC) const;
    TString GetTopicParent() const;
    TString GetDatabase() const;

    ::NPersQueue::TTestServer& GetServer();
    NActors::TTestActorRuntime& GetRuntime();
    TLog& GetLog();

    TTopicClient MakeClient() const;

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
