#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>


namespace NYdb::inline Dev::NTopic::NTests {

inline static const std::string TEST_TOPIC = "test-topic";
inline static const std::string TEST_CONSUMER = "test-consumer";
inline static const std::string TEST_MESSAGE_GROUP_ID = "test-message_group_id";

class ITopicTestSetup {
public:
    virtual void CreateTopic(const std::string& name = TEST_TOPIC,
                             const std::string& consumer = TEST_CONSUMER,
                             std::size_t partitionCount = 1,
                             std::optional<std::size_t> maxPartitionCount = std::nullopt,
                             const TDuration retention = TDuration::Hours(1),
                             bool important = false);

    TTopicDescription DescribeTopic(const std::string& name = TEST_TOPIC);

    std::string GetTopicPath(const std::string& name = TEST_TOPIC) const {
        return TopicPrefix_ + name;
    }

    std::string GetConsumerName(const std::string& consumer = TEST_CONSUMER) const {
        return ConsumerPrefix_ + consumer;
    }

    TDriver MakeDriver() const {
        return TDriver(MakeDriverConfig());
    }

    virtual std::string GetEndpoint() const = 0;
    virtual std::string GetDatabase() const = 0;

    virtual TDriverConfig MakeDriverConfig() const = 0;

    virtual std::vector<std::uint32_t> GetNodeIds() = 0;
    virtual std::uint16_t GetPort() const = 0;

    virtual ~ITopicTestSetup() = default;

protected:
    std::string TopicPrefix_;
    std::string ConsumerPrefix_;
};

}
