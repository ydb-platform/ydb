#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>


namespace NYdb::inline Dev::NTopic::NTests {

inline static const std::string TEST_MESSAGE_GROUP_ID = "test-message_group_id";

class ITopicTestSetup {
public:
    virtual void CreateTopic(const std::optional<std::string>& path = std::nullopt,
                             const std::optional<std::string>& consumer = std::nullopt,
                             std::size_t partitionCount = 1,
                             std::optional<std::size_t> maxPartitionCount = std::nullopt,
                             const TDuration retention = TDuration::Hours(1),
                             bool important = false) = 0;

    virtual std::string GetTopicPath() const = 0;
    virtual std::string GetConsumerName() const = 0;

    virtual TDriverConfig MakeDriverConfig() const = 0;

    virtual std::vector<std::uint32_t> GetNodeIds() = 0;
    virtual std::uint16_t GetPort() const = 0;

    virtual ~ITopicTestSetup() = default;
};

}
