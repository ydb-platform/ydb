#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <gtest/gtest.h>

namespace NYdb::inline Dev::NTopic::NTests {

extern const bool EnableDirectRead;

class TTopicTestFixture : public ::testing::Test {
public:
    void SetUp() override;

    void CreateTopic(const std::string& path, const std::string& consumer, std::size_t partitionCount = 1,
                     std::optional<std::size_t> maxPartitionCount = std::nullopt);

    void CreateTopic(const std::string& path, std::size_t partitionCount = 1,
                     std::optional<std::size_t> maxPartitionCount = std::nullopt);

    std::string GetTopicPath();
    std::string GetConsumerName();

    void DropTopic(const std::string& path);

    TDriverConfig MakeDriverConfig() const;

    TDriver MakeDriver() const;

    std::uint16_t GetPort() const;
    std::vector<std::uint32_t> GetNodeIds() const;

private:
    std::string TopicPath_;
    std::string ConsumerName_;
};

}

#ifdef PQ_EXPERIMENTAL_DIRECT_READ
  #define TEST_NAME(name) DirectRead_##name
#else
  #define TEST_NAME(name) name
#endif
