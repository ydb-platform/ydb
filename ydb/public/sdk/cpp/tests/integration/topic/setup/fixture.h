#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <gtest/gtest.h>

namespace NYdb::inline Dev::NTopic::NTests {

class TTopicTestFixture : public ::testing::Test {
public:
    void SetUp() override;  
    void TearDown() override;

    void CreateTopic(const std::string& path, const std::string& consumer = "test-consumer", std::size_t partitionCount = 1,
                     std::optional<std::size_t> maxPartitionCount = std::nullopt);

    std::string GetTopicPath();

    void DropTopic(const std::string& path);

    TDriverConfig MakeDriverConfig() const;

    TDriver MakeDriver() const;

    std::uint16_t GetPort() const;
    std::vector<std::uint32_t> GetNodeIds() const;
private:
    std::string TopicPath_;
};

}
