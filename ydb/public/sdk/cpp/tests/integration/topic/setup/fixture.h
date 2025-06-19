#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/setup.h>

#include <gtest/gtest.h>


namespace NYdb::inline Dev::NTopic::NTests {

extern const bool EnableDirectRead;

class TTopicTestFixture : public ::testing::Test, public ITopicTestSetup {
public:
    void SetUp() override;

    void CreateTopic(const std::optional<std::string>& path = std::nullopt,
                     const std::optional<std::string>& consumer = std::nullopt,
                     std::size_t partitionCount = 1,
                     std::optional<std::size_t> maxPartitionCount = std::nullopt) override;

    std::string GetTopicPath() const override;
    std::string GetConsumerName() const override;

    void DropTopic(const std::string& path);

    TDriverConfig MakeDriverConfig() const override;

    TDriver MakeDriver() const;

    std::uint16_t GetPort() const override;
    std::vector<std::uint32_t> GetNodeIds() override;

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
