#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/setup.h>

#include <gtest/gtest.h>


namespace NYdb::inline Dev::NTopic::NTests {

extern const bool EnableDirectRead;

class TTopicTestFixture : public ::testing::Test, public ITopicTestSetup {
public:
    void SetUp() override;

    std::string GetEndpoint() const override;
    std::string GetDatabase() const override;

    void DropTopic(const std::string& name);

    TDriverConfig MakeDriverConfig() const override;

    std::uint16_t GetPort() const override;
    std::vector<std::uint32_t> GetNodeIds() override;

    void RemoveDirectoryRecurive(const std::string& path) const;
};

}

#ifdef PQ_EXPERIMENTAL_DIRECT_READ
    #define TEST_NAME(name) DirectRead_##name
#else
    #define TEST_NAME(name) name
#endif
