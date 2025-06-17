#include "fixture.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h>

#include <util/system/execpath.h>

namespace NYdb::inline Dev::NTopic::NTests {

void TTopicTestFixture::SetUp() {
    TTopicClient client(MakeDriver());

    const testing::TestInfo* const testInfo = testing::UnitTest::GetInstance()->current_test_info();
    std::filesystem::path execPath(std::string{GetExecPath()});

    std::stringstream builder;
    builder << std::getenv("YDB_TEST_ROOT") << "/" << execPath.filename().string() << "/" << testInfo->test_suite_name() << "_" << testInfo->name();
    TopicPath_ = builder.str();

    client.DropTopic(TopicPath_).GetValueSync();
    CreateTopic(TopicPath_);
}

void TTopicTestFixture::TearDown() {
    DropTopic(GetTopicPath());
}

void TTopicTestFixture::CreateTopic(const std::string& path, const std::string& consumer, size_t partitionCount, std::optional<size_t> maxPartitionCount) {
    TTopicClient client(MakeDriver());

    TCreateTopicSettings topics;
    topics
        .BeginConfigurePartitioningSettings()
        .MinActivePartitions(partitionCount)
        .MaxActivePartitions(maxPartitionCount.value_or(partitionCount));

    if (maxPartitionCount.has_value() && maxPartitionCount.value() > partitionCount) {
        topics
            .BeginConfigurePartitioningSettings()
            .BeginConfigureAutoPartitioningSettings()
            .Strategy(EAutoPartitioningStrategy::ScaleUp);
    }

    TConsumerSettings<TCreateTopicSettings> consumers(topics, consumer);
    topics.AppendConsumers(consumers);

    auto status = client.CreateTopic(path, topics).GetValueSync();
    Y_ENSURE(status.IsSuccess(), status);
}

std::string TTopicTestFixture::GetTopicPath() {
    return TopicPath_;
}

void TTopicTestFixture::DropTopic(const std::string& path) {
    TTopicClient client(MakeDriver());
    auto status = client.DropTopic(path).GetValueSync();
    Y_ENSURE(status.IsSuccess(), status);
}

TDriverConfig TTopicTestFixture::MakeDriverConfig() const {
    return TDriverConfig()
        .SetEndpoint(std::getenv("YDB_ENDPOINT"))
        .SetDatabase(std::getenv("YDB_DATABASE"))
        .SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG).Release()));
}

TDriver TTopicTestFixture::MakeDriver() const {
    return TDriver(MakeDriverConfig());
}

std::uint16_t TTopicTestFixture::GetPort() const {
    auto endpoint = std::getenv("YDB_ENDPOINT");
    Y_ENSURE(endpoint, "YDB_ENDPOINT is not set");

    auto portPos = std::string(endpoint).find(":");
    return std::stoi(std::string(endpoint).substr(portPos + 1));
}

std::vector<std::uint32_t> TTopicTestFixture::GetNodeIds() const {
    NDiscovery::TDiscoveryClient client(MakeDriver());
    auto result = client.ListEndpoints().GetValueSync();
    Y_ENSURE(result.IsSuccess(), static_cast<TStatus>(result));

    std::vector<std::uint32_t> nodeIds;
    for (const auto& endpoint : result.GetEndpointsInfo()) {
        nodeIds.push_back(endpoint.NodeId);
    }

    return nodeIds;
}

}
