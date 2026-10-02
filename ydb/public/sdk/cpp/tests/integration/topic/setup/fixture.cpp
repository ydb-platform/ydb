#include "fixture.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>


using namespace NYdb::NScheme;
using namespace NYdb::NTable;

namespace NYdb::inline Dev::NTopic::NTests {

const bool EnableDirectRead = !std::string{std::getenv("PQ_EXPERIMENTAL_DIRECT_READ") ? std::getenv("PQ_EXPERIMENTAL_DIRECT_READ") : ""}.empty();

void TTopicTestFixture::SetUp() {
    char* ydbVersion = std::getenv("YDB_VERSION");

    if (EnableDirectRead && ydbVersion != nullptr && std::string(ydbVersion) != "trunk" && std::string(ydbVersion) <= "25.1") {
        GTEST_SKIP() << "Skipping test for YDB version " << ydbVersion;
    }

    TTopicClient client(MakeDriver());

    const auto& testInfo = testing::UnitTest::GetInstance()->current_test_info();

    std::stringstream topicBuilder;
    topicBuilder << std::getenv("YDB_TEST_ROOT") << "/" << testInfo->test_suite_name() << "-" << testInfo->name() << "/";
    TopicPrefix_ = topicBuilder.str();
    
    std::stringstream consumerBuilder;
    consumerBuilder << testInfo->test_suite_name() << "-" << testInfo->name() << "-";
    ConsumerPrefix_ = consumerBuilder.str();

    RemoveDirectoryRecurive(GetDatabase() + "/" + TopicPrefix_);

    CreateTopic();
}

void TTopicTestFixture::RemoveDirectoryRecurive(const std::string& path) const {
    TSchemeClient schemeClient(MakeDriver());

    auto describeResult = schemeClient.DescribePath(path).GetValueSync();
    if (describeResult.GetStatus() == EStatus::SCHEME_ERROR) {
        return;
    }
    NStatusHelpers::ThrowOnError(describeResult);
    auto entry = describeResult.GetEntry();

    if (entry.Type == ESchemeEntryType::Table || entry.Type == ESchemeEntryType::ColumnTable) {
        TTableClient client(MakeDriver());
        NStatusHelpers::ThrowOnError(client.RetryOperationSync([&path](TSession session) {
            return session.DropTable(path).GetValueSync();
        }));
    } else if (entry.Type == ESchemeEntryType::Topic) {
        TTopicClient client(MakeDriver());
        NStatusHelpers::ThrowOnError(client.DropTopic(path).GetValueSync());
    } else if (entry.Type == ESchemeEntryType::Directory) {
        auto listResult = schemeClient.ListDirectory(path).GetValueSync();
        NStatusHelpers::ThrowOnError(listResult);
        for (const auto& entry : listResult.GetChildren()) {
            RemoveDirectoryRecurive(path + "/" + entry.Name);
        }
    } else {
        ythrow TYdbException() << "Entry type " << entry.Type << " is not supported" << Endl;
    }
}

std::string TTopicTestFixture::GetEndpoint() const {
    auto endpoint = std::getenv("YDB_ENDPOINT");
    Y_ENSURE_BT(endpoint, "YDB_ENDPOINT is not set");
    return endpoint;
}

std::string TTopicTestFixture::GetDatabase() const {
    auto database = std::getenv("YDB_DATABASE");
    Y_ENSURE_BT(database, "YDB_DATABASE is not set");
    return database;
}

void TTopicTestFixture::DropTopic(const std::string& name) {
    TTopicClient client(MakeDriver());
    auto status = client.DropTopic(GetTopicPath(name)).GetValueSync();
    Y_ENSURE_BT(status.IsSuccess(), status);
}

TDriverConfig TTopicTestFixture::MakeDriverConfig() const {
    return TDriverConfig()
        .SetEndpoint(GetEndpoint())
        .SetDatabase(GetDatabase())
        .SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG).Release()));
}

std::uint16_t TTopicTestFixture::GetPort() const {
    auto endpoint = GetEndpoint();

    auto portPos = std::string(endpoint).find(":");
    return std::stoi(std::string(endpoint).substr(portPos + 1));
}

std::vector<std::uint32_t> TTopicTestFixture::GetNodeIds() {
    NDiscovery::TDiscoveryClient client(MakeDriver());
    auto result = client.ListEndpoints().GetValueSync();
    Y_ENSURE_BT(result.IsSuccess(), static_cast<TStatus>(result));

    std::vector<std::uint32_t> nodeIds;
    for (const auto& endpoint : result.GetEndpointsInfo()) {
        nodeIds.push_back(endpoint.NodeId);
    }

    return nodeIds;
}

}
