#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/constants.h>
#include <ydb/public/sdk/cpp/tests/common/fake_metric_registry.h>
#include <ydb/public/sdk/cpp/tests/common/fake_trace_provider.h>

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <cstdint>
#include <mutex>
#include <string_view>

using namespace NYdb;

namespace {

class TBuildInfoRecorder {
public:
    std::string GetBuildInfo() const {
        std::lock_guard lock(Mutex_);
        return BuildInfo_;
    }

protected:
    void SaveBuildInfo(grpc::ServerContext* context) {
        const auto& metadata = context->client_metadata();
        const auto it = metadata.find(YDB_SDK_BUILD_INFO_HEADER);
        ASSERT_NE(it, metadata.end());

        std::lock_guard lock(Mutex_);
        BuildInfo_.assign(it->second.data(), it->second.length());
    }

    mutable std::mutex Mutex_;
    std::string BuildInfo_;
};

class TMockDiscoveryService final
    : public Ydb::Discovery::V1::DiscoveryService::Service
    , public TBuildInfoRecorder
{
public:
    void SetPort(std::uint32_t port) {
        Port_ = port;
    }

    grpc::Status ListEndpoints(
        grpc::ServerContext* context,
        const Ydb::Discovery::ListEndpointsRequest*,
        Ydb::Discovery::ListEndpointsResponse* response) override
    {
        SaveBuildInfo(context);

        Ydb::Discovery::ListEndpointsResult result;
        auto* endpoint = result.add_endpoints();
        endpoint->set_address("127.0.0.1");
        endpoint->set_port(Port_);

        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }

private:
    std::uint32_t Port_ = 0;
};

class TMockTableService final
    : public Ydb::Table::V1::TableService::Service
    , public TBuildInfoRecorder
{
public:
    grpc::Status CreateSession(
        grpc::ServerContext* context,
        const Ydb::Table::CreateSessionRequest*,
        Ydb::Table::CreateSessionResponse* response) override
    {
        SaveBuildInfo(context);

        Ydb::Table::CreateSessionResult result;
        result.set_session_id("observability-build-info-test-session");

        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }

    grpc::Status DeleteSession(
        grpc::ServerContext*,
        const Ydb::Table::DeleteSessionRequest*,
        Ydb::Table::DeleteSessionResponse* response) override
    {
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        return grpc::Status::OK;
    }
};

struct TObservedBuildInfo {
    std::string Discovery;
    std::string Table;
};

class ObservabilityBuildInfoTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto port = Port_.GetPort();
        Endpoint_ = "localhost:" + std::to_string(port);
        DiscoveryService_.SetPort(port);

        Server_ = grpc::ServerBuilder()
            .AddListeningPort("127.0.0.1:" + std::to_string(port), grpc::InsecureServerCredentials())
            .RegisterService(&DiscoveryService_)
            .RegisterService(&TableService_)
            .BuildAndStart();
        ASSERT_NE(Server_, nullptr);
    }

    void TearDown() override {
        if (Server_) {
            Server_->Shutdown();
            Server_->Wait();
        }
    }

    TObservedBuildInfo MakeRequest(
        bool tracingEnabled,
        bool metricsEnabled,
        std::string_view buildInfoExtra = {})
    {
        TDriverConfig config;
        config
            .SetEndpoint(Endpoint_)
            .SetDatabase("/Root/observability-build-info-test");

        if (tracingEnabled) {
            config.SetTraceProvider(std::make_shared<NTests::TFakeTraceProvider>());
        }
        if (metricsEnabled) {
            config.SetMetricRegistry(std::make_shared<NTests::TFakeMetricRegistry>());
        }
        if (!buildInfoExtra.empty()) {
            config.AppendBuildInfo(buildInfoExtra);
        }

        TDriver driver(config);
        NTable::TTableClient client(driver);
        auto result = client.CreateSession().ExtractValueSync();
        EXPECT_TRUE(result.IsSuccess()) << result.GetIssues().ToString();
        return {
            .Discovery = DiscoveryService_.GetBuildInfo(),
            .Table = TableService_.GetBuildInfo(),
        };
    }

private:
    NTesting::TFreePortOwner Port_;
    TMockDiscoveryService DiscoveryService_;
    TMockTableService TableService_;
    std::unique_ptr<grpc::Server> Server_;
    std::string Endpoint_;
};

} // namespace

TEST_F(ObservabilityBuildInfoTest, ReportsConfiguredChainsOnlyToDiscovery) {
    const auto disabled = MakeRequest(false, false);
    const auto tracing = MakeRequest(true, false);
    const auto metrics = MakeRequest(false, true);
    const auto both = MakeRequest(true, true);
    const auto bothWithExtra = MakeRequest(true, true, "test-client/1.2.3");

    const auto base = "ydb-cpp-sdk/" + GetSdkSemver();

    EXPECT_EQ(disabled.Discovery, base);
    EXPECT_EQ(
        tracing.Discovery,
        base + " ydb-sdk-tracing/" + std::string(NObservability::kTracingChainVersion));
    EXPECT_EQ(
        metrics.Discovery,
        base + " ydb-sdk-metrics/" + std::string(NObservability::kMetricsChainVersion));
    EXPECT_EQ(
        both.Discovery,
        base
            + " ydb-sdk-tracing/" + std::string(NObservability::kTracingChainVersion)
            + " ydb-sdk-metrics/" + std::string(NObservability::kMetricsChainVersion));
    EXPECT_EQ(bothWithExtra.Discovery, both.Discovery + ";test-client/1.2.3");

    EXPECT_EQ(disabled.Table, base);
    EXPECT_EQ(tracing.Table, base);
    EXPECT_EQ(metrics.Table, base);
    EXPECT_EQ(both.Table, base);
    EXPECT_EQ(bothWithExtra.Table, base + ";test-client/1.2.3");
}
