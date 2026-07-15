#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/constants.h>
#include <ydb/public/sdk/cpp/tests/common/fake_metric_registry.h>
#include <ydb/public/sdk/cpp/tests/common/fake_trace_provider.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <mutex>
#include <string_view>

using namespace NYdb;

namespace {

class TMockTableService final : public Ydb::Table::V1::TableService::Service {
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

    std::string GetBuildInfo() const {
        std::lock_guard lock(Mutex_);
        return BuildInfo_;
    }

private:
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

class ObservabilityBuildInfoTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto port = Port_.GetPort();
        Endpoint_ = "localhost:" + std::to_string(port);

        Server_ = grpc::ServerBuilder()
            .AddListeningPort("127.0.0.1:" + std::to_string(port), grpc::InsecureServerCredentials())
            .RegisterService(&Service_)
            .BuildAndStart();
        ASSERT_NE(Server_, nullptr);
    }

    void TearDown() override {
        if (Server_) {
            Server_->Shutdown();
            Server_->Wait();
        }
    }

    std::string MakeRequest(
        bool tracingEnabled,
        bool metricsEnabled,
        std::string_view buildInfoExtra = {})
    {
        TDriverConfig config;
        config
            .SetEndpoint(Endpoint_)
            .SetDatabase("/Root/observability-build-info-test")
            .SetDiscoveryMode(EDiscoveryMode::Off);

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
        return Service_.GetBuildInfo();
    }

private:
    NTesting::TFreePortOwner Port_;
    TMockTableService Service_;
    std::unique_ptr<grpc::Server> Server_;
    std::string Endpoint_;
};

} // namespace

TEST_F(ObservabilityBuildInfoTest, ReportsOnlyConfiguredChains) {
    const auto disabled = MakeRequest(false, false);
    const auto tracing = MakeRequest(true, false);
    const auto metrics = MakeRequest(false, true);
    const auto both = MakeRequest(true, true);
    const auto bothWithExtra = MakeRequest(true, true, "test-client/1.2.3");

    EXPECT_EQ(disabled, "ydb-cpp-sdk/" + GetSdkSemver());
    EXPECT_EQ(
        tracing,
        disabled + " ydb-sdk-tracing/" + std::string(NObservability::kTracingChainVersion));
    EXPECT_EQ(
        metrics,
        disabled + " ydb-sdk-metrics/" + std::string(NObservability::kMetricsChainVersion));
    EXPECT_EQ(
        both,
        disabled
            + " ydb-sdk-tracing/" + std::string(NObservability::kTracingChainVersion)
            + " ydb-sdk-metrics/" + std::string(NObservability::kMetricsChainVersion));
    EXPECT_EQ(bothWithExtra, both + ";test-client/1.2.3");
}
