#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>

#include <library/cpp/testing/common/network.h>

#include <util/string/builder.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <gtest/gtest.h>

using namespace NYdb;

namespace {

class TMockQueryService : public Ydb::Query::V1::QueryService::Service {
public:
    std::atomic<int> ExecuteScriptCallCount{0};
    int ExecuteScriptFailCount = 0;

    grpc::Status ExecuteScript(
        grpc::ServerContext* /* context */,
        const Ydb::Query::ExecuteScriptRequest* /* request */,
        Ydb::Operations::Operation* response) override
    {
        ExecuteScriptCallCount.fetch_add(1);

        response->set_ready(true);
        if (ExecuteScriptCallCount.load() <= ExecuteScriptFailCount) {
            response->set_status(Ydb::StatusIds::UNAVAILABLE);
            return grpc::Status::OK;
        }

        response->set_status(Ydb::StatusIds::SUCCESS);
        response->set_id("ydb://operation/1");
        return grpc::Status::OK;
    }
};

template<class TService>
std::unique_ptr<grpc::Server> StartGrpcServer(const std::string& address, TService& service) {
    return grpc::ServerBuilder()
        .AddListeningPort(TString{address}, grpc::InsecureServerCredentials())
        .RegisterService(&service)
        .BuildAndStart();
}

} // namespace

TEST(QueryTest, ExecuteScriptRetriesOnUnavailable) {
    TMockQueryService queryService;
    queryService.ExecuteScriptFailCount = 1;

    NTesting::InitPortManagerFromEnv();
    const auto portHolder = NTesting::GetFreePort();
    const ui16 port = static_cast<ui16>(portHolder);

    auto grpcServer = StartGrpcServer(
        TStringBuilder() << "127.0.0.1:" << port,
        queryService);

    TDriver driver(
        TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << port)
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/My/DB"));

    NQuery::TQueryClient client(driver);

    auto retrySettings = NRetry::TRetryOperationSettings()
        .MaxRetries(3)
        .FastBackoffSettings(NRetry::TBackoffSettings()
            .SlotDuration(TDuration::MilliSeconds(1))
            .Ceiling(1));

    auto future = client.ExecuteScript(
        "SELECT 1",
        NQuery::TExecuteScriptSettings(),
        retrySettings);

    ASSERT_TRUE(future.Wait(TDuration::Seconds(10)));
    auto result = future.ExtractValueSync();
    ASSERT_TRUE(result.Status().IsSuccess());
    EXPECT_GE(queryService.ExecuteScriptCallCount.load(), 2);
}
