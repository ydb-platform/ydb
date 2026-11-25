#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/config/config.h>

#include <ydb/public/api/grpc/ydb_config_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace NYdb::inline Dev::NConfig {
namespace {

class TMockConfigService final : public Ydb::Config::V1::ConfigService::Service {
public:
    grpc::Status FetchConfig(grpc::ServerContext*,
                             const Ydb::Config::FetchConfigRequest* request,
                             Ydb::Config::FetchConfigResponse* response) override {
        {
            std::lock_guard lock(Mutex_);
            Request_ = request->SerializeAsString();
        }

        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        Ydb::Config::FetchConfigResult result;
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }

    Ydb::Config::FetchConfigRequest GetRequest() {
        std::lock_guard lock(Mutex_);
        Ydb::Config::FetchConfigRequest request;
        UNIT_ASSERT(request.ParseFromString(Request_));
        return request;
    }

private:
    std::mutex Mutex_;
    std::string Request_;
};

template <typename TCallback>
void WithClient(TCallback callback) {
    int port = 0;
    TMockConfigService service;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    UNIT_ASSERT(server);

    TDriver driver(TDriverConfig().SetEndpoint("127.0.0.1:" + std::to_string(port)).SetDiscoveryMode(EDiscoveryMode::Off));
    Y_DEFER {
        driver.Stop(true);
    };
    TConfigClient client(driver);
    callback(client, service);
}

} // namespace

Y_UNIT_TEST_SUITE(ConfigFetchRequest) {
    Y_UNIT_TEST(SeedNodeMarkerDistinguishesOverloads) {
        struct TTestCase {
            const char* Name;
            bool SeedNode;
            std::vector<std::string> Hosts;
            std::int32_t Port;
        };
        const std::vector<TTestCase> testCases = {
            {"ordinary fetch", false, {}, 0},
            {"static seed-node fetch", true, {"node.example.org", "node-alt.example.org"}, 19001},
            {"dynamic seed-node fetch", true, {}, 0},
        };

        WithClient([&](TConfigClient& client, TMockConfigService& service) {
            for (const auto& testCase : testCases) {
                auto future = testCase.SeedNode
                              ? client.FetchAllConfigs(testCase.Hosts, testCase.Port)
                              : client.FetchAllConfigs();
                UNIT_ASSERT_C(future.Wait(TDuration::Seconds(10)), testCase.Name);
                UNIT_ASSERT_C(future.ExtractValueSync().IsSuccess(), testCase.Name);

                const auto request = service.GetRequest();
                UNIT_ASSERT_C(request.has_all(), testCase.Name);
                UNIT_ASSERT_C(request.all().has_none(), testCase.Name);
                UNIT_ASSERT_VALUES_EQUAL_C(request.has_seed_node_fetch_requestor(), testCase.SeedNode, testCase.Name);

                const auto& requestor = request.seed_node_fetch_requestor();
                UNIT_ASSERT_VALUES_EQUAL_C(requestor.host_size(), testCase.Hosts.size(), testCase.Name);
                for (int i = 0; i < requestor.host_size(); ++i) {
                    UNIT_ASSERT_VALUES_EQUAL_C(requestor.host(i), testCase.Hosts[i], testCase.Name);
                }
                UNIT_ASSERT_VALUES_EQUAL_C(requestor.port(), testCase.Port, testCase.Name);
            }
        });
    }
}

} // namespace NYdb::NConfig
