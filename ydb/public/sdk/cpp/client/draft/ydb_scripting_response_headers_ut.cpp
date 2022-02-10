
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h> 
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h> 
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h> 

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NYdb;
using namespace NYdb::NScripting;

namespace {

template<class TService>
std::unique_ptr<grpc::Server> StartGrpcServer(const TString& address, TService& service) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    return builder.BuildAndStart();
}

class TMockSlyDbProxy : public Ydb::Scripting::V1::ScriptingService::Service
{
public:
    grpc::Status ExecuteYql(
        grpc::ServerContext* context,
        const Ydb::Scripting::ExecuteYqlRequest* request,
        Ydb::Scripting::ExecuteYqlResponse* response) override {
            context->AddInitialMetadata("key", "value");
            Y_UNUSED(request);

            // Just to make sdk core happy
            auto* op = response->mutable_operation();
            op->set_ready(true);
            op->set_status(Ydb::StatusIds::SUCCESS);
            op->mutable_result();

            return grpc::Status::OK;
        }
};

}

Y_UNIT_TEST_SUITE(ResponseHeaders) {
    Y_UNIT_TEST(PassHeader) {
        TMockSlyDbProxy slyDbProxy;

        TString addr = "localhost:2135";

        auto server = StartGrpcServer(addr, slyDbProxy);

        auto config = TDriverConfig()
            .SetEndpoint(addr);
        TDriver driver(config);
        TScriptingClient client(driver);

        auto result = client.ExecuteYqlScript("SMTH").GetValueSync();
        auto metadata = result.GetResponseMetadata();

        UNIT_ASSERT(metadata.find("key") != metadata.end());
        UNIT_ASSERT_VALUES_EQUAL(metadata.find("key")->second, "value");
    }
}
