#include <ydb/library/grpc/actor_client/grpc_service_client.h>
#include <ydb/public/api/grpc/draft/dummy.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/generic/strbuf.h>
#include <util/system/mutex.h>

#include <grpcpp/server_builder.h>

namespace {

class TUserAgentService : public Draft::Dummy::DummyService::Service {
public:
    grpc::Status Ping(
        grpc::ServerContext* context,
        const Draft::Dummy::PingRequest* request,
        Draft::Dummy::PingResponse* response) override
    {
        const auto [begin, end] = context->client_metadata().equal_range("user-agent");
        with_lock (Mutex) {
            UserAgent.clear();
            for (auto it = begin; it != end; ++it) {
                UserAgent.assign(it->second.cbegin(), it->second.cend());
            }
        }
        if (request->copy()) {
            response->set_payload(request->payload());
        }
        return grpc::Status::OK;
    }

    TString GetUserAgent() {
        with_lock (Mutex) {
            return UserAgent;
        }
    }

private:
    TMutex Mutex;
    TString UserAgent;
};

class TGrpcServiceClientUserAgentFixture : public NUnitTest::TBaseFixture {
public:
    using TClient = NGrpcActorClient::TGrpcServiceClient<Draft::Dummy::DummyService>;

    TGrpcServiceClientUserAgentFixture()
        : Endpoint(TStringBuilder() << "localhost:" << PortManager.GetPort())
    {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(Endpoint, grpc::InsecureServerCredentials());
        builder.RegisterService(&Service);
        Server = builder.BuildAndStart();
        UNIT_ASSERT(Server);
    }

    TString Call(const TString& userAgentHint) {
        NGrpcActorClient::TGrpcClientSettings settings(userAgentHint);
        settings.Endpoint = Endpoint;
        const auto config = TClient::InitGrpcConfig(settings);
        auto channel = NYdbGrpc::CreateChannelInterface(config);
        auto stub = Draft::Dummy::DummyService::NewStub(channel);

        grpc::ClientContext context;
        Draft::Dummy::PingRequest request;
        Draft::Dummy::PingResponse response;
        request.set_copy(true);
        request.set_payload("payload");

        const auto status = stub->Ping(&context, request, &response);
        UNIT_ASSERT_C(status.ok(), status.error_message());
        UNIT_ASSERT_VALUES_EQUAL(response.payload(), request.payload());
        return Service.GetUserAgent();
    }

    void AssertStandardUserAgentIsPreserved(const TString& userAgent) {
        UNIT_ASSERT_STRING_CONTAINS(userAgent, "grpc-c++/");
    }

private:
    TPortManager PortManager;
    const TString Endpoint;
    TUserAgentService Service;
    std::unique_ptr<grpc::Server> Server;
};

} // namespace

Y_UNIT_TEST_SUITE_F(GrpcServiceClientUserAgentTests, TGrpcServiceClientUserAgentFixture) {
    Y_UNIT_TEST(WritesUserAgentWithoutHintAndPreservesGrpcInfo) {
        const TString userAgent = Call("");

        UNIT_ASSERT_C(TStringBuf(userAgent).StartsWith("ydb/"), userAgent);
        AssertStandardUserAgentIsPreserved(userAgent);
    }

    Y_UNIT_TEST(WritesUserAgentWithHintAndPreservesGrpcInfo) {
        const TString userAgent = Call("grpc_service_client_ut");

        UNIT_ASSERT_C(TStringBuf(userAgent).StartsWith("ydb-grpc_service_client_ut/"), userAgent);
        AssertStandardUserAgentIsPreserved(userAgent);
    }
}
