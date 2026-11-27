#include <ydb/core/grpc_streaming/ut/grpc/streaming_service.grpc.pb.h>
#include <ydb/library/grpc/server/grpc_server.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <chrono>

namespace NYdbGrpc {
namespace {

using namespace NKikimr::NGRpcServer::NStreamingTest;

auto Deadline() {
    return std::chrono::system_clock::now() + std::chrono::seconds(10);
}

class TEchoService : public TStreamingService::Service {
    grpc::Status Session(grpc::ServerContext*, grpc::ServerReaderWriter<TSessionResponse, TSessionRequest>* stream) override {
        TSessionRequest request;
        while (stream->Read(&request)) {
            TSessionResponse response;
            response.SetResponseCookie(request.GetRequestCookie());
            if (!stream->Write(response)) {
                break;
            }
        }
        return grpc::Status::OK;
    }
};

struct TServerFixture : NUnitTest::TBaseFixture {
    NTesting::TPortHolder Port = NTesting::GetFreePort();
    TEchoService Echo;
    TGRpcServer Server{TServerOptions().SetHost("[::1]").SetPort(Port).SetServerBuilderMutator(
        [this](grpc::ServerBuilder& builder) { builder.RegisterService(&Echo); })};

    void SetUp(NUnitTest::TTestContext&) override {
        Server.Start();
    }

    void TearDown(NUnitTest::TTestContext&) override {
        Server.Stop();
    }

    std::shared_ptr<grpc::Channel> Connect() {
        grpc::ChannelArguments args;
        args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1); // Each client must own a separate transport.
        auto channel = grpc::CreateCustomChannel("[::1]:" + ToString(Port), grpc::InsecureChannelCredentials(), args);
        UNIT_ASSERT(channel->WaitForConnected(Deadline()));
        return channel;
    }
};

Y_UNIT_TEST_SUITE(DisconnectClients) {
    Y_UNIT_TEST_F(IdleConnectionsCanReconnect, TServerFixture) {
        auto first = Connect();
        auto second = Connect();

        Server.DisconnectClients();

        for (auto& channel : {first, second}) {
            UNIT_ASSERT(channel->WaitForStateChange(GRPC_CHANNEL_READY, Deadline()));
            UNIT_ASSERT(channel->WaitForConnected(Deadline()));
        }
    }

    Y_UNIT_TEST_F(ActiveStreamIsInterrupted, TServerFixture) {
        auto channel = Connect();
        auto stub = TStreamingService::NewStub(channel);
        grpc::ClientContext context;
        context.set_deadline(Deadline());
        auto stream = stub->Session(&context);
        TSessionRequest request;
        request.SetRequestCookie(42);
        TSessionResponse response;
        UNIT_ASSERT(stream->Write(request));
        UNIT_ASSERT(stream->Read(&response));
        UNIT_ASSERT_VALUES_EQUAL(response.GetResponseCookie(), 42);

        Server.DisconnectClients();

        UNIT_ASSERT(!stream->Read(&response));
        const auto status = stream->Finish();
        UNIT_ASSERT_C(status.error_code() == grpc::StatusCode::UNAVAILABLE, status.error_message());

        grpc::ClientContext nextContext;
        nextContext.set_deadline(Deadline());
        nextContext.set_wait_for_ready(true);
        auto next = stub->Session(&nextContext);
        UNIT_ASSERT(next->Write(request));
        UNIT_ASSERT(next->WritesDone());
        UNIT_ASSERT(next->Read(&response));
        UNIT_ASSERT_VALUES_EQUAL(response.GetResponseCookie(), 42);
        UNIT_ASSERT(next->Finish().ok());
    }
}

} // namespace
} // namespace NYdbGrpc
