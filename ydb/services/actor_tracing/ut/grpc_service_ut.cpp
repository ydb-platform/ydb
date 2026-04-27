#include <ydb/services/actor_tracing/grpc_service.h>
#include <ydb/core/actor_tracing/tracing_service.h>
#include <ydb/core/actor_tracing/tracing_events.h>
#include <ydb/library/actors/trace_data/trace_data.h>

#include <ydb/public/api/grpc/ydb_actor_tracing_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_actor_tracing.pb.h>

#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/create_channel.h>

using namespace NKikimr;
using namespace Tests;

namespace {

class TActorTracingTestServer {
public:
    TActorTracingTestServer() {
        ui16 port = PortManager.GetPort(2134);
        ui16 grpc = PortManager.GetPort(2135);

        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBuiltinDomain(true);

        ServerSettings = new TServerSettings(port, authConfig);
        ServerSettings->SetGrpcPort(grpc);
        ServerSettings->SetDomainName("Root");
        ServerSettings->AddStoragePool("ssd");

        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TActorTracingGRpcService>("actor_tracing");

        Server.Reset(new TServer(*ServerSettings));

        auto* runtime = Server->GetRuntime();
        runtime->RegisterService(
            NActorTracing::MakeActorTracingServiceId(runtime->GetNodeId(0)),
            runtime->Register(NActorTracing::CreateActorTracingService(), 0));

        Server->EnableGRpc(grpc);

        TClient annoyingClient(*ServerSettings);
        annoyingClient.InitRootScheme("Root");

        GRpcPort = grpc;
    }

    std::shared_ptr<grpc::Channel> GetChannel() {
        return grpc::CreateChannel(
            "localhost:" + ToString(GRpcPort),
            grpc::InsecureChannelCredentials());
    }

private:
    TPortManager PortManager;
    TServerSettings::TPtr ServerSettings;
    THolder<TServer> Server;
    ui16 GRpcPort;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(ActorTracingGRpcService) {

    Y_UNIT_TEST(TraceStartStop) {
        TActorTracingTestServer server;
        auto stub = Ydb::ActorTracing::V1::ActorTracingService::NewStub(server.GetChannel());

        {
            Ydb::ActorTracing::TraceStartRequest request;
            Ydb::ActorTracing::TraceStartResponse response;
            grpc::ClientContext ctx;
            auto status = stub->TraceStart(&ctx, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_message());
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }

        {
            Ydb::ActorTracing::TraceStopRequest request;
            Ydb::ActorTracing::TraceStopResponse response;
            grpc::ClientContext ctx;
            auto status = stub->TraceStop(&ctx, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_message());
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(TraceFetchReturnsData) {
        TActorTracingTestServer server;
        auto stub = Ydb::ActorTracing::V1::ActorTracingService::NewStub(server.GetChannel());

        {
            Ydb::ActorTracing::TraceStartRequest request;
            Ydb::ActorTracing::TraceStartResponse response;
            grpc::ClientContext ctx;
            stub->TraceStart(&ctx, request, &response);
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }

        Sleep(TDuration::MilliSeconds(100));

        {
            Ydb::ActorTracing::TraceStopRequest request;
            Ydb::ActorTracing::TraceStopResponse response;
            grpc::ClientContext ctx;
            stub->TraceStop(&ctx, request, &response);
        }

        {
            Ydb::ActorTracing::TraceFetchRequest request;
            Ydb::ActorTracing::TraceFetchResponse response;
            grpc::ClientContext ctx;
            auto status = stub->TraceFetch(&ctx, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_message());
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

            Ydb::ActorTracing::TraceFetchResult result;
            response.operation().result().UnpackTo(&result);

            UNIT_ASSERT(result.trace_data().size() > 0);

            NActors::NTracing::TTraceChunk chunk;
            ui32 nodeId = 0;
            TBuffer buf(result.trace_data().data(), result.trace_data().size());
            UNIT_ASSERT(NActors::NTracing::DeserializeTrace(buf, chunk, nodeId));
            UNIT_ASSERT(chunk.Events.size() > 0);

            bool hasSend = false;
            bool hasReceive = false;
            bool hasNew = false;
            for (const auto& ev : chunk.Events) {
                auto type = static_cast<NActors::NTracing::ETraceEventType>(ev.Type);
                switch (type) {
                    case NActors::NTracing::ETraceEventType::SendLocal:
                        hasSend = true;
                        UNIT_ASSERT(ev.Sender != 0 || ev.Recipient != 0);
                        break;
                    case NActors::NTracing::ETraceEventType::ReceiveLocal:
                        hasReceive = true;
                        UNIT_ASSERT(ev.Recipient != 0);
                        UNIT_ASSERT(ev.MessageType != 0);
                        break;
                    case NActors::NTracing::ETraceEventType::New:
                        hasNew = true;
                        UNIT_ASSERT(ev.Sender != 0);
                        break;
                    case NActors::NTracing::ETraceEventType::Die:
                        UNIT_ASSERT(ev.Sender != 0);
                        break;
                    case NActors::NTracing::ETraceEventType::ForwardLocal:
                        UNIT_ASSERT(ev.HandleHash != 0);
                        UNIT_ASSERT(ev.Sender != 0);
                        break;
                }
            }
            UNIT_ASSERT_C(hasSend, "No SendLocal events found in trace");
            UNIT_ASSERT_C(hasReceive, "No ReceiveLocal events found in trace");
            UNIT_ASSERT_C(hasNew, "No New events found in trace");

            UNIT_ASSERT_C(!chunk.EventNamesDict.empty(), "EventNamesDict is empty");
        }
    }

    Y_UNIT_TEST(TraceFetchEmptyWhenNotStarted) {
        TActorTracingTestServer server;
        auto stub = Ydb::ActorTracing::V1::ActorTracingService::NewStub(server.GetChannel());

        Ydb::ActorTracing::TraceFetchRequest request;
        Ydb::ActorTracing::TraceFetchResponse response;
        grpc::ClientContext ctx;
        auto status = stub->TraceFetch(&ctx, request, &response);
        UNIT_ASSERT_C(status.ok(), status.error_message());
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

        Ydb::ActorTracing::TraceFetchResult result;
        response.operation().result().UnpackTo(&result);

        NActors::NTracing::TTraceChunk chunk;
        ui32 nodeId = 0;
        TBuffer buf(result.trace_data().data(), result.trace_data().size());
        bool ok = NActors::NTracing::DeserializeTrace(buf, chunk, nodeId);
        if (ok) {
            UNIT_ASSERT(chunk.Events.empty());
        }
    }
}
