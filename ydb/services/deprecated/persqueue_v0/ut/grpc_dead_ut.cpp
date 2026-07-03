#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/services/deprecated/persqueue_v0/api/grpc/persqueue.grpc.pb.h>
#include <ydb/services/deprecated/persqueue_v0/grpc_pq_actor.h>
#include <ydb/services/deprecated/persqueue_v0/persqueue.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>
#include <util/system/guard.h>

namespace NKikimr::NGRpcService {

namespace {

constexpr TDuration StreamDeadline = TDuration::Seconds(5);
constexpr TDuration InitRetryDeadline = TDuration::Seconds(30);

class TGrpcDeadGuard {
public:
    TGrpcDeadGuard() {
        NYdbGrpc::GrpcDead = false;
    }

    ~TGrpcDeadGuard() {
        NYdbGrpc::GrpcDead = false;
    }
};

class TDeprecatedPQGrpcServer {
public:
    explicit TDeprecatedPQGrpcServer(::NPersQueue::TTestServer& testServer)
        : Port_(testServer.PortManager->GetPort(2136))
    {
        auto* actorSystem = testServer.CleverServer->GetRuntime()->GetAnyNodeActorSystem();
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();

        NYdbGrpc::TServerOptions options;
        options.SetHost("[::1]");
        options.SetPort(Port_);
        options.SetGRpcShutdownDeadline(TDuration::Seconds(3));

        Server_ = MakeHolder<NYdbGrpc::TGRpcServer>(options);
        NYdbGrpc::IGRpcService* service = new NKikimr::NGRpcService::TGRpcPersQueueService(
            actorSystem,
            counters,
            NMsgBusProxy::CreatePersQueueMetaCacheV2Id());
        Server_->AddService(service);
        Server_->Start();
    }

    ~TDeprecatedPQGrpcServer() {
        Stop();
    }

    TDeprecatedPQGrpcServer(const TDeprecatedPQGrpcServer&) = delete;
    TDeprecatedPQGrpcServer& operator=(const TDeprecatedPQGrpcServer&) = delete;

    ui16 GetPort() const {
        return Port_;
    }

    TString GetEndpoint() const {
        return TStringBuilder() << "localhost:" << Port_;
    }

    void Stop() {
        if (Server_) {
            Server_->Stop();
            Server_.Reset();
        }
    }

private:
    ui16 Port_;
    THolder<NYdbGrpc::TGRpcServer> Server_;
};

struct TDeprecatedWriteSession {
    grpc::ClientContext Context;
    std::unique_ptr<NPersQueue::PersQueueService::Stub> Stub;
    std::unique_ptr<grpc::ClientReaderWriter<NPersQueue::TWriteRequest, NPersQueue::TWriteResponse>> Stream;
};

void ConfigureCompatTopics(::NPersQueue::TTestServer& testServer) {
    testServer.ServerSettings.PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root/LbCommunal");
    testServer.ServerSettings.PQConfig.SetTestDatabaseRoot("/Root/LbCommunal");
    testServer.ServerSettings.PQConfig.SetTopicsAreFirstClassCitizen(false);
}

void InitTestData(::NPersQueue::TTestServer& testServer) {
    testServer.AnnoyingClient->MkDir("/Root", "LbCommunal");
    testServer.AnnoyingClient->MkDir("/Root/LbCommunal", "account");
    testServer.AnnoyingClient->CreateTopicNoLegacy(
        "/Root/LbCommunal/account/topic2",
        1,
        true,
        true,
        {},
        {"user", "test-consumer"},
        "account");
    testServer.WaitInit("account/topic2");
}

std::unique_ptr<TDeprecatedWriteSession> CreateWriteSession(const TString& endpoint) {
    auto result = std::make_unique<TDeprecatedWriteSession>();
    result->Context.AddMetadata("x-ydb-database", "/Root/LbCommunal/account");
    result->Context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(StreamDeadline.MilliSeconds()));

    auto channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    result->Stub = NPersQueue::PersQueueService::NewStub(channel);
    result->Stream = result->Stub->WriteSession(&result->Context);
    UNIT_ASSERT(result->Stream);
    return result;
}

bool TryWriteInit(TDeprecatedWriteSession& session) {
    NPersQueue::TWriteRequest request;
    auto* init = request.MutableInit();
    init->SetTopic("account/topic2");
    init->SetSourceId("grpc-dead-ut-source");
    init->SetVersion("grpc-dead-ut");
    init->SetProxyCookie(NGRpcProxy::MAGIC_COOKIE_VALUE);

    UNIT_ASSERT_C(session.Stream->Write(request), "failed to write deprecated PQ init request");

    NPersQueue::TWriteResponse response;
    UNIT_ASSERT_C(session.Stream->Read(&response), "failed to read deprecated PQ init response");
    if (response.HasError() && response.GetError().GetCode() == NPersQueue::NErrorCode::INITIALIZING) {
        return false;
    }
    UNIT_ASSERT_C(response.HasInit(), response.ShortDebugString());
    return true;
}

std::unique_ptr<TDeprecatedWriteSession> CreateInitializedWriteSession(const TString& endpoint) {
    const auto deadline = TInstant::Now() + InitRetryDeadline;
    do {
        auto session = CreateWriteSession(endpoint);
        if (TryWriteInit(*session)) {
            return session;
        }

        session->Stream->WritesDone();
        session->Stream->Finish();
        Sleep(TDuration::MilliSeconds(100));
    } while (TInstant::Now() < deadline);

    UNIT_FAIL("deprecated PQ write session is still initializing");
    return nullptr;
}

void WriteDataAndWaitAck(TDeprecatedWriteSession& session, ui64 seqNo) {
    NPersQueue::TWriteRequest request;
    auto* data = request.MutableData();
    data->SetSeqNo(seqNo);
    data->SetData(TStringBuilder() << "message-" << seqNo);
    data->SetCreateTimeMs(TInstant::Now().MilliSeconds());
    data->SetCodec(NPersQueueCommon::RAW);
    data->SetUncompressedSize(data->GetData().size());

    UNIT_ASSERT_C(session.Stream->Write(request), "failed to write deprecated PQ data request");

    NPersQueue::TWriteResponse response;
    UNIT_ASSERT_C(session.Stream->Read(&response), "failed to read deprecated PQ ack response");
    UNIT_ASSERT_C(response.HasAck(), response.ShortDebugString());
    UNIT_ASSERT_VALUES_EQUAL(response.GetAck().GetSeqNo(), seqNo);
}

void FinishWriteSession(TDeprecatedWriteSession& session) {
    session.Stream->WritesDone();
    const auto status = session.Stream->Finish();
    UNIT_ASSERT_C(status.ok(), status.error_message());
}

} // namespace

Y_UNIT_TEST_SUITE(TDeprecatedPQGrpcDeadTest) {
    Y_UNIT_TEST(StoppingAnotherGrpcServerDoesNotStopLiveDeprecatedPQWriteSession) {
        TGrpcDeadGuard grpcDeadGuard;
        ::NPersQueue::TTestServer testServer(false);
        ConfigureCompatTopics(testServer);
        testServer.StartServer();
        InitTestData(testServer);

        TDeprecatedPQGrpcServer stoppedServer(testServer);
        TDeprecatedPQGrpcServer liveServer(testServer);

        auto writeSession = CreateInitializedWriteSession(liveServer.GetEndpoint());
        WriteDataAndWaitAck(*writeSession, 1);

        stoppedServer.Stop();
        UNIT_ASSERT(NYdbGrpc::GrpcDead.load());

        WriteDataAndWaitAck(*writeSession, 2);
        FinishWriteSession(*writeSession);
    }

    Y_UNIT_TEST(ProcessGlobalGrpcDeadDoesNotStopLiveDeprecatedPQWriteSession) {
        TGrpcDeadGuard grpcDeadGuard;
        ::NPersQueue::TTestServer testServer(false);
        ConfigureCompatTopics(testServer);
        testServer.StartServer();
        InitTestData(testServer);

        NYdbGrpc::GrpcDead = true;

        TDeprecatedPQGrpcServer liveServer(testServer);

        auto writeSession = CreateInitializedWriteSession(liveServer.GetEndpoint());
        WriteDataAndWaitAck(*writeSession, 1);
        FinishWriteSession(*writeSession);
    }
}

} // namespace NKikimr::NGRpcService
