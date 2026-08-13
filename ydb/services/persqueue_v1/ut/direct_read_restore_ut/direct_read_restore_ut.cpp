#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/services/persqueue_v1/actors/events.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/data_plane_helpers.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/thread/pool.h>

#include <atomic>
#include <memory>

using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace Ydb::Topic;

namespace NKikimr::NPersQueueTests {
namespace {

constexpr const char* kTopic = "dr-restore-topic";
constexpr const char* kTopicPath = "/Root/dr-restore-topic";
constexpr const char* kConsumer = "user";

#define DR_ENSURE(cond) \
    do { \
        if (!(cond)) { \
            ythrow yexception() << "check failed: " << #cond; \
        } \
    } while (false)

IThreadPool& DispatchPool() {
    struct THolder {
        TThreadPool Pool;
        THolder() {
            Pool.Start(2);
        }
    };
    static THolder holder;
    return holder.Pool;
}

template <typename TFunc>
auto RunWithDispatch(NActors::TTestActorRuntime& runtime, TFunc&& func) {
    auto future = NThreading::Async(std::forward<TFunc>(func), DispatchPool());
    return static_cast<NKikimr::TTestActorRuntime&>(runtime).WaitFuture(std::move(future));
}

template <typename TCondition>
void WaitUntil(NActors::TTestActorRuntime& runtime, TCondition&& condition, TDuration deadline = TDuration::Seconds(10)) {
    TDispatchOptions opts;
    opts.CustomFinalCondition = std::forward<TCondition>(condition);
    UNIT_ASSERT_C(runtime.DispatchEvents(opts, deadline), "WaitUntil condition not met before deadline");
}

TDriverConfig MakeAsyncDriverConfig(const TString& endpoint) {
    TDriverConfig config;
    config.SetEndpoint(endpoint);
    config.SetDatabase("/Root");
    config.SetAuthToken("root@builtin");
    config.SetDiscoveryMode(EDiscoveryMode::Async);
    return config;
}

ui64 ResolvePqTabletId(::NPersQueue::TTestServer& server, const TString& topicPath, ui32 partition = 0) {
    auto& runtime = *server.CleverServer->GetRuntime();
    const auto edge = runtime.AllocateEdgeActor();

    auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
    navigate->DatabaseName = "/Root";
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(topicPath);
    entry.SyncVersion = true;
    entry.ShowPrivatePath = true;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
    navigate->ResultSet.push_back(std::move(entry));

    runtime.Send(MakeSchemeCacheID(), edge,
        new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()),
        0, true);
    auto response = runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySetResult>();
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Request->ErrorCount, 0u);

    auto& front = response->Request->ResultSet.front();
    UNIT_ASSERT(front.PQGroupInfo);
    for (const auto& p : front.PQGroupInfo->Description.GetPartitions()) {
        if (p.GetPartitionId() == partition) {
            return p.GetTabletId();
        }
    }
    UNIT_FAIL("partition not found");
    return 0;
}

struct TDirectReadRestoreEnv {
    std::unique_ptr<::NPersQueue::TTestServer> Server;
    TString Endpoint;
    ui64 PqTabletId = 0;

    // Drop restore Prepare/Publish responses to keep restore stuck in Prepare (Forget race).
    std::atomic<ui64> HoldRestorePreparePublish{0};
    std::atomic<ui64> HeldPrepareOrPublish{0};

    // Hold CmdPrepareReadResult (re-inject later) to race with ResendRecentRequests after pipe restart.
    std::atomic<ui64> HoldPrepareResponses{0};
    std::atomic<ui64> HeldPrepareResponses{0};
    TVector<THolder<IEventHandle>> HeldPrepareEvents;

    // Hold CmdPublishReadResult (re-inject later) / keep restore stuck in Publish.
    std::atomic<ui64> HoldPublishResponses{0};
    std::atomic<ui64> HeldPublishResponses{0};
    TVector<THolder<IEventHandle>> HeldPublishEvents;

    // Hold CmdForgetReadResult (re-inject later) to keep restore stuck in Forget stage.
    std::atomic<ui64> HoldForgetResponses{0};
    std::atomic<ui64> HeldForgetResponses{0};
    TVector<THolder<IEventHandle>> HeldForgetEvents;

    // Any TEvCloseSession with ErrorCode != OK (ENSURE, empty-queue CloseSessionAndDie, bad ack, …).
    // Teardown DropHooks() clears the observer before gRPC cancel, so shutdown noise is ignored.
    std::atomic<ui64> ErrorCloseSession{0};
    TString ErrorCloseReason;

    // OnDirectReadsRestored → TEvUpdateSession; proves restore finished (not silent-hang).
    std::atomic<ui64> UpdateSessionCount{0};
    // Normal-path Publish completed → TEvDirectReadResponse to read session.
    std::atomic<ui64> DirectReadResponseCount{0};
    // Control-path DirectReadAck delivered to partition actor.
    std::atomic<ui64> DirectReadAckCount{0};

    // Prepare/Publish delivered while Forget responses are held (late reply onto Forget stage).
    std::atomic<ui64> LateNonForgetReplyDuringForget{0};

    NActors::TTestActorRuntime& Runtime() {
        return *Server->CleverServer->GetRuntime();
    }

    void Start() {
        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.SetUseRealThreads(false);
        settings.SetNodeCount(1);
        // Skip SysViews roster WaitFor in TServer::Initialize (~10s under UseRealThreads=false).
        settings.FeatureFlags.SetEnableRealSystemViewPaths(false);

        // Inline TTestServer::StartServer so AnnoyingClient (Sync discovery) can run
        // under RunWithDispatch; shared StartServer constructs it without a pump.
        Server = std::make_unique<::NPersQueue::TTestServer>(settings, /*start=*/false);
        Endpoint = Server->Endpoint;

        Server->PrepareNetDataFile();
        Server->CleverServer = MakeHolder<NKikimr::Tests::TServer>(Server->ServerSettings);
        Server->CleverServer->EnableGRpc(Server->GrpcServerOptions);

        Server->Log.SetFormatter([](ELogPriority priority, TStringBuf message) {
            return TStringBuilder() << TInstant::Now() << " " << priority << ": " << message << Endl;
        });
        Server->Log << TLOG_INFO << "TTestServer started on Port " << Server->Port
                    << " GrpcPort " << Server->GrpcPort;

        auto& runtime = Runtime();
        runtime.SetScheduledLimit(100'000);

        // TFlatMsgBusPQClient builds NYdb::TDriver with default Sync discovery, which blocks
        // on ListEndpoints (GET_ENDPOINTS_TIMEOUT=10s). With UseRealThreads=false the request
        // is only served while we DispatchEvents — so construct it under RunWithDispatch.
        RunWithDispatch(runtime, [&] {
            Server->AnnoyingClient = MakeHolder<NKikimr::NPersQueueTests::TFlatMsgBusPQClient>(
                Server->ServerSettings, Server->GrpcPort, TString("/Root"));
            return true;
        });

        Server->AnnoyingClient->SetNoConfigMode();

        // Configure service log levels here if needed, e.g.:
        // runtime.SetLogPriority(NKikimrServices::PQ_READ_PROXY, NActors::NLog::PRI_DEBUG);

        InstallHooks();

        // No-config mode: FullInit() would still call InitSourceIds({}) with an empty
        // path (CreateTable fails with "Path does not exist"). Only need Root + /PQ.
        RunWithDispatch(runtime, [&] {
            Server->AnnoyingClient->InitRootScheme();
            return true;
        });
        RunWithDispatch(runtime, [&] {
            Server->AnnoyingClient->MkDir("/Root", "PQ");
            return true;
        });

        RunWithDispatch(runtime, [&] {
            TDriver driver(MakeAsyncDriverConfig(Endpoint));
            TTopicClient client(driver);
            auto status = client.CreateTopic(
                kTopic,
                TCreateTopicSettings()
                    .BeginConfigurePartitioningSettings()
                        .MinActivePartitions(1)
                        .MaxActivePartitions(1)
                        .BeginConfigureAutoPartitioningSettings()
                            .Strategy(EAutoPartitioningStrategy::Disabled)
                        .EndConfigureAutoPartitioningSettings()
                    .EndConfigurePartitioningSettings()
                    .BeginAddConsumer(kConsumer)
                    .EndAddConsumer()).ExtractValueSync();
            if (!status.IsSuccess()) {
                ythrow yexception() << status.GetIssues().ToString();
            }
            driver.Stop(true);
            return true;
        });

        PqTabletId = ResolvePqTabletId(*Server, kTopicPath);
    }

    void InstallHooks() {
        auto& runtime = Runtime();

        runtime.SetEventFilter([this](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            auto* msg = ev->CastAsLocal<TEvPersQueue::TEvResponse>();
            if (!msg || !msg->Record.HasPartitionResponse()) {
                return false;
            }
            const auto& part = msg->Record.GetPartitionResponse();

            if (HoldPrepareResponses.load() && part.HasCmdPrepareReadResult()) {
                ++HeldPrepareResponses;
                HeldPrepareEvents.emplace_back(ev.Release());
                return true;
            }

            if (HoldPublishResponses.load() && part.HasCmdPublishReadResult()) {
                ++HeldPublishResponses;
                HeldPublishEvents.emplace_back(ev.Release());
                return true;
            }

            if (HoldForgetResponses.load() && part.HasCmdForgetReadResult()) {
                ++HeldForgetResponses;
                HeldForgetEvents.emplace_back(ev.Release());
                return true;
            }

            if (HoldRestorePreparePublish.load()
                    && (part.HasCmdPrepareReadResult() || part.HasCmdPublishReadResult())) {
                ++HeldPrepareOrPublish;
                return true; // drop — keep restore stuck in Prepare
            }

            // Late Prepare/Publish reaching the actor while Forget stage is held under test.
            if (HoldForgetResponses.load()
                    && (part.HasCmdPrepareReadResult() || part.HasCmdPublishReadResult())) {
                ++LateNonForgetReplyDuringForget;
            }
            return false;
        });

        runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (auto* msg = ev->CastAsLocal<NGRpcProxy::V1::TEvPQProxy::TEvCloseSession>()) {
                if (msg->ErrorCode != Ydb::PersQueue::ErrorCode::OK) {
                    ErrorCloseReason = msg->Reason;
                    ++ErrorCloseSession;
                }
            }
            if (ev->CastAsLocal<NGRpcProxy::V1::TEvPQProxy::TEvUpdateSession>()) {
                ++UpdateSessionCount;
            }
            if (ev->CastAsLocal<NGRpcProxy::V1::TEvPQProxy::TEvDirectReadResponse>()) {
                ++DirectReadResponseCount;
            }
            if (ev->CastAsLocal<NGRpcProxy::V1::TEvPQProxy::TEvDirectReadAck>()) {
                ++DirectReadAckCount;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
    }

    void ReleaseHeldPrepares() {
        HoldPrepareResponses.store(0);
        auto& runtime = Runtime();
        for (auto& ev : HeldPrepareEvents) {
            runtime.Send(ev.Release(), /*senderNodeIndex=*/0, /*viaActorSystem=*/true);
        }
        HeldPrepareEvents.clear();
    }

    void ReleaseHeldPublishes() {
        HoldPublishResponses.store(0);
        auto& runtime = Runtime();
        for (auto& ev : HeldPublishEvents) {
            runtime.Send(ev.Release(), /*senderNodeIndex=*/0, /*viaActorSystem=*/true);
        }
        HeldPublishEvents.clear();
    }

    void ReleaseHeldForgets() {
        HoldForgetResponses.store(0);
        auto& runtime = Runtime();
        for (auto& ev : HeldForgetEvents) {
            runtime.Send(ev.Release(), /*senderNodeIndex=*/0, /*viaActorSystem=*/true);
        }
        HeldForgetEvents.clear();
    }

    void DropHooks() {
        auto& runtime = Runtime();
        runtime.SetEventFilter(&TTestActorRuntimeBase::DefaultFilterFunc);
        runtime.SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
        HeldPrepareEvents.clear();
        HeldPublishEvents.clear();
        HeldForgetEvents.clear();
    }

    void RebootPqTablet() {
        auto& runtime = Runtime();
        const auto edge = runtime.AllocateEdgeActor();
        RebootTablet(runtime, PqTabletId, edge);
        // Callers WaitUntil for restore progress; DispatchEvents advances RESTART_PIPE_DELAY_MS.
    }
};

struct TGrpcDirectReadClient {
    using TService = Ydb::Topic::V1::TopicService;

    std::shared_ptr<grpc::Channel> Channel;
    std::unique_ptr<TService::Stub> Stub;
    THolder<grpc::ClientContext> ControlContext;
    THolder<grpc::ClientContext> DirectContext;
    std::unique_ptr<grpc::ClientReaderWriter<StreamReadMessage::FromClient, StreamReadMessage::FromServer>> ControlStream;
    std::unique_ptr<grpc::ClientReaderWriter<StreamDirectReadMessage::FromClient, StreamDirectReadMessage::FromServer>> DirectStream;
    TString SessionId;
    ui64 AssignId = 0;
    ui64 Generation = 0;

    void Connect(const TString& endpoint) {
        grpc::ChannelArguments args;
        args.SetMaxReceiveMessageSize(64_MB);
        args.SetMaxSendMessageSize(64_MB);
        Channel = grpc::CreateCustomChannel(
            endpoint,
            grpc::InsecureChannelCredentials(),
            args);
        Stub = TService::NewStub(Channel);
    }

    void InitControlSession(NActors::TTestActorRuntime& runtime) {
        RunWithDispatch(runtime, [&] {
            ControlContext = MakeHolder<grpc::ClientContext>();
            ControlStream = Stub->StreamRead(ControlContext.Get());
            DR_ENSURE(ControlStream);

            StreamReadMessage::FromClient req;
            req.mutable_init_request()->add_topics_read_settings()->set_path(kTopicPath);
            req.mutable_init_request()->set_consumer(kConsumer);
            req.mutable_init_request()->set_direct_read(true);
            DR_ENSURE(ControlStream->Write(req));

            StreamReadMessage::FromServer resp;
            DR_ENSURE(ControlStream->Read(&resp));
            if (resp.server_message_case() != StreamReadMessage::FromServer::kInitResponse) {
                ythrow yexception() << "expected InitResponse, got " << resp.ShortDebugString();
            }
            SessionId = resp.init_response().session_id();

            StreamReadMessage::FromClient readReq;
            readReq.mutable_read_request()->set_bytes_size(100_KB);
            DR_ENSURE(ControlStream->Write(readReq));
            return true;
        });
    }

    void AcceptAssign(NActors::TTestActorRuntime& runtime) {
        RunWithDispatch(runtime, [&] {
            StreamReadMessage::FromServer resp;
            DR_ENSURE(ControlStream->Read(&resp));
            if (resp.server_message_case() != StreamReadMessage::FromServer::kStartPartitionSessionRequest) {
                ythrow yexception() << "expected StartPartitionSessionRequest, got " << resp.ShortDebugString();
            }
            AssignId = resp.start_partition_session_request().partition_session().partition_session_id();
            Generation = resp.start_partition_session_request().partition_location().generation();

            StreamReadMessage::FromClient req;
            req.mutable_start_partition_session_response()->set_partition_session_id(AssignId);
            DR_ENSURE(ControlStream->Write(req));
            return true;
        });
    }

    void InitDirectSession(NActors::TTestActorRuntime& runtime) {
        RunWithDispatch(runtime, [&] {
            DirectContext = MakeHolder<grpc::ClientContext>();
            DirectStream = Stub->StreamDirectRead(DirectContext.Get());
            DR_ENSURE(DirectStream);

            StreamDirectReadMessage::FromClient req;
            req.mutable_init_request()->add_topics_read_settings()->set_path(kTopicPath);
            req.mutable_init_request()->set_consumer(kConsumer);
            req.mutable_init_request()->set_session_id(SessionId);
            DR_ENSURE(DirectStream->Write(req));

            StreamDirectReadMessage::FromServer resp;
            DR_ENSURE(DirectStream->Read(&resp));
            if (resp.status() != Ydb::StatusIds::SUCCESS) {
                ythrow yexception() << "direct init failed: " << resp.ShortDebugString();
            }
            return true;
        });
    }

    void StartDirectReadPartition(NActors::TTestActorRuntime& runtime) {
        RunWithDispatch(runtime, [&] {
            StreamDirectReadMessage::FromClient req;
            auto& start = *req.mutable_start_direct_read_partition_session_request();
            start.set_partition_session_id(AssignId);
            start.set_generation(Generation);
            DR_ENSURE(DirectStream->Write(req));

            StreamDirectReadMessage::FromServer resp;
            DR_ENSURE(DirectStream->Read(&resp));
            if (resp.status() != Ydb::StatusIds::SUCCESS
                    || resp.server_message_case() != StreamDirectReadMessage::FromServer::kStartDirectReadPartitionSessionResponse) {
                ythrow yexception() << "start direct partition failed: " << resp.ShortDebugString();
            }
            return true;
        });
    }

    void ReadDataNoAck(NActors::TTestActorRuntime& runtime, i64 expectedDirectReadId) {
        RunWithDispatch(runtime, [&] {
            StreamDirectReadMessage::FromServer resp;
            DR_ENSURE(DirectStream->Read(&resp));
            if (resp.status() != Ydb::StatusIds::SUCCESS
                    || resp.server_message_case() != StreamDirectReadMessage::FromServer::kDirectReadResponse
                    || resp.direct_read_response().direct_read_id() != expectedDirectReadId
                    || resp.direct_read_response().partition_session_id() != static_cast<i64>(AssignId)) {
                ythrow yexception() << "unexpected DirectReadResponse: " << resp.ShortDebugString();
            }
            return true;
        });
    }

    void SendDirectReadAckNoWait(NActors::TTestActorRuntime& runtime, ui64 directReadId) {
        RunWithDispatch(runtime, [&] {
            StreamReadMessage::FromClient req;
            auto& ack = *req.mutable_direct_read_ack();
            ack.set_partition_session_id(AssignId);
            ack.set_direct_read_id(directReadId);
            DR_ENSURE(ControlStream->Write(req));
            return true;
        });
    }

};

void TearDownGrpcAndServer(TDirectReadRestoreEnv& env, TGrpcDirectReadClient& client) {
    env.DropHooks();

    if (client.ControlContext) {
        client.ControlContext->TryCancel();
    }
    if (client.DirectContext) {
        client.DirectContext->TryCancel();
    }
    client.ControlStream.reset();
    client.DirectStream.reset();
    client.Stub.reset();
    client.Channel.reset();

    if (!env.Server) {
        return;
    }

    auto& runtime = env.Runtime();
    auto shutdown = NThreading::Async([&] {
        env.Server->ShutdownGRpc();
        return true;
    }, DispatchPool());
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(5);
    while (!shutdown.HasValue() && !shutdown.HasException()) {
        if (TInstant::Now() >= deadline) {
            UNIT_FAIL("ShutdownGRpc did not complete within 5 seconds");
        }
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
    }
    shutdown.GetValueSync();
    env.Server->ShutdownServer();
    env.Server.reset();
}

class TDirectReadRestoreFixture : public NUnitTest::TBaseFixture {
protected:
    TDirectReadRestoreEnv Env;
    TGrpcDirectReadClient Client;

    void SetUp(NUnitTest::TTestContext&) override {
        Env.Start();
    }

    void TearDown(NUnitTest::TTestContext&) override {
        TearDownGrpcAndServer(Env, Client);
    }

    NActors::TTestActorRuntime& Runtime() {
        return Env.Runtime();
    }

    // 1. Produce one large message so DirectRead has data to restore.
    void WriteOneMessage() {
        RunWithDispatch(Runtime(), [&] {
            TDriver driver(MakeAsyncDriverConfig(Env.Endpoint));
            auto writer = CreateSimpleWriter(driver, kTopicPath, "src", /*partitionGroup=*/{}, TString("raw"));
            if (!writer->Write(TString(1_MB, 'x'))) {
                ythrow yexception() << "write failed";
            }
            writer->Close();
            driver.Stop(true);
            return true;
        });
    }

    // 2. Open control+direct sessions; optionally consume DirectReadId=1 without ack.
    void ConnectControlSession() {
        Client.Connect(Env.Endpoint);
        Client.InitControlSession(Runtime());
    }

    void AcceptAssignAndStartDirectRead(bool readDataNoAck = true) {
        auto& runtime = Runtime();
        Client.AcceptAssign(runtime);
        Client.InitDirectSession(runtime);
        Client.StartDirectReadPartition(runtime);
        if (readDataNoAck) {
            Client.ReadDataNoAck(runtime, /*expectedDirectReadId=*/1);
        }
    }

    void OpenDirectReadSession(bool readDataNoAck = true) {
        ConnectControlSession();
        AcceptAssignAndStartDirectRead(readDataNoAck);
    }

    // 3. Reboot PQ tablet and wait until the matching hold counter reaches minHeld.
    // Callers set Hold* flags before the first reboot when several holds are needed together.
    void RebootAndWaitHeldPrepares(ui64 minHeld) {
        if (Env.HoldPrepareResponses.load() == 0) {
            Env.HoldPrepareResponses.store(1);
        }
        Env.RebootPqTablet();
        WaitUntil(Runtime(), [&] {
            return Env.HeldPrepareResponses.load() >= minHeld;
        });
        UNIT_ASSERT_C(Env.HeldPrepareResponses.load() >= minHeld,
            "expected held CmdPrepareReadResult count >= " << minHeld
                << "; held=" << Env.HeldPrepareResponses.load());
    }

    void RebootAndWaitHeldPublishes(ui64 minHeld) {
        if (Env.HoldPublishResponses.load() == 0) {
            Env.HoldPublishResponses.store(1);
        }
        Env.RebootPqTablet();
        WaitUntil(Runtime(), [&] {
            return Env.HeldPublishResponses.load() >= minHeld;
        });
        UNIT_ASSERT_C(Env.HeldPublishResponses.load() >= minHeld,
            "expected held CmdPublishReadResult count >= " << minHeld
                << "; held=" << Env.HeldPublishResponses.load());
    }

    void RebootAndWaitHeldForgets(ui64 minHeld) {
        if (Env.HoldForgetResponses.load() == 0) {
            Env.HoldForgetResponses.store(1);
        }
        Env.RebootPqTablet();
        WaitUntil(Runtime(), [&] {
            return Env.HeldForgetResponses.load() >= minHeld;
        });
        UNIT_ASSERT_C(Env.HeldForgetResponses.load() >= minHeld,
            "expected held CmdForgetReadResult count >= " << minHeld
                << "; held=" << Env.HeldForgetResponses.load());
    }

    void RebootAndWaitHeldRestorePrepareOrPublish() {
        Env.HoldRestorePreparePublish.store(1);
        Env.RebootPqTablet();
        WaitUntil(Runtime(), [&] {
            return Env.HeldPrepareOrPublish.load() > 0;
        });
        UNIT_ASSERT_C(Env.HeldPrepareOrPublish.load() > 0,
            "expected dropped CmdPrepareReadResult/CmdPublishReadResult during restore");
    }

    // 4. Ack DirectRead and wait until the ack reaches the partition actor.
    void AckDirectRead(ui64 directReadId = 1) {
        const ui64 before = Env.DirectReadAckCount.load();
        Client.SendDirectReadAckNoWait(Runtime(), directReadId);
        WaitUntil(Runtime(), [&] {
            return Env.DirectReadAckCount.load() > before
                || Env.ErrorCloseSession.load() > 0;
        });
        AssertNoErrorClose("DirectReadAck must not kill partition actor");
    }

    // 9–10. Shared asserts.
    void AssertNoErrorClose(const TString& what) {
        UNIT_ASSERT_C(Env.ErrorCloseSession.load() == 0,
            what
                << "; heldPrepare=" << Env.HeldPrepareResponses.load()
                << "; heldPublish=" << Env.HeldPublishResponses.load()
                << "; heldForget=" << Env.HeldForgetResponses.load()
                << "; heldRestorePP=" << Env.HeldPrepareOrPublish.load()
                << "; reason=" << Env.ErrorCloseReason);
    }

    void AssertUpdateSessionAdvanced(ui64 before, const TString& what) {
        UNIT_ASSERT_C(Env.UpdateSessionCount.load() > before, what);
    }

    void AssertDirectReadAdvanced(ui64 before, const TString& what) {
        UNIT_ASSERT_C(Env.DirectReadResponseCount.load() > before, what);
    }

    // 5. Release held Prepares; wait until Publish is held (or crash).
    void ReleasePreparesAndWaitPublishHeld() {
        Env.ReleaseHeldPrepares();
        WaitUntil(Runtime(), [&] {
            return Env.HeldPublishResponses.load() >= 1
                || Env.ErrorCloseSession.load() > 0;
        });
        AssertNoErrorClose(
            "late Prepare during Publish restore must not kill partition actor via unhandled exception");
        UNIT_ASSERT_C(Env.HeldPublishResponses.load() >= 1,
            "expected Publish stage after releasing held Prepares");
    }

    // 6. Release held replies and wait for OnDirectReadsRestored → TEvUpdateSession.
    template <typename TRelease>
    void ReleaseAndWaitUpdateSession(TRelease&& release, const TString& what) {
        const ui64 before = Env.UpdateSessionCount.load();
        release();
        WaitUntil(Runtime(), [&] {
            return Env.UpdateSessionCount.load() > before
                || Env.ErrorCloseSession.load() > 0;
        });
        AssertNoErrorClose(what);
        AssertUpdateSessionAdvanced(before,
            "restore must complete with TEvUpdateSession (not silent-hang)");
    }

    // 7. Release held Prepares and wait for normal-path DirectReadResponse.
    template <typename TRelease>
    void ReleaseAndWaitDirectReadResponse(TRelease&& release, const TString& what) {
        const ui64 before = Env.DirectReadResponseCount.load();
        release();
        WaitUntil(Runtime(), [&] {
            return Env.DirectReadResponseCount.load() > before
                || Env.ErrorCloseSession.load() > 0;
        });
        AssertNoErrorClose(what);
        AssertDirectReadAdvanced(before,
            "Prepare/Publish must complete after releasing held Prepares (not silent-hang)");
    }

    // 8. Release a late reply onto Forget stage; wait until it is delivered (or ENSURE fires).
    template <typename TRelease>
    void ReleaseLateReplyAndAssertSurvived(TRelease&& release, const TString& what) {
        const ui64 lateBefore = Env.LateNonForgetReplyDuringForget.load();
        const ui64 errorCloseBefore = Env.ErrorCloseSession.load();
        release();
        WaitUntil(Runtime(), [&] {
            return Env.LateNonForgetReplyDuringForget.load() > lateBefore
                || Env.ErrorCloseSession.load() > errorCloseBefore;
        });
        AssertNoErrorClose(what);
    }

    // Wait for UpdateSession after an already-triggered action (e.g. nested reboot).
    void WaitAndAssertUpdateSessionAdvanced(ui64 before, const TString& what) {
        WaitUntil(Runtime(), [&] {
            return Env.UpdateSessionCount.load() > before
                || Env.ErrorCloseSession.load() > 0;
        });
        AssertNoErrorClose(what);
        AssertUpdateSessionAdvanced(before,
            "restore must complete with TEvUpdateSession (not silent-hang)");
    }
};

} // namespace

Y_UNIT_TEST_SUITE_F(TDirectReadRestoreRaceTest, TDirectReadRestoreFixture) {

// LOGBROKER-10590: forget-first after nested pipe restart with RestoredDirectReadId==0
// must not kill the partition actor (Forget stage must tolerate RestoredDirectReadId==0).
Y_UNIT_TEST(RestoredDirectReadIdZeroOnForgetAfterDoubleRestart) {
    WriteOneMessage();
    OpenDirectReadSession();

    RebootAndWaitHeldRestorePrepareOrPublish();
    AckDirectRead();

    // HoldRestorePreparePublish filters only Prepare/Publish, not Forget. Clear it before the
    // nested reboot so Session→Forget is not mixed with the first-attempt prepare hold.
    Env.HoldRestorePreparePublish.store(0);
    const ui64 updateSessionsBefore = Env.UpdateSessionCount.load();
    Env.RebootPqTablet();
    WaitAndAssertUpdateSessionAdvanced(updateSessionsBefore,
        "Forget after nested restart must not kill partition actor via unhandled exception");
}

// LOGBROKER-10590: after pipe restart with RequestInfly, ResendRecentRequests re-sends Prepare;
// a late/duplicate CmdPrepareReadResult on the normal path must be ignored (not ENSURE).
Y_UNIT_TEST(RequestInflyOnDuplicatePrepareAfterPipeRestart) {
    WriteOneMessage();
    ConnectControlSession();

    // Hold Prepare before partition starts reading, so RequestInfly stays true across reboot.
    Env.HoldPrepareResponses.store(1);
    AcceptAssignAndStartDirectRead(/*readDataNoAck=*/false);

    WaitUntil(Runtime(), [&] {
        return Env.HeldPrepareResponses.load() >= 1;
    });
    UNIT_ASSERT_C(Env.HeldPrepareResponses.load() >= 1,
        "expected at least one held CmdPrepareReadResult before reboot");

    // Restore of empty DirectReadResults finishes quickly → ResendRecentRequests → second Prepare.
    RebootAndWaitHeldPrepares(/*minHeld=*/2);

    ReleaseAndWaitDirectReadResponse(
        [&] { Env.ReleaseHeldPrepares(); },
        "duplicate Prepare after pipe restart must not kill partition actor via unhandled exception");
}

// LOGBROKER-10590: nested pipe restart re-sends restore Prepare for the same DirectReadId;
// a late Prepare after restore has moved to Publish must be ignored (not ENSURE).
Y_UNIT_TEST(HasCmdPublishReadResultOnPrepareDuringPublishRestore) {
    WriteOneMessage();
    OpenDirectReadSession();

    // Hold Prepare (and Publish) so nested restart can queue a second real Prepare
    // before either response is delivered — same shape as late pipe replies in prod.
    Env.HoldPublishResponses.store(1);
    RebootAndWaitHeldPrepares(/*minHeld=*/1);
    RebootAndWaitHeldPrepares(/*minHeld=*/2);

    // First Prepare advances restore to Publish; second is soft-ignored on Publish stage.
    ReleasePreparesAndWaitPublishHeld();
    ReleaseAndWaitUpdateSession(
        [&] { Env.ReleaseHeldPublishes(); },
        "releasing Publish after late Prepare must not kill partition actor");
}

// LOGBROKER-10590: after ack during Prepare-restore + nested pipe restart, forget-first runs with
// RestoredDirectReadId==0; a late Prepare from the previous restore attempt must be ignored.
Y_UNIT_TEST(HasCmdForgetReadResultOnPrepareDuringForgetRestore) {
    WriteOneMessage();
    OpenDirectReadSession();

    // Hold Prepare from the first restore so it can be re-injected later (late delivery).
    RebootAndWaitHeldPrepares(/*minHeld=*/1);

    // Ack while RestoredDirectReadId==id → DirectReadsToForget; DirectReadResults cleared.
    AckDirectRead();

    // Keep Forget stuck so released Prepare arrives on Forget stage (prod: Prepare in flight /
    // mailbox vs new Forget after nested disconnect).
    RebootAndWaitHeldForgets(/*minHeld=*/1);

    ReleaseLateReplyAndAssertSurvived(
        [&] { Env.ReleaseHeldPrepares(); },
        "late Prepare during Forget restore must not kill partition actor via unhandled exception");
    ReleaseAndWaitUpdateSession(
        [&] { Env.ReleaseHeldForgets(); },
        "releasing Forget after late Prepare must not kill partition actor");
}

// LOGBROKER-10590: Prepare+Publish restore in flight, client acks, nested pipe restart → forget-first;
// late Publish from the previous restore attempt must be ignored (not ENSURE on Forget).
Y_UNIT_TEST(HasCmdForgetReadResultOnPublishDuringForgetRestore) {
    WriteOneMessage();
    OpenDirectReadSession();

    // Let Prepare complete; hold Publish (prod: Publish reply still on the wire / in mailbox).
    RebootAndWaitHeldPublishes(/*minHeld=*/1);

    // Ack while RestoredDirectReadId==id → DirectReadsToForget; DirectReadResults cleared.
    AckDirectRead();
    RebootAndWaitHeldForgets(/*minHeld=*/1);

    ReleaseLateReplyAndAssertSurvived(
        [&] { Env.ReleaseHeldPublishes(); },
        "late Publish during Forget restore must not kill partition actor via unhandled exception");
    ReleaseAndWaitUpdateSession(
        [&] { Env.ReleaseHeldForgets(); },
        "releasing Forget after late Publish must not kill partition actor");
}

} // Y_UNIT_TEST_SUITE_F(TDirectReadRestoreRaceTest)

} // namespace NKikimr::NPersQueueTests
