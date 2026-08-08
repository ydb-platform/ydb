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

    // Hold CmdPublishReadResult to keep restore stuck in Publish stage.
    std::atomic<ui64> HoldPublishResponses{0};
    std::atomic<ui64> HeldPublishResponses{0};

    // PARTITION_ENSURE → OnUnhandledException → CloseSession("unexpected error: ...").
    std::atomic<ui64> UnexpectedErrorCloseSession{0};
    TString UnexpectedErrorCloseReason;

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
                return true; // drop — keep restore stuck in Publish
            }

            if (!HoldRestorePreparePublish.load()) {
                return false;
            }
            if (part.HasCmdPrepareReadResult() || part.HasCmdPublishReadResult()) {
                ++HeldPrepareOrPublish;
                return true; // drop — keep restore stuck in Prepare
            }
            return false;
        });

        runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (auto* msg = ev->CastAsLocal<NGRpcProxy::V1::TEvPQProxy::TEvCloseSession>()) {
                // OnUnhandledException always prefixes with "unexpected error:".
                if (msg->Reason.Contains("unexpected error:")) {
                    UnexpectedErrorCloseReason = msg->Reason;
                    ++UnexpectedErrorCloseSession;
                }
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

    void DropHooks() {
        auto& runtime = Runtime();
        runtime.SetEventFilter(&TTestActorRuntimeBase::DefaultFilterFunc);
        runtime.SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
        HeldPrepareEvents.clear();
    }

    void RebootPqTablet() {
        auto& runtime = Runtime();
        const auto edge = runtime.AllocateEdgeActor();
        RebootTablet(runtime, PqTabletId, edge);
        // Partition actor schedules pipe restart with RESTART_PIPE_DELAY_MS=100.
        runtime.SimulateSleep(TDuration::MilliSeconds(250));
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

    auto& runtime = env.Runtime();
    auto shutdown = NThreading::Async([&] {
        env.Server->ShutdownGRpc();
        return true;
    }, DispatchPool());
    while (!shutdown.HasValue() && !shutdown.HasException()) {
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
    }
    shutdown.GetValueSync();
    env.Server->ShutdownServer();
    env.Server.reset();
}

} // namespace

Y_UNIT_TEST_SUITE(TDirectReadRestoreRaceTest) {

// LOGBROKER-10590: forget-first after nested pipe restart with RestoredDirectReadId==0
// must not kill the partition actor (Forget stage must tolerate RestoredDirectReadId==0).
Y_UNIT_TEST(RestoredDirectReadIdZeroOnForgetAfterDoubleRestart) {
    TDirectReadRestoreEnv env;
    env.Start();
    auto& runtime = env.Runtime();

    RunWithDispatch(runtime, [&] {
        TDriver driver(MakeAsyncDriverConfig(env.Endpoint));
        auto writer = CreateSimpleWriter(driver, kTopicPath, "src", /*partitionGroup=*/{}, TString("raw"));
        if (!writer->Write(TString(1_MB, 'x'))) {
            ythrow yexception() << "write failed";
        }
        writer->Close();
        driver.Stop(true);
        return true;
    });

    TGrpcDirectReadClient client;
    client.Connect(env.Endpoint);
    client.InitControlSession(runtime);
    client.AcceptAssign(runtime);
    client.InitDirectSession(runtime);
    client.StartDirectReadPartition(runtime);
    client.ReadDataNoAck(runtime, /*expectedDirectReadId=*/1);

    env.HoldRestorePreparePublish.store(1);
    env.RebootPqTablet();

    WaitUntil(runtime, [&] {
        return env.HeldPrepareOrPublish.load() > 0;
    });
    UNIT_ASSERT_C(env.HeldPrepareOrPublish.load() > 0,
        "expected dropped CmdPrepareReadResult/CmdPublishReadResult during restore");

    client.SendDirectReadAckNoWait(runtime, 1);
    runtime.SimulateSleep(TDuration::MilliSeconds(50));

    env.RebootPqTablet();
    // Allow Session→Forget path to run; do not hold Forget responses.
    env.HoldRestorePreparePublish.store(0);
    runtime.SimulateSleep(TDuration::Seconds(1));

    UNIT_ASSERT_C(env.UnexpectedErrorCloseSession.load() == 0,
        "Forget after nested restart must not kill partition actor via unhandled exception"
            << "; held=" << env.HeldPrepareOrPublish.load()
            << "; reason=" << env.UnexpectedErrorCloseReason);

    TearDownGrpcAndServer(env, client);
}

// LOGBROKER-10590: after pipe restart with RequestInfly, ResendRecentRequests re-sends Prepare;
// a late/duplicate CmdPrepareReadResult on the normal path must be ignored (not ENSURE).
Y_UNIT_TEST(RequestInflyOnDuplicatePrepareAfterPipeRestart) {
    TDirectReadRestoreEnv env;
    env.Start();
    auto& runtime = env.Runtime();

    RunWithDispatch(runtime, [&] {
        TDriver driver(MakeAsyncDriverConfig(env.Endpoint));
        auto writer = CreateSimpleWriter(driver, kTopicPath, "src", /*partitionGroup=*/{}, TString("raw"));
        if (!writer->Write(TString(1_MB, 'x'))) {
            ythrow yexception() << "write failed";
        }
        writer->Close();
        driver.Stop(true);
        return true;
    });

    TGrpcDirectReadClient client;
    client.Connect(env.Endpoint);
    client.InitControlSession(runtime);

    // Hold Prepare before partition starts reading, so RequestInfly stays true across reboot.
    env.HoldPrepareResponses.store(1);

    client.AcceptAssign(runtime);
    client.InitDirectSession(runtime);
    client.StartDirectReadPartition(runtime);

    WaitUntil(runtime, [&] {
        return env.HeldPrepareResponses.load() >= 1;
    });
    UNIT_ASSERT_C(env.HeldPrepareResponses.load() >= 1,
        "expected at least one held CmdPrepareReadResult before reboot");

    env.RebootPqTablet();

    // Restore of empty DirectReadResults finishes quickly → ResendRecentRequests → second Prepare.
    WaitUntil(runtime, [&] {
        return env.HeldPrepareResponses.load() >= 2;
    });
    UNIT_ASSERT_C(env.HeldPrepareResponses.load() >= 2,
        "expected held Prepare from original read and from ResendRecentRequests after restore"
            << "; held=" << env.HeldPrepareResponses.load());

    env.ReleaseHeldPrepares();
    runtime.SimulateSleep(TDuration::Seconds(1));

    UNIT_ASSERT_C(env.UnexpectedErrorCloseSession.load() == 0,
        "duplicate Prepare after pipe restart must not kill partition actor via unhandled exception"
            << "; held=" << env.HeldPrepareResponses.load()
            << "; reason=" << env.UnexpectedErrorCloseReason);

    TearDownGrpcAndServer(env, client);
}

// LOGBROKER-10590: nested pipe restart re-sends restore Prepare for the same DirectReadId;
// a late Prepare after restore has moved to Publish must be ignored (not ENSURE).
Y_UNIT_TEST(HasCmdPublishReadResultOnPrepareDuringPublishRestore) {
    TDirectReadRestoreEnv env;
    env.Start();
    auto& runtime = env.Runtime();

    RunWithDispatch(runtime, [&] {
        TDriver driver(MakeAsyncDriverConfig(env.Endpoint));
        auto writer = CreateSimpleWriter(driver, kTopicPath, "src", /*partitionGroup=*/{}, TString("raw"));
        if (!writer->Write(TString(1_MB, 'x'))) {
            ythrow yexception() << "write failed";
        }
        writer->Close();
        driver.Stop(true);
        return true;
    });

    TGrpcDirectReadClient client;
    client.Connect(env.Endpoint);
    client.InitControlSession(runtime);
    client.AcceptAssign(runtime);
    client.InitDirectSession(runtime);
    client.StartDirectReadPartition(runtime);
    client.ReadDataNoAck(runtime, /*expectedDirectReadId=*/1);

    // Hold Prepare (and Publish) so nested restart can queue a second real Prepare
    // before either response is delivered — same shape as late pipe replies in prod.
    env.HoldPrepareResponses.store(1);
    env.HoldPublishResponses.store(1);

    env.RebootPqTablet();

    WaitUntil(runtime, [&] {
        return env.HeldPrepareResponses.load() >= 1;
    });
    UNIT_ASSERT_C(env.HeldPrepareResponses.load() >= 1,
        "expected held Prepare from first restore attempt");

    env.RebootPqTablet();

    WaitUntil(runtime, [&] {
        return env.HeldPrepareResponses.load() >= 2;
    });
    UNIT_ASSERT_C(env.HeldPrepareResponses.load() >= 2,
        "expected two held Prepares from nested restore attempts"
            << "; held=" << env.HeldPrepareResponses.load());

    env.ReleaseHeldPrepares();
    runtime.SimulateSleep(TDuration::Seconds(1));

    UNIT_ASSERT_C(env.UnexpectedErrorCloseSession.load() == 0,
        "late Prepare during Publish restore must not kill partition actor via unhandled exception"
            << "; heldPrepare=" << env.HeldPrepareResponses.load()
            << "; heldPublish=" << env.HeldPublishResponses.load()
            << "; reason=" << env.UnexpectedErrorCloseReason);

    TearDownGrpcAndServer(env, client);
}

} // Y_UNIT_TEST_SUITE(TDirectReadRestoreRaceTest)

} // namespace NKikimr::NPersQueueTests
