#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
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

// With UseRealThreads=false the gRPC server is only served while DispatchEvents is
// pumped, and SDK ListEndpoints (Sync/Async discovery) never completes — StreamRead
// never opens and the read session hangs silently. EDiscoveryMode::Off routes RPCs
// straight to the configured endpoint (like CreateSimpleWriter's
// ClusterDiscoveryMode::Off), so StreamRead starts without any discovery round-trip.
TDriverConfig MakeNoDiscoveryDriverConfig(const TString& endpoint) {
    TDriverConfig config;
    config.SetEndpoint(endpoint);
    config.SetDatabase("/Root");
    config.SetAuthToken("root@builtin");
    config.SetDiscoveryMode(EDiscoveryMode::Off);
    return config;
}

NKikimrSchemeOp::TPersQueueGroupDescription NavigatePqGroup(
        ::NPersQueue::TTestServer& server, const TString& topicPath)
{
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
    return front.PQGroupInfo->Description;
}

ui64 ResolvePqTabletId(::NPersQueue::TTestServer& server, const TString& topicPath, ui32 partition = 0) {
    const auto& description = NavigatePqGroup(server, topicPath);
    for (const auto& p : description.GetPartitions()) {
        if (p.GetPartitionId() == partition) {
            return p.GetTabletId();
        }
    }
    UNIT_FAIL("partition not found");
    return 0;
}

ui64 ResolvePqrbTabletId(::NPersQueue::TTestServer& server, const TString& topicPath) {
    const ui64 tabletId = NavigatePqGroup(server, topicPath).GetBalancerTabletID();
    UNIT_ASSERT_C(tabletId != 0, "balancer tablet id is zero");
    return tabletId;
}

struct TDirectReadRestoreEnv {
    std::unique_ptr<::NPersQueue::TTestServer> Server;
    TString Endpoint;
    ui64 PqTabletId = 0;
    ui64 PqrbTabletId = 0;

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

    // Hold TEvRegisterDirectReadSession (PQ → dread cache) to model late Register after re-lock.
    std::atomic<ui64> HoldRegisterDirectRead{0};
    std::atomic<ui64> HeldRegisterDirectRead{0};
    TVector<THolder<IEventHandle>> HeldRegisterDirectReadEvents;
    // Stage/Publish toward cache while Register is held (buffered until Register).
    std::atomic<ui64> StageOrPublishWhileRegisterHeld{0};
    std::atomic<ui64> StageWhileRegisterHeld{0};
    std::atomic<ui64> PublishWhileRegisterHeld{0};
    std::atomic<ui32> LastStageGenWhileRegisterHeld{0};
    std::atomic<ui32> LastPublishGenWhileRegisterHeld{0};

    // Hold TEvStageDirectReadData after Stage(M) is buffered so Stage(N) cannot upgrade pending.
    std::atomic<ui64> HoldStageDirectRead{0};
    std::atomic<ui64> HeldStageDirectRead{0};
    TVector<THolder<IEventHandle>> HeldStageDirectReadEvents;

    // Hold TEvDeregisterDirectReadSession so Stage(M) is not wiped by MarkSessionRetired on PQ
    // reboot before Publish(N) lands (delayed teardown vs new-gen Publish).
    std::atomic<ui64> HoldDeregisterDirectRead{0};
    std::atomic<ui64> HeldDeregisterDirectRead{0};
    TVector<THolder<IEventHandle>> HeldDeregisterDirectReadEvents;

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
            TDriver driver(MakeNoDiscoveryDriverConfig(Endpoint));
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
        PqrbTabletId = ResolvePqrbTabletId(*Server, kTopicPath);
    }

    void InstallHooks() {
        auto& runtime = Runtime();

        runtime.SetEventFilter([this](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (HoldRegisterDirectRead.load()
                    && ev->CastAsLocal<TEvPQ::TEvRegisterDirectReadSession>()) {
                ++HeldRegisterDirectRead;
                HeldRegisterDirectReadEvents.emplace_back(ev.Release());
                return true;
            }
            if (HoldStageDirectRead.load()
                    && ev->CastAsLocal<TEvPQ::TEvStageDirectReadData>()) {
                ++HeldStageDirectRead;
                HeldStageDirectReadEvents.emplace_back(ev.Release());
                return true;
            }
            if (HoldDeregisterDirectRead.load()
                    && ev->CastAsLocal<TEvPQ::TEvDeregisterDirectReadSession>()) {
                ++HeldDeregisterDirectRead;
                HeldDeregisterDirectReadEvents.emplace_back(ev.Release());
                return true;
            }

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
            // While Register is delayed, Stage/Publish still reach the cache and are buffered
            // until Register — tablet DirectRead state may already have advanced.
            if (HoldRegisterDirectRead.load() && !HeldRegisterDirectReadEvents.empty()) {
                if (auto* stage = ev->CastAsLocal<TEvPQ::TEvStageDirectReadData>()) {
                    ++StageOrPublishWhileRegisterHeld;
                    ++StageWhileRegisterHeld;
                    LastStageGenWhileRegisterHeld.store(stage->TabletGeneration);
                } else if (auto* publish = ev->CastAsLocal<TEvPQ::TEvPublishDirectRead>()) {
                    ++StageOrPublishWhileRegisterHeld;
                    ++PublishWhileRegisterHeld;
                    LastPublishGenWhileRegisterHeld.store(publish->TabletGeneration);
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

    void ReleaseHeldRegisterDirectRead() {
        HoldRegisterDirectRead.store(0);
        auto& runtime = Runtime();
        for (auto& ev : HeldRegisterDirectReadEvents) {
            runtime.Send(ev.Release(), /*senderNodeIndex=*/0, /*viaActorSystem=*/true);
        }
        HeldRegisterDirectReadEvents.clear();
    }

    // Release only max-generation Register so Flush sees pending Stage(M)+Publish(N>M)
    // without first applying Stage via an older Register(M).
    void ReleaseHeldRegisterDirectReadHighestGenOnly() {
        HoldRegisterDirectRead.store(0);
        auto& runtime = Runtime();
        ui32 maxGen = 0;
        for (const auto& ev : HeldRegisterDirectReadEvents) {
            if (const auto* reg = ev->Get<TEvPQ::TEvRegisterDirectReadSession>()) {
                maxGen = Max(maxGen, reg->Generation);
            }
        }
        for (auto& ev : HeldRegisterDirectReadEvents) {
            const auto* reg = ev->Get<TEvPQ::TEvRegisterDirectReadSession>();
            if (reg && reg->Generation == maxGen) {
                runtime.Send(ev.Release(), /*senderNodeIndex=*/0, /*viaActorSystem=*/true);
            }
        }
        HeldRegisterDirectReadEvents.clear();
    }

    void ReleaseHeldStages() {
        HoldStageDirectRead.store(0);
        auto& runtime = Runtime();
        for (auto& ev : HeldStageDirectReadEvents) {
            runtime.Send(ev.Release(), /*senderNodeIndex=*/0, /*viaActorSystem=*/true);
        }
        HeldStageDirectReadEvents.clear();
    }

    void ReleaseHeldDeregisters() {
        HoldDeregisterDirectRead.store(0);
        auto& runtime = Runtime();
        for (auto& ev : HeldDeregisterDirectReadEvents) {
            runtime.Send(ev.Release(), /*senderNodeIndex=*/0, /*viaActorSystem=*/true);
        }
        HeldDeregisterDirectReadEvents.clear();
    }

    void DropHooks() {
        auto& runtime = Runtime();
        runtime.SetEventFilter(&TTestActorRuntimeBase::DefaultFilterFunc);
        runtime.SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
        HeldPrepareEvents.clear();
        HeldPublishEvents.clear();
        HeldForgetEvents.clear();
        HeldRegisterDirectReadEvents.clear();
        HeldStageDirectReadEvents.clear();
        HeldDeregisterDirectReadEvents.clear();
    }

    void RebootPqTablet() {
        auto& runtime = Runtime();
        const auto edge = runtime.AllocateEdgeActor();
        RebootTablet(runtime, PqTabletId, edge);
        // Callers WaitUntil for restore progress; DispatchEvents advances RESTART_PIPE_DELAY_MS.
    }

    void RebootPqrbTablet() {
        auto& runtime = Runtime();
        const auto edge = runtime.AllocateEdgeActor();
        RebootTablet(runtime, PqrbTabletId, edge);
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

    void SendReadRequest(NActors::TTestActorRuntime& runtime, ui64 bytesSize = 100_KB) {
        RunWithDispatch(runtime, [&] {
            StreamReadMessage::FromClient readReq;
            readReq.mutable_read_request()->set_bytes_size(bytesSize);
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

    void StartDirectReadPartition(NActors::TTestActorRuntime& runtime, ui64 generation) {
        RunWithDispatch(runtime, [&] {
            StreamDirectReadMessage::FromClient req;
            auto& start = *req.mutable_start_direct_read_partition_session_request();
            start.set_partition_session_id(AssignId);
            start.set_generation(generation);
            // last_direct_read_id=0 → client has not advanced; cache should start from direct_read_id=1.
            start.set_last_direct_read_id(0);
            DR_ENSURE(DirectStream->Write(req));

            StreamDirectReadMessage::FromServer resp;
            DR_ENSURE(DirectStream->Read(&resp));
            if (resp.status() != Ydb::StatusIds::SUCCESS
                    || resp.server_message_case() != StreamDirectReadMessage::FromServer::kStartDirectReadPartitionSessionResponse) {
                ythrow yexception() << "start direct partition failed: " << resp.ShortDebugString();
            }
            Generation = generation;
            return true;
        });
    }

    void StartDirectReadPartition(NActors::TTestActorRuntime& runtime) {
        StartDirectReadPartition(runtime, Generation);
    }

    // After tablet reboot, RegisterDirectReadSession(newGen) destroys the old cache client
    // and dread cache sends StopDirectReadPartitionSession on the data stream.
    void ExpectStopDirectRead(NActors::TTestActorRuntime& runtime) {
        RunWithDispatch(runtime, [&] {
            StreamDirectReadMessage::FromServer resp;
            DR_ENSURE(DirectStream->Read(&resp));
            if (resp.server_message_case() != StreamDirectReadMessage::FromServer::kStopDirectReadPartitionSession) {
                ythrow yexception() << "expected StopDirectReadPartitionSession, got "
                                    << resp.ShortDebugString();
            }
            if (resp.stop_direct_read_partition_session().partition_session_id()
                    != static_cast<i64>(AssignId)) {
                ythrow yexception() << "StopDirectRead for unexpected partition_session_id: "
                                    << resp.ShortDebugString();
            }
            return true;
        });
    }

    ui64 ReadUpdatePartitionSession(NActors::TTestActorRuntime& runtime) {
        return RunWithDispatch(runtime, [&] {
            StreamReadMessage::FromServer resp;
            DR_ENSURE(ControlStream->Read(&resp));
            if (resp.server_message_case() != StreamReadMessage::FromServer::kUpdatePartitionSession) {
                ythrow yexception() << "expected UpdatePartitionSession, got "
                                    << resp.ShortDebugString();
            }
            if (resp.update_partition_session().partition_session_id()
                    != static_cast<i64>(AssignId)) {
                ythrow yexception() << "UpdatePartitionSession for unexpected partition_session_id: "
                                    << resp.ShortDebugString();
            }
            const ui64 newGen = resp.update_partition_session().partition_location().generation();
            Generation = newGen;
            return newGen;
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

    // Same as ReadDataNoAck but accepts any direct_read_id (new partition session after re-lock
    // may have already bumped the id while Register was delayed).
    void ReadNextDataNoAck(NActors::TTestActorRuntime& runtime) {
        RunWithDispatch(runtime, [&] {
            StreamDirectReadMessage::FromServer resp;
            DR_ENSURE(DirectStream->Read(&resp));
            if (resp.status() != Ydb::StatusIds::SUCCESS
                    || resp.server_message_case() != StreamDirectReadMessage::FromServer::kDirectReadResponse
                    || resp.direct_read_response().partition_session_id() != static_cast<i64>(AssignId)
                    || resp.direct_read_response().direct_read_id() < 1) {
                ythrow yexception() << "unexpected DirectReadResponse: " << resp.ShortDebugString();
            }
            return true;
        });
    }

    // Non-blocking-ish: pump until DirectReadResponse or deadline. Returns false on timeout.
    // On timeout cancels the DirectRead stream and waits up to 20s for the async Read to finish
    // so it does not leak a pool thread or touch DirectStream during fixture teardown.
    // Async errors are rethrown (not masked as timeout).
    bool TryReadNextDataNoAck(NActors::TTestActorRuntime& runtime, TDuration timeout) {
        auto future = NThreading::Async([&] {
            StreamDirectReadMessage::FromServer resp;
            if (!DirectStream->Read(&resp)) {
                return false;
            }
            return resp.status() == Ydb::StatusIds::SUCCESS
                && resp.server_message_case() == StreamDirectReadMessage::FromServer::kDirectReadResponse
                && resp.direct_read_response().partition_session_id() == static_cast<i64>(AssignId)
                && resp.direct_read_response().direct_read_id() >= 1;
        }, DispatchPool());

        const TInstant deadline = TInstant::Now() + timeout;
        while (TInstant::Now() < deadline && !future.HasValue() && !future.HasException()) {
            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
        }
        const bool timedOut = !future.HasValue() && !future.HasException();
        if (timedOut) {
            if (DirectContext) {
                DirectContext->TryCancel();
            }
            const TInstant cancelDeadline = TInstant::Now() + TDuration::Seconds(20);
            while (TInstant::Now() < cancelDeadline && !future.HasValue() && !future.HasException()) {
                runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
            }
            if (!future.HasValue() && !future.HasException()) {
                ythrow yexception() << "DirectRead cancel did not finish within 20s";
            }
        }
        future.TryRethrow();
        if (timedOut) {
            return false;
        }
        return future.GetValueSync();
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

    // After PQRB death ProcessBalancerDead sends forceful Stop (graceful=false) and drops the
    // partition without waiting for the client ack.
    void ExpectForcefulStopAndConfirm(NActors::TTestActorRuntime& runtime) {
        RunWithDispatch(runtime, [&] {
            StreamReadMessage::FromServer resp;
            DR_ENSURE(ControlStream->Read(&resp));
            if (resp.server_message_case()
                    != StreamReadMessage::FromServer::kStopPartitionSessionRequest) {
                ythrow yexception() << "expected StopPartitionSessionRequest, got "
                                    << resp.ShortDebugString();
            }
            const auto& stop = resp.stop_partition_session_request();
            if (stop.graceful()) {
                ythrow yexception() << "expected forceful Stop (graceful=false), got "
                                    << resp.ShortDebugString();
            }
            if (stop.partition_session_id() != static_cast<i64>(AssignId)) {
                ythrow yexception() << "Stop for unexpected partition_session_id: "
                                    << resp.ShortDebugString();
            }

            StreamReadMessage::FromClient req;
            auto& reply = *req.mutable_stop_partition_session_response();
            reply.set_partition_session_id(AssignId);
            reply.set_graceful(false);
            DR_ENSURE(ControlStream->Write(req));
            return true;
        });
    }

    // StartDirectRead while dread cache has no server session → BAD_REQUEST "Unknown session".
    // Closes the DirectRead stream (same as prod SDK path that remaps to OVERLOADED and retries).
    void StartDirectReadExpectUnknownSession(NActors::TTestActorRuntime& runtime) {
        RunWithDispatch(runtime, [&] {
            StreamDirectReadMessage::FromClient req;
            auto& start = *req.mutable_start_direct_read_partition_session_request();
            start.set_partition_session_id(AssignId);
            start.set_generation(Generation);
            start.set_last_direct_read_id(0);
            DR_ENSURE(DirectStream->Write(req));

            StreamDirectReadMessage::FromServer resp;
            DR_ENSURE(DirectStream->Read(&resp));
            if (resp.status() == Ydb::StatusIds::SUCCESS
                    && resp.server_message_case()
                        == StreamDirectReadMessage::FromServer::kStartDirectReadPartitionSessionResponse) {
                ythrow yexception() << "expected Unknown session close, got success: "
                                    << resp.ShortDebugString();
            }
            if (resp.status() != Ydb::StatusIds::BAD_REQUEST) {
                ythrow yexception() << "expected BAD_REQUEST Unknown session, got "
                                    << resp.ShortDebugString();
            }
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
            TDriver driver(MakeNoDiscoveryDriverConfig(Env.Endpoint));
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

    // While restore is stuck: no UpdateSession, no new DirectRead publish, control stays alive.
    // Models the client hang window after StopDirectRead and before UpdatePartitionSession.
    void AssertHangWindowWhileRestoreStuck(ui64 updateBefore, ui64 directReadBefore, TDuration duration) {
        const TInstant deadline = TInstant::Now() + duration;
        while (TInstant::Now() < deadline) {
            Runtime().DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
            UNIT_ASSERT_VALUES_EQUAL_C(Env.UpdateSessionCount.load(), updateBefore,
                "UpdateSession must not arrive while restore Prepare is held");
            UNIT_ASSERT_VALUES_EQUAL_C(Env.DirectReadResponseCount.load(), directReadBefore,
                "no new DirectRead publish while restore Prepare is held");
            AssertNoErrorClose("control session must stay alive while restore is stuck");
        }
    }

    // After PQRB reboot with Register held: control stays up (client hang shape from the incident).
    // Do not assert on DirectReadResponseCount — partition may still Publish toward the cache
    // after CreateSession even while Register delivery is delayed.
    void AssertHangWindowAfterPqrbUnknownSession(TDuration duration) {
        const TInstant deadline = TInstant::Now() + duration;
        while (TInstant::Now() < deadline) {
            Runtime().DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
            UNIT_ASSERT_C(Env.HoldRegisterDirectRead.load() != 0
                    && !Env.HeldRegisterDirectReadEvents.empty(),
                "RegisterDirectReadSession must stay held during the hang window");
            AssertNoErrorClose(
                "control session must stay alive after PQRB restart / Unknown session on DirectRead");
        }
    }

    // Pump actor system until promise is set or timeout. UseRealThreads=false requires DispatchEvents.
    bool WaitPromise(NThreading::TPromise<void>& promise, TDuration timeout) {
        const TInstant deadline = TInstant::Now() + timeout;
        while (TInstant::Now() < deadline) {
            if (promise.HasValue()) {
                return true;
            }
            TDispatchOptions opts;
            opts.CustomFinalCondition = [&] {
                return promise.HasValue();
            };
            Runtime().DispatchEvents(opts, TDuration::MilliSeconds(50));
        }
        return promise.HasValue();
    }

    // Sparse pump + wall-clock sleep. Use when hanging is expected: avoids drowning in idle
    // tablet timers while SDK threads still get occasional progress.
    bool WaitPromiseSparse(NThreading::TPromise<void>& promise, TDuration timeout) {
        const TInstant deadline = TInstant::Now() + timeout;
        while (TInstant::Now() < deadline) {
            if (promise.HasValue()) {
                return true;
            }
            Runtime().DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
            Sleep(TDuration::MilliSeconds(100));
        }
        return promise.HasValue();
    }

    // Write under a reconnect storm without RunWithDispatch melting the actor queue.
    void WriteOneMessageSparse() {
        auto future = NThreading::Async([&] {
            TDriver driver(MakeNoDiscoveryDriverConfig(Env.Endpoint));
            auto writer = CreateSimpleWriter(driver, kTopicPath, "src", /*partitionGroup=*/{}, TString("raw"));
            if (!writer->Write(TString(1_MB, 'x'))) {
                ythrow yexception() << "sparse write failed";
            }
            writer->Close();
            driver.Stop(true);
            return true;
        }, DispatchPool());

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(60);
        while (TInstant::Now() < deadline && !future.HasValue() && !future.HasException()) {
            Runtime().DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(future.HasValue(), "sparse write did not finish in time");
        future.GetValueSync();
    }

    // Sparse wait used by PQRB+Register hang tests under UseRealThreads=false.
    void WaitHeldRegisterSparse(ui64 minHeld = 1, TDuration timeout = TDuration::Seconds(30)) {
        const TInstant deadline = TInstant::Now() + timeout;
        while (TInstant::Now() < deadline && Env.HeldRegisterDirectRead.load() < minHeld) {
            Runtime().DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(Env.HeldRegisterDirectRead.load() >= minHeld,
            "expected RegisterDirectReadSession after PQRB re-lock; held="
                << Env.HeldRegisterDirectRead.load());
    }

    void WaitStageOrPublishWhileRegisterHeldAtLeast(
            ui64 minCount, TDuration timeout = TDuration::Seconds(30))
    {
        const TInstant deadline = TInstant::Now() + timeout;
        while (TInstant::Now() < deadline && Env.StageOrPublishWhileRegisterHeld.load() < minCount) {
            Runtime().DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(Env.StageOrPublishWhileRegisterHeld.load() >= minCount,
            "expected Stage/Publish to cache while Register held; got="
                << Env.StageOrPublishWhileRegisterHeld.load());
    }

    void WaitStageOrPublishWhileRegisterHeldAbove(
            ui64 exclusiveMin, TDuration timeout = TDuration::Seconds(30))
    {
        const TInstant deadline = TInstant::Now() + timeout;
        while (TInstant::Now() < deadline
                && Env.StageOrPublishWhileRegisterHeld.load() <= exclusiveMin) {
            Runtime().DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(Env.StageOrPublishWhileRegisterHeld.load() > exclusiveMin,
            "expected further Stage/Publish while Register held; before="
                << exclusiveMin
                << "; after=" << Env.StageOrPublishWhileRegisterHeld.load());
    }

    void AssertRegisterStillHeld(const TString& what) {
        UNIT_ASSERT_C(Env.HoldRegisterDirectRead.load() != 0
                && !Env.HeldRegisterDirectReadEvents.empty(),
            what);
    }

    void AssertStageStillHeld(const TString& what) {
        UNIT_ASSERT_C(Env.HoldStageDirectRead.load() != 0
                && !Env.HeldStageDirectReadEvents.empty(),
            what);
    }

    void WaitHeldStageSparse(ui64 minHeld = 1, TDuration timeout = TDuration::Seconds(30)) {
        const TInstant deadline = TInstant::Now() + timeout;
        while (TInstant::Now() < deadline && Env.HeldStageDirectRead.load() < minHeld) {
            Runtime().DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(Env.HeldStageDirectRead.load() >= minHeld,
            "expected Stage held while Register delayed; held="
                << Env.HeldStageDirectRead.load());
    }

    void WaitStageBufferedWhileRegisterHeld(ui64 minCount = 1, TDuration timeout = TDuration::Seconds(30)) {
        const TInstant deadline = TInstant::Now() + timeout;
        while (TInstant::Now() < deadline && Env.StageWhileRegisterHeld.load() < minCount) {
            Runtime().DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(Env.StageWhileRegisterHeld.load() >= minCount,
            "expected Stage buffered while Register held; got="
                << Env.StageWhileRegisterHeld.load());
    }

    void WaitHigherGenPublishWhileRegisterHeld(
            ui32 minPublishGenExclusive, TDuration timeout = TDuration::Seconds(30))
    {
        const TInstant deadline = TInstant::Now() + timeout;
        while (TInstant::Now() < deadline) {
            if (Env.PublishWhileRegisterHeld.load() >= 1
                    && Env.LastPublishGenWhileRegisterHeld.load() > minPublishGenExclusive) {
                return;
            }
            Runtime().DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(false,
            "expected Publish with gen > " << minPublishGenExclusive
                << " while Register held; publishCount="
                << Env.PublishWhileRegisterHeld.load()
                << "; lastPublishGen=" << Env.LastPublishGenWhileRegisterHeld.load()
                << "; lastStageGen=" << Env.LastStageGenWhileRegisterHeld.load()
                << "; heldDeregister=" << Env.HeldDeregisterDirectRead.load());
    }

    void StopSdkSparse(
            std::shared_ptr<TDriver>& driver,
            std::shared_ptr<NTopic::IReadSession>& reader)
    {
        if (!driver) {
            return;
        }
        // Capture by value so Close/Stop outlive caller pointer resets.
        const auto driverLocal = driver;
        const auto readerLocal = reader;
        auto stop = NThreading::Async([driverLocal, readerLocal] {
            if (readerLocal) {
                readerLocal->Close(TDuration::MilliSeconds(50));
            }
            driverLocal->Stop(true);
            return true;
        }, DispatchPool());
        // ~20s window so WaitCallbacksDrained spin logs can show stuck InFlight count.
        for (int i = 0; i < 400 && !stop.HasValue() && !stop.HasException(); ++i) {
            Runtime().DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(stop.HasValue() || stop.HasException(),
            "SDK sparse stop did not finish in time");
        stop.TryRethrow();
        stop.GetValueSync();
        reader.reset();
        driver.reset();
    }
};

} // namespace

Y_UNIT_TEST_SUITE_F(TDirectReadRestoreRaceTest, TDirectReadRestoreFixture) {

// LOGBROKER-10590 / cache-generation hang hypothesis:
// tablet reboot → RegisterDirectReadSession(newGen) destroys old cache client →
// StopDirectReadPartitionSession on data path; while restore Prepare is held,
// UpdatePartitionSession does not arrive → data-path stays dead (client hang window).
// Releasing Prepare completes restore → Update → Start(newGen) → DirectReadResponse.
Y_UNIT_TEST(StopDirectReadOnGenerationBumpWhileRestoreStuck) {
    WriteOneMessage();
    OpenDirectReadSession(/*readDataNoAck=*/true);

    const ui64 updateBefore = Env.UpdateSessionCount.load();
    const ui64 directReadBefore = Env.DirectReadResponseCount.load();
    const ui64 oldGen = Client.Generation;
    UNIT_ASSERT_C(oldGen > 0, "expected non-zero partition generation after assign");

    RebootAndWaitHeldPrepares(/*minHeld=*/1);

    // Hypothesis A: dread cache kills the old client binding on generation bump.
    Client.ExpectStopDirectRead(Runtime());

    // Hypothesis B: without UpdatePartitionSession the data path stays silent.
    AssertHangWindowWhileRestoreStuck(updateBefore, directReadBefore, TDuration::Seconds(2));

    // Recovery: finish restore → UpdatePartitionSession with new generation → rebind.
    ReleaseAndWaitUpdateSession(
        [&] { Env.ReleaseHeldPrepares(); },
        "releasing Prepare after StopDirectRead hang window must complete restore");

    const ui64 newGen = Client.ReadUpdatePartitionSession(Runtime());
    UNIT_ASSERT_C(newGen > oldGen,
        "UpdatePartitionSession generation must advance after tablet reboot"
            << "; oldGen=" << oldGen << "; newGen=" << newGen);

    Client.StartDirectReadPartition(Runtime(), newGen);
    Client.ReadDataNoAck(Runtime(), /*expectedDirectReadId=*/1);
}

// PQRB restart hang (prod: control alive, DirectRead Unknown session / BytesRead=0):
// ProcessBalancerDead force-stops partitions and re-locks; CreateSession fires
// RegisterDirectReadSession to dread cache before Start reaches the client.
// If Register is delayed, partition still Stage/Publish and advances inFlight DirectRead;
// client StartDirectRead → Unknown session while Register is held.
// Without buffering, dropped Stage/Publish left tablet inFlight without cache data → durable
// hang after Register and re-init. With buffering, FlushPending on Register restores data.
Y_UNIT_TEST(UnknownSessionAfterPqrbRestartWhileRegisterHeld) {
    WriteOneMessage();
    OpenDirectReadSession(/*readDataNoAck=*/true);

    const ui64 oldAssignId = Client.AssignId;

    // Hold only new Registers after re-lock; the first session is already in the cache.
    Env.HoldRegisterDirectRead.store(1);
    Env.RebootPqrbTablet();

    Client.ExpectForcefulStopAndConfirm(Runtime());

    // Old DirectRead binding is destroyed with the dropped partition (Deregister).
    Client.ExpectStopDirectRead(Runtime());

    WaitUntil(Runtime(), [&] {
        return Env.HeldRegisterDirectRead.load() >= 1
            || Env.ErrorCloseSession.load() > 0;
    });
    AssertNoErrorClose("PQRB restart must re-lock and CreateSession without killing control");
    UNIT_ASSERT_C(Env.HeldRegisterDirectRead.load() >= 1,
        "expected RegisterDirectReadSession after PQRB re-lock; held="
            << Env.HeldRegisterDirectRead.load());

    // New Start can arrive while Register is still held (Register is fire-and-forget from PQ).
    Client.AcceptAssign(Runtime());
    UNIT_ASSERT_C(Client.AssignId != oldAssignId,
        "expected a new partition_session_id after PQRB re-lock"
            << "; old=" << oldAssignId << "; new=" << Client.AssignId);

    Client.StartDirectReadExpectUnknownSession(Runtime());

    AssertHangWindowAfterPqrbUnknownSession(TDuration::Seconds(2));

    Env.ReleaseHeldRegisterDirectRead();

    // Unknown session closed the DirectRead stream; re-init like SDK Reconnect + Init.
    Client.SendReadRequest(Runtime());
    Client.InitDirectSession(Runtime());
    Client.StartDirectReadPartition(Runtime());
    WriteOneMessage();

    // After Register + re-init, buffered Stage/Publish must be visible (or fail fast on hang).
    UNIT_ASSERT_C(
        Client.TryReadNextDataNoAck(Runtime(), TDuration::Seconds(15)),
        "DirectRead hung after PQRB restart: Register was delayed, cache returned Unknown session, "
        "partition already published/inFlight DirectRead that the client never saw; "
        "control stayed alive but further DirectRead did not recover");
}

// Same scenario as UnknownSessionAfterPqrbRestartWhileRegisterHeld via Topic SDK
// (UseRealThreads=false): no Commit on first DataReceived; hold Register after PQRB reboot until
// Stage/Publish are buffered; write a second message while Register is held (unread topic data);
// release Register so SDK Start succeeds. Without buffering this hung (no further DataReceived);
// with FlushPending on Register, DataReceived must resume.
Y_UNIT_TEST(SdkHangAfterPqrbRestartWhileRegisterHeld) {
    Runtime().SetScheduledLimit(10'000'000);

    WriteOneMessage();

    auto gotFirstMessage = NThreading::NewPromise<void>();
    auto gotDataAfterRestart = NThreading::NewPromise<void>();
    std::atomic<ui64> messagesReceived{0};
    std::atomic<ui64> messagesAtRestart{0};
    std::atomic<bool> pastRestart{false};
    std::atomic<bool> sessionClosed{false};

    std::shared_ptr<TDriver> driver;
    std::shared_ptr<NTopic::IReadSession> reader;

    RunWithDispatch(Runtime(), [&] {
        driver = std::make_shared<TDriver>(MakeNoDiscoveryDriverConfig(Env.Endpoint));
        TTopicClient topicClient(*driver);

        auto settings = TReadSessionSettings()
            .ConsumerName(kConsumer)
            .AppendTopics(TTopicReadSettings(std::string(kTopicPath)))
            .DirectRead(true);

        settings.EventHandlers_
            .DataReceivedHandler([&](NTopic::TReadSessionEvent::TDataReceivedEvent& ev) {
                // Do not Commit — leave DirectRead in flight like raw readDataNoAck=true.
                const ui64 n = messagesReceived.fetch_add(ev.GetMessages().size()) + ev.GetMessages().size();
                if (n >= 1) {
                    gotFirstMessage.TrySetValue();
                }
                if (pastRestart.load() && n > messagesAtRestart.load()) {
                    gotDataAfterRestart.TrySetValue();
                }
            })
            .StartPartitionSessionHandler([&](NTopic::TReadSessionEvent::TStartPartitionSessionEvent& ev) {
                ev.Confirm();
            })
            .StopPartitionSessionHandler([&](NTopic::TReadSessionEvent::TStopPartitionSessionEvent& ev) {
                ev.Confirm();
            })
            .SessionClosedHandler([&](const NTopic::TSessionClosedEvent&) {
                sessionClosed.store(true);
            });

        reader = topicClient.CreateReadSession(settings);
        return true;
    });

    UNIT_ASSERT_C(WaitPromise(gotFirstMessage, TDuration::Seconds(30)),
        "SDK DirectRead did not receive the first message before PQRB restart");
    messagesAtRestart.store(messagesReceived.load());
    pastRestart.store(true);

    Env.HoldRegisterDirectRead.store(1);
    Env.RebootPqrbTablet();

    WaitHeldRegisterSparse();
    AssertNoErrorClose("PQRB restart must not kill the read proxy control session");
    UNIT_ASSERT_C(!sessionClosed.load(),
        "SDK read session closed during PQRB restart; expected control to stay alive");

    // Wait until PQ Stage/Publish reaches cache while Register is still held (buffered
    // until Register) — regression precondition for the unbuffered hang.
    WaitStageOrPublishWhileRegisterHeldAtLeast(1);
    AssertRegisterStillHeld("Register must still be held after buffered Stage/Publish");

    // Second message while Register is still held — topic has unread data after release, so a
    // silent wait cannot be explained by an empty partition. Stage/Publish for the new data
    // is also buffered until Register (do not write after release: that can deliver a later
    // DirectReadId and trip SDK VERIFY on NextDirectReadId gap).
    const ui64 stageBeforeSecondWrite = Env.StageOrPublishWhileRegisterHeld.load();
    WriteOneMessageSparse();
    WaitStageOrPublishWhileRegisterHeldAbove(stageBeforeSecondWrite);
    AssertRegisterStillHeld("Register must still be held after second write");

    // Register lands; FlushPending applies buffered Stage/Publish; SDK retry should deliver data.
    Env.ReleaseHeldRegisterDirectRead();

    const bool gotAfter = WaitPromiseSparse(gotDataAfterRestart, TDuration::Seconds(5));
    const bool closed = sessionClosed.load();

    // Soft stop without RunWithDispatch: dense pump melts under DirectRead reconnects.
    StopSdkSparse(driver, reader);

    UNIT_ASSERT_C(!closed,
        "SDK read session closed after PQRB restart; expected control/session to stay alive");
    UNIT_ASSERT_C(gotAfter,
        "SDK DirectRead durable hang after PQRB restart: Register was delayed, Stage/Publish "
        "buffered until Register (including second write while Register held), then Register "
        "was released and Start could succeed, but inFlight DirectRead never reached the "
        "client; session stayed open and topic had unread data, no further DataReceived");
}

// Reviewer Flush race: pending Stage(readId, gen=M) + Publish(readId, gen=N) with M < N.
// Flush(gen=N) drops Stage(M), PublishToSession finds no staged payload → hang.
// Sequence: Hold Register after PQRB → buffer Stage(M) → hold further Stage + Deregister →
// reboot PQ (gen bump) → Publish(N) while Stage(M) still pending → release Register then Stage.
// Hold Deregister models delayed teardown so MarkSessionRetired does not wipe Stage(M) before
// Publish(N) (without it, Deregister(M) clears pending Stage before the higher-gen Publish).
Y_UNIT_TEST(SdkHangWhenPublishFlushedBeforeMatchingStage) {
    Runtime().SetScheduledLimit(10'000'000);

    WriteOneMessage();

    auto gotFirstMessage = NThreading::NewPromise<void>();
    auto gotDataAfterRestart = NThreading::NewPromise<void>();
    std::atomic<ui64> messagesReceived{0};
    std::atomic<ui64> messagesAtRestart{0};
    std::atomic<bool> pastRestart{false};
    std::atomic<bool> sessionClosed{false};

    std::shared_ptr<TDriver> driver;
    std::shared_ptr<NTopic::IReadSession> reader;

    RunWithDispatch(Runtime(), [&] {
        driver = std::make_shared<TDriver>(MakeNoDiscoveryDriverConfig(Env.Endpoint));
        TTopicClient topicClient(*driver);

        auto settings = TReadSessionSettings()
            .ConsumerName(kConsumer)
            .AppendTopics(TTopicReadSettings(std::string(kTopicPath)))
            .DirectRead(true);

        settings.EventHandlers_
            .DataReceivedHandler([&](NTopic::TReadSessionEvent::TDataReceivedEvent& ev) {
                const ui64 n = messagesReceived.fetch_add(ev.GetMessages().size()) + ev.GetMessages().size();
                if (n >= 1) {
                    gotFirstMessage.TrySetValue();
                }
                if (pastRestart.load() && n > messagesAtRestart.load()) {
                    gotDataAfterRestart.TrySetValue();
                }
            })
            .StartPartitionSessionHandler([&](NTopic::TReadSessionEvent::TStartPartitionSessionEvent& ev) {
                ev.Confirm();
            })
            .StopPartitionSessionHandler([&](NTopic::TReadSessionEvent::TStopPartitionSessionEvent& ev) {
                ev.Confirm();
            })
            .SessionClosedHandler([&](const NTopic::TSessionClosedEvent&) {
                sessionClosed.store(true);
            });

        reader = topicClient.CreateReadSession(settings);
        return true;
    });

    UNIT_ASSERT_C(WaitPromise(gotFirstMessage, TDuration::Seconds(30)),
        "SDK DirectRead did not receive the first message before PQRB restart");
    messagesAtRestart.store(messagesReceived.load());
    pastRestart.store(true);

    Env.HoldRegisterDirectRead.store(1);
    Env.RebootPqrbTablet();

    WaitHeldRegisterSparse();
    WaitStageBufferedWhileRegisterHeld();
    AssertNoErrorClose("PQRB restart must not kill the read proxy control session");
    UNIT_ASSERT_C(!sessionClosed.load(),
        "SDK read session closed during PQRB restart; expected control to stay alive");

    const ui32 stageGenM = Env.LastStageGenWhileRegisterHeld.load();
    UNIT_ASSERT_C(stageGenM > 0, "expected non-zero Stage generation buffered while Register held");

    // Keep Stage(M) in pending: block Stage(N) upgrade and Deregister(M) retired cleanup.
    Env.HoldStageDirectRead.store(1);
    Env.HoldDeregisterDirectRead.store(1);
    Env.RebootPqTablet();

    WaitHigherGenPublishWhileRegisterHeld(stageGenM);
    AssertRegisterStillHeld("Register must still be held after higher-gen Publish");
    UNIT_ASSERT_C(Env.LastPublishGenWhileRegisterHeld.load() > stageGenM,
        "expected Publish gen > Stage gen; stageGen=" << stageGenM
            << "; publishGen=" << Env.LastPublishGenWhileRegisterHeld.load());
    UNIT_ASSERT_C(Env.HeldDeregisterDirectRead.load() >= 1,
        "expected Deregister held so Stage(M) was not retired before Publish(N)");

    // Unread data after release so silence cannot be explained by an empty partition.
    const ui64 publishBeforeSecondWrite = Env.PublishWhileRegisterHeld.load();
    WriteOneMessageSparse();
    {
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
        while (TInstant::Now() < deadline
                && Env.PublishWhileRegisterHeld.load() <= publishBeforeSecondWrite) {
            Runtime().DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
            Sleep(TDuration::MilliSeconds(50));
        }
        UNIT_ASSERT_C(Env.PublishWhileRegisterHeld.load() > publishBeforeSecondWrite,
            "expected further Publish while Register held after second write; before="
                << publishBeforeSecondWrite
                << "; after=" << Env.PublishWhileRegisterHeld.load());
    }
    AssertRegisterStillHeld("Register must still be held after second write");

    // Flush(gen=N): drops Stage(M), PublishToSession fails without staged payload.
    // Only the newest Register — older Register(M) would consume Stage(M) first.
    Env.ReleaseHeldRegisterDirectReadHighestGenOnly();
    Env.ReleaseHeldStages();
    Env.ReleaseHeldDeregisters();

    const bool gotAfter = WaitPromiseSparse(gotDataAfterRestart, TDuration::Seconds(5));
    const bool closed = sessionClosed.load();

    UNIT_ASSERT_C(!closed,
        "SDK read session closed after PQRB/PQ restart; expected control/session to stay alive");
    UNIT_ASSERT_C(gotAfter,
        "SDK DirectRead durable hang: pending Stage(gen=M) + Publish(gen=N>M); Flush dropped "
        "Stage(M) and failed Publish with no staged payload; later Stage left the read "
        "staged/unpublished; session stayed open with unread topic data but no further DataReceived");

    // Repro Stop hang (InFlight drain investigation): keep StopSdkSparse.
    StopSdkSparse(driver, reader);
}

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
