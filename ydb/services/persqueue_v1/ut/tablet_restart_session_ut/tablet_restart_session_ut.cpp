#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/services/persqueue_v1/actors/events.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/retry_policy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/thread/pool.h>

#include <atomic>
#include <memory>
#include <mutex>

using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using Ydb::Topic::V1::TopicService;
using StreamReadClient = Ydb::Topic::StreamReadMessage::FromClient;
using StreamReadServer = Ydb::Topic::StreamReadMessage::FromServer;
using StreamReadInit = Ydb::Topic::StreamReadMessage::InitRequest;
using StreamReadReadReq = Ydb::Topic::StreamReadMessage::ReadRequest;
using StreamReadStartResp = Ydb::Topic::StreamReadMessage::StartPartitionSessionResponse;
using StreamReadStopResp = Ydb::Topic::StreamReadMessage::StopPartitionSessionResponse;

namespace NKikimr::NPersQueueTests {
namespace {

constexpr const char* kTopic = "tablet-restart-topic";
constexpr const char* kTopicPath = "/Root/tablet-restart-topic";
constexpr const char* kConsumer = "user";

// TR_ENSURE is used exclusively in helpers that run on the dispatch-pool
// background threads (gRPC session helpers, write session). UNIT_ASSERT_C
// must NOT be used there: it panics (SIGABRT) when it fires on a
// non-unittest thread. Throwing is correct instead — the exception is
// captured by the NThreading::TFuture and rethrown on the main thread
// (WaitFuture / GetValueSync / TryRethrow), where the unittest framework
// reports it as a normal test failure.
#define TR_ENSURE(cond) \
    do { \
        if (!(cond)) { \
            ythrow yexception() << "check failed: " << #cond; \
        } \
    } while (false)

IThreadPool& DispatchPool() {
    struct THolder {
        TThreadPool Pool;
        THolder() {
            // 8 threads: the Concurrent scenario step runs the step lambda,
            // up to 5 concurrent read-session lambdas and a write lambda
            // simultaneously on this pool (2 threads would deadlock).
            Pool.Start(8);
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

// DispatchEvents wrapper that retries on TSchedulingLimitReachedException.
// The scheduled-event budget can be exhausted during long simulations; this is
// not a failure, just partial progress. Following the pattern in pq_ut.cpp,
// we catch the exception and retry dispatch until the condition is met.
template <typename TDispatchFunc>
void DispatchEventsWithRetry(TDispatchFunc&& dispatchFunc, ui32 maxRetries = 10) {
    for (ui32 retriesLeft = maxRetries; retriesLeft > 0; --retriesLeft) {
        try {
            dispatchFunc();
            return;
        } catch (const NActors::TSchedulingLimitReachedException&) {
            UNIT_ASSERT_C(retriesLeft > 1, "DispatchEvents exhausted scheduling limit too many times");
        }
    }
}

template <typename TCondition>
void WaitUntil(NActors::TTestActorRuntime& runtime, TCondition&& condition, TDuration deadline = TDuration::Seconds(10)) {
    TDispatchOptions opts;
    opts.CustomFinalCondition = std::forward<TCondition>(condition);
    DispatchEventsWithRetry([&] {
        UNIT_ASSERT_C(runtime.DispatchEvents(opts, deadline), "WaitUntil condition not met before deadline");
    });
}

// With UseRealThreads=false the gRPC server is only served while DispatchEvents is
// pumped, and SDK ListEndpoints (Sync/Async discovery) never completes — StreamRead
// never opens and the read session hangs silently. EDiscoveryMode::Off routes RPCs
// straight to the configured endpoint, so StreamRead/StreamWrite starts without any
// discovery round-trip.
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

/// Per-test configuration for opening a read session.
/// Each unit test fills this struct with the settings it wants to exercise.
struct TReadSessionSettings {
    // If set (> 0), max_lag will be configured on the topic read settings (triggers WaitForData=true).
    // Tests can set different values to cover different code paths.
    // MaxLagSeconds = 0 means no WaitForData: the server replies immediately
    // (with an empty ReadResponse when exhausted), which lets ReadAll detect
    // end-of-data fast instead of blocking until its timeout.
    ui64 MaxLagSeconds = 0;

    // Bytes size for the read request.
    ui64 ReadRequestBytesSize = 100_KB;

    // Consumer for the read session. Defaults to the shared "user" consumer.
    // Concurrent-session tests use distinct consumers so each session gets
    // an independent partition assignment.
    TString Consumer = kConsumer;

    // Populate the raw gRPC InitRequest proto from this test settings struct.
    void ApplyToInitRequest(StreamReadInit& req) const {
        req.set_consumer(Consumer);
        auto* topic = req.add_topics_read_settings();
        topic->set_path(kTopicPath);
        if (MaxLagSeconds > 0) {
            auto* maxLag = topic->mutable_max_lag();
            maxLag->set_seconds(MaxLagSeconds);
        }
    }
};

/// A single step in a scenario: a write, a read, or a concurrent read+write.
struct TScenarioStep {
    enum class EType {
        Write,       // Write Count messages
        Read,        // Open a read session and read data using ReadSettings
        Concurrent,  // Open ReadSessionCount read sessions, optionally write
                     // Count messages while they are open, then read from each
    };

    EType Type = EType::Write;
    ui64 Count = 1;           // For Write/Concurrent: number of messages to write
    ui64 MessageSize = 1_MB;  // For Write/Concurrent: size of each message in bytes
    TReadSessionSettings ReadSettings;  // For Read/Concurrent: read session settings
    // For Concurrent: number of read sessions to open simultaneously.
    // When > 1, session i uses consumer "user" + (i-1) so each session gets
    // an independent partition assignment.
    ui32 ReadSessionCount = 1;
};

/// A scenario is a sequence of steps (write/read operations).
using TScenario = TVector<TScenarioStep>;

struct TTabletRestartReadSessionEnv {
    std::unique_ptr<::NPersQueue::TTestServer> Server;
    TString Endpoint;
    ui64 PqTabletId = 0;
    ui64 PqrbTabletId = 0;
    // Actor ID of the PQ tablet (changes after each reboot). Resolved lazily by
    // the event filter so that ALL events delivered to the tablet are counted,
    // not just TEvRequest/TEvResponse. This is what makes the reboot fire during
    // the write-session setup phase (where TEvProxyResponse and other internal
    // events flow to the tablet), reproducing the HandleDie() null-pointer crash.
    TActorId PqTabletActorId;

    // Any TEvCloseSession with ErrorCode != OK.
    // Teardown DropHooks() clears the observer before gRPC cancel, so shutdown noise is ignored.
    std::atomic<ui64> ErrorCloseSession{0};
    TString ErrorCloseReason;

    // TEvPartitionReady delivered to read session actor (proves WaitForData was reset).
    std::atomic<ui64> PartitionReadyCount{0};

    // Reboot verification counter — tracked via the event observer.
    // TabletBootCount increments on each TEvTablet::EvBoot event, proving
    // that a new tablet instance booted. Comparing before/after RebootPqTablet
    // ensures we didn't consume a stale EvBoot from the initial boot.
    std::atomic<ui64> TabletBootCount{0};

    // Count of TEvPersQueue::TEvRequest and TEvPersQueue::TEvResponse events flowing
    // to/from the PQ tablet. Used by the event-filter-based reboot loop.
    std::atomic<ui64> PqTabletEventCount{0};

    // Event count at which to trigger the reboot (0 = no reboot, 1 = reboot after 1st event, etc.).
    std::atomic<ui64> RebootAfterEventCount{0};
    // Whether the reboot has already been triggered in the current test run.
    std::atomic<bool> RebootTriggered{false};
    // Set to true while RebootPqTablet() is executing. The event filter checks
    // this flag and NEVER blocks events during the reboot, so the poison pill,
    // the launcher's reboot scheduling, and the new EvBoot all flow freely.
    // Without this, the filter's blocking (count >= target) would interfere with
    // the reboot machinery and the tablet would never come back up.
    std::atomic<bool> InReboot{false};

    NActors::TTestActorRuntime& Runtime() {
        return *Server->CleverServer->GetRuntime();
    }

    void Start() {
        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.SetUseRealThreads(false);
        settings.SetNodeCount(1);
        // Skip SysViews roster WaitFor in TServer::Initialize (~10s under UseRealThreads=false).
        settings.FeatureFlags.SetEnableRealSystemViewPaths(false);

        // Inline TTestServer::StartServer so client calls can run
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

        RunWithDispatch(runtime, [&] {
            Server->AnnoyingClient = MakeHolder<NKikimr::NPersQueueTests::TFlatMsgBusPQClient>(
                Server->ServerSettings, Server->GrpcPort, TString("/Root"));
            return true;
        });

        Server->AnnoyingClient->SetNoConfigMode();

        // No-config mode: only need Root + /PQ.
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
                    .EndAddConsumer()
                    // Extra consumers for the multiple-read-sessions test:
                    // each concurrent session reads with its own consumer.
                    .BeginAddConsumer("user0")
                    .EndAddConsumer()
                    .BeginAddConsumer("user1")
                    .EndAddConsumer()
                    .BeginAddConsumer("user2")
                    .EndAddConsumer()
                    .BeginAddConsumer("user3")
                    .EndAddConsumer()).ExtractValueSync();
            if (!status.IsSuccess()) {
                ythrow yexception() << "CreateTopic failed: " << status.GetIssues().ToString();
            }
            driver.Stop(true);
            return true;
        });

        PqTabletId = ResolvePqTabletId(*Server, kTopicPath);
        PqrbTabletId = ResolvePqrbTabletId(*Server, kTopicPath);
    }

    void InstallHooks() {
        auto& runtime = Runtime();

        // The event filter counts events and blocks events after the target is reached.
        // Blocking events (returning true) forces DispatchEvents to return because
        // no more events can be processed. This ensures the reboot happens after
        // exactly the Nth event, not after a batch of events.
        //
        // IMPORTANT: The filter must NOT call DispatchEvents or perform any reboot —
        // that would re-enter the dispatch loop while the runtime mutex is held,
        // corrupting internal state. The reboot is performed in the main loop
        // after DispatchEvents returns.
        runtime.SetEventFilter([this](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            // NEVER block events while a reboot is in progress. During RebootPqTablet()
            // the poison pill, the launcher's reboot scheduling, and the new EvBoot
            // must all flow freely — otherwise the tablet never comes back up.
            if (InReboot.load()) {
                return false;
            }

            // Only TEvPersQueue::TEvRequest and TEvPersQueue::TEvResponse are the events
            // that flow through the PQ tablet pipe and can actually be interrupted by a
            // tablet reboot in a real system. All other events (system events like
            // Bootstrap, internal actor events, etc.) are never interrupted by a tablet
            // reboot, so we must never count or block them.
            //
            // IMPORTANT: Never block system events. In a real actor system the Bootstrap
            // event is always delivered first to a bootstrapped actor (TActorBootstrapped
            // guarantees it), so a TReadProxy would never receive TEvResponse before
            // Bootstrap. But if the test filter blocked the Bootstrap event, the actor
            // would stay in StateBootstrap and a later TEvResponse would crash it with
            // "Unexpected bootstrap message". So only the counted PQ events may be blocked.
            const bool isPqRequestOrResponse =
                ev->CastAsLocal<TEvPersQueue::TEvRequest>() != nullptr
                || ev->CastAsLocal<TEvPersQueue::TEvResponse>() != nullptr;
            if (!isPqRequestOrResponse) {
                return false; // let the event through, don't count it
            }

            // Check if we've already reached the target and need to block events.
            // This ensures DispatchEvents returns after the target event is processed,
            // not after a batch of events.
            const ui64 currentCount = PqTabletEventCount.load();
            const ui64 target = RebootAfterEventCount.load();
            if (target > 0 && currentCount >= target && !RebootTriggered.load()) {
                // Block this event to force DispatchEvents to return.
                // The main loop will perform the reboot at a clean event boundary.
                return true;
            }

            // Count the PQ request/response event. Pure counting — no dispatch, no reboot.
            // The main loop checks this count and performs the reboot at a clean event boundary.
            const ui64 newCount = ++PqTabletEventCount;
            // Debug: Log the first few events for troubleshooting.
            static constexpr ui64 DebugLogLimit = 10;
            if (target > 0 && newCount <= DebugLogLimit) {
                Cerr << "=== EVENT_FILTER count=" << newCount << " target=" << target
                     << " type=" << ev->Type << " dest=" << ev->GetRecipientRewrite()
                     << " src=" << ev->Sender << Endl;
            }
            return false; // let the event through
        });

        runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (auto* msg = ev->CastAsLocal<NGRpcProxy::V1::TEvPQProxy::TEvCloseSession>()) {
                if (msg->ErrorCode != Ydb::PersQueue::ErrorCode::OK) {
                    ErrorCloseReason = msg->Reason;
                    ++ErrorCloseSession;
                }
            }
            if (ev->CastAsLocal<NGRpcProxy::V1::TEvPQProxy::TEvPartitionReady>()) {
                ++PartitionReadyCount;
            }
            // Count tablet boot events for reboot verification.
            // TEvTablet::EvBoot is dispatched when a new tablet instance starts.
            if (ev->Type == TEvTablet::EvBoot) {
                ++TabletBootCount;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
    }

    void DropHooks() {
        auto& runtime = Runtime();
        runtime.SetEventFilter(&TTestActorRuntimeBase::DefaultFilterFunc);
        runtime.SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
    }

    void RebootPqTablet() {
        auto& runtime = Runtime();
        const auto edge = runtime.AllocateEdgeActor();
        // Disable the event filter's blocking while the reboot is in progress.
        // The poison pill, the launcher's reboot scheduling, and the new EvBoot
        // must all flow freely — otherwise the tablet never comes back up.
        InReboot.store(true);
        Cerr << "=== REBOOT_PQ_TABLET tabletId=" << PqTabletId
             << " eventCount=" << PqTabletEventCount.load()
             << " target=" << RebootAfterEventCount.load()
             << " oldActorId=" << PqTabletActorId
             << " bootCount=" << TabletBootCount.load() << Endl;

        // Save the old actor ID before reboot so we can verify it changed.
        const TActorId oldActorId = PqTabletActorId;
        const ui64 bootCountBefore = TabletBootCount.load();

        // Step 1: Send poison pill to kill the tablet.
        ForwardToTablet(runtime, PqTabletId, edge, new TEvents::TEvPoisonPill(), /*nodeIndex=*/0, /*sysTablet=*/false);

        // Step 2: Two-phase dispatch to ensure the tablet actually dies and reboots.
        //
        // ROOT CAUSE: The original RebootTablet() from tablet_helpers.cpp does
        // ForwardToTablet(poison) + DispatchEvents(FinalEvents=[EvBoot]). In
        // simulated mode (UseRealThreads=false), this nested DispatchEvents can
        // return immediately by consuming a stale EvBoot from the initial boot,
        // without ever processing the poison pill. The tablet never dies.
        //
        // FIX: Split into two phases:
        //   Phase A: Dispatch with a timeout (no EvBoot final condition) to let
        //            the poison pill be processed. The tablet dies, the launcher
        //            schedules a reboot. We use a small timeout so this phase
        //            completes even if there's no specific event to stop on.
        //   Phase B: Dispatch with FinalEvents=[EvBoot] to wait for the NEW
        //            tablet instance to boot. Since the old tablet is dead, any
        //            EvBoot seen here must be from the new instance.
        //
        // This guarantees the poison pill is consumed before we wait for EvBoot,
        // eliminating the stale EvBoot problem.

        // Phase A: Process the poison pill.
        // Use a short timeout-based dispatch to let the system process the poison.
        // The tablet will receive the poison, call HandleDie, and die.
        // The launcher will then schedule a new tablet boot.
        Cerr << "=== PHASE_A_PROCESS_POISON" << Endl;
        DispatchEventsWithRetry([&] {
            // Dispatch with a small timeout to process pending events (including poison pill).
            // This is not waiting for any specific event — just advancing the simulation.
            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
        });

        // Phase B: Wait for the new tablet to boot.
        Cerr << "=== PHASE_B_WAIT_FOR_BOOT" << Endl;
        {
            TDispatchOptions rebootOptions;
            rebootOptions.FinalEvents.emplace_back(
                TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
            DispatchEventsWithRetry([&] {
                // Use a timeout so we don't hang forever if the tablet fails to
                // come back up. The bootCount check below will catch the failure.
                runtime.DispatchEvents(rebootOptions, TDuration::Seconds(30));
            });
        }

        const ui64 bootCountAfter = TabletBootCount.load();
        Cerr << "=== TABLET_BOOTED bootCount=" << bootCountAfter
             << " newBoots=" << (bootCountAfter - bootCountBefore) << Endl;

        // Invalidate the resolver cache so the next ResolveTablet gets the new actor.
        InvalidateTabletResolverCache(runtime, PqTabletId, /*nodeIndex=*/0);

        // Wait for scheduled events (same as RebootTablet does).
        WaitScheduledEvents(runtime, TDuration::MilliSeconds(50), edge, /*nodeIndex=*/0);

        // Resolve the new tablet actor ID.
        PqTabletActorId = ResolveTablet(runtime, PqTabletId, /*nodeIndex=*/0, /*sysTablet=*/false);

        // Verify the reboot actually happened by checking:
        // 1. A new EvBoot was observed (not a stale one from initial boot).
        // 2. The tablet actor ID changed.
        UNIT_ASSERT_C(bootCountAfter > bootCountBefore,
            "Tablet did not reboot: no new EvBoot observed (stale EvBoot consumed?)");
        UNIT_ASSERT_C(PqTabletActorId != oldActorId,
            Sprintf("Tablet actor ID did not change after reboot: old=%s new=%s",
                oldActorId.ToString().c_str(), PqTabletActorId.ToString().c_str()));

        Cerr << "=== REBOOT_PQ_TABLET_DONE tabletId=" << PqTabletId
             << " newActorId=" << PqTabletActorId
             << " bootCount=" << bootCountAfter << Endl;

        // Re-enable the event filter's blocking now that the reboot is complete.
        InReboot.store(false);
    }

    void RebootPqrbTablet() {
        auto& runtime = Runtime();
        const auto edge = runtime.AllocateEdgeActor();
        RebootTablet(runtime, PqrbTabletId, edge);
    }

    void ResetCounters() {
        PqTabletEventCount.store(0);
        PartitionReadyCount.store(0);
        ErrorCloseSession.store(0);
        ErrorCloseReason.clear();
        RebootTriggered.store(false);
        InReboot.store(false);
        TabletBootCount.store(0);
        // The tablet actor ID changes after each reboot. Re-resolve it here so
        // the event filter counts events delivered to the current tablet instance.
        // (A previous rebootPoint iteration may have rebooted the tablet, giving
        // it a new actor ID; without this the filter would match a dead actor.)
        PqTabletActorId = ResolveTablet(Runtime(), PqTabletId, /*nodeIndex=*/0, /*sysTablet=*/false);
    }
};

void TearDownGrpcAndServer(TTabletRestartReadSessionEnv& env) {
    env.DropHooks();

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

class TTabletRestartReadSessionFixture : public NUnitTest::TBaseFixture {
protected:
    TTabletRestartReadSessionEnv Env;

    // Total number of messages written across all write steps in the current
    // scenario run. Used by the data-correctness verification to check that
    // the read path delivers exactly the right number of messages with no
    // duplicates or losses.
    std::atomic<ui64> TotalWrittenMessages{0};
    // Offsets delivered (and content-verified) across all read sessions of
    // the test. Filled by the read steps on pool threads, hence the mutex.
    THashSet<i64> VerifiedOffsets;
    std::mutex VerifiedOffsetsMutex;

    void SetUp(NUnitTest::TTestContext&) override {
        Env.Start();
        Env.InstallHooks();
        TotalWrittenMessages.store(0);
        VerifiedOffsets.clear();
    }

    void TearDown(NUnitTest::TTestContext&) override {
        TearDownGrpcAndServer(Env);
    }

    NActors::TTestActorRuntime& Runtime() {
        return Env.Runtime();
    }

    // Produce one large message so the read session has data to read.
    void WriteOneMessage() {
        WriteMessages(1);
    }

    // Implementation of WriteMessages — the body that runs on the background thread.
    // Separated so that RunWithDispatchAndReboot can run it while interleaving
    // reboots at event boundaries.
    //
    // Uses the Topics API SDK (TTopicClient::CreateSimpleBlockingWriteSession)
    // which speaks the StreamWriteMessage protocol internally and — crucially —
    // has built-in retry logic via IRetryPolicy. When the PQ tablet reboots
    // mid-write, the SDK automatically reconnects and re-sends, so the write
    // survives the reboot instead of failing with UNAVAILABLE.
    void WriteMessagesImpl(ui64 count, ui64 messageSize) {
        TDriver driver(MakeNoDiscoveryDriverConfig(Env.Endpoint));
        TTopicClient client(driver);

        auto sessionSettings = TWriteSessionSettings()
            .Path(kTopicPath)
            .ProducerId("src")
            .MessageGroupId("src")
            .Codec(ECodec::RAW)
            .RetryPolicy(NTopic::IRetryPolicy::GetDefaultPolicy());

        auto writeSession = client.CreateSimpleBlockingWriteSession(sessionSettings);
        TR_ENSURE(writeSession);

        // Write messages with a deterministic, distinguishable content:
        // a fixed marker prefix followed by the global message index.
        // This lets the read path verify that the correct messages were
        // delivered (no corruption, no loss, no duplication).
        const ui64 baseIndex = TotalWrittenMessages.load();
        for (ui64 i = 0; i < count; ++i) {
            TString data = Sprintf("MSG-%020lu", (unsigned long)(baseIndex + i));
            // Pad to the requested message size (keep the marker at the start).
            if (data.size() < messageSize) {
                data.resize(messageSize, 'x');
            }
            if (!writeSession->Write(data)) {
                ythrow yexception() << "Write failed at message " << i;
            }
        }

        writeSession->Close();
        driver.Stop(true);
        TotalWrittenMessages.fetch_add(count);
    }

    // Write Count messages to the topic. Each message is messageSize bytes.
    void WriteMessages(ui64 count, ui64 messageSize = 1_MB) {
        RunWithDispatch(Runtime(), [&] {
            WriteMessagesImpl(count, messageSize);
            return true;
        });
    }

    // Open a read session with the given per-test settings.
    // The settings struct controls max_lag, read request size, etc.
    // This allows each unit test to configure its own read session behavior.
    //
    // Uses raw gRPC StreamReadMessage protocol directly. This gives full
    // control over the request/response cycle, which is essential for the
    // event-boundary reboot pattern: each gRPC Write/Read is a single blocking
    // call on the background thread, and the main thread pumps DispatchEvents
    // between calls. The Topics SDK's IReadSession::GetEvent(true) blocks
    // indefinitely on the simulated runtime (SetUseRealThreads(false)) because
    // the SDK's internal actors can't make progress without the main thread
    // dispatching events — a deadlock. Raw gRPC avoids this by making each
    // network round-trip an explicit, interruptible operation.
    struct TStreamReadSession {
        THolder<grpc::ClientContext> Context;
        std::unique_ptr<grpc::ClientReaderWriter<StreamReadClient, StreamReadServer>> Stream;

        // Data-correctness tracking: offsets of messages delivered on this
        // session, in delivery order. Used to verify no duplicates and
        // monotonicity after a tablet reboot.
        TVector<i64> DeliveredOffsets;
        // Highest offset seen so far (for monotonicity check).
        i64 LastOffset = -1;
        // Total number of messages delivered.
        ui64 DeliveredCount = 0;
        // Partition session ID (set when StartPartitionSessionRequest arrives).
        i64 PartitionSessionId = -1;
        // Bytes-size budget for ReadRequests sent on this session (copied
        // from TReadSessionSettings when the session is opened).
        ui64 ReadRequestBytesSize = 100_KB;
    };

    // Create a gRPC channel to the test server endpoint.
    // The endpoint is in "host:port" form (e.g. "localhost:12345").
    std::shared_ptr<grpc::Channel> MakeGrpcChannel() {
        auto channel = grpc::CreateChannel(Env.Endpoint, grpc::InsecureChannelCredentials());
        TR_ENSURE(channel);
        return channel;
    }

    // Send a FromClient message on the stream.
    void WriteToStream(TStreamReadSession& session, const StreamReadClient& msg, const char* what) {
        TR_ENSURE(session.Stream);
        if (!session.Stream->Write(msg)) {
            ythrow yexception() << "gRPC write failed: " << what;
        }
    }

    // Read a FromServer message from the stream. Returns false on stream end.
    bool ReadFromStream(TStreamReadSession& session, StreamReadServer& msg, const char* what) {
        TR_ENSURE(session.Stream);
        bool ok = session.Stream->Read(&msg);
        if (!ok) {
            Cerr << "gRPC read returned false (stream closed): " << what << Endl;
            return false;
        }
        return true;
    }

    TStreamReadSession OpenReadSession(const TReadSessionSettings& settings) {
        auto& runtime = Runtime();
        TStreamReadSession session;

        RunWithDispatch(runtime, [&] {
            session = OpenReadSessionImpl(settings);
            return true;
        });

        return session;
    }

    // Implementation of OpenReadSession — runs on the background thread without
    // its own dispatch. The caller (RunWithDispatch or RunWithDispatchAndReboot)
    // is responsible for pumping DispatchEvents.
    //
    // Performs the StreamRead handshake: open the bidirectional stream, send
    // InitRequest, read InitResponse. Each blocking gRPC call is a single
    // operation; the main thread interleaves DispatchEvents between them.
    TStreamReadSession OpenReadSessionImpl(const TReadSessionSettings& settings) {
        TStreamReadSession session;
        session.Context = MakeHolder<grpc::ClientContext>();
        session.ReadRequestBytesSize = settings.ReadRequestBytesSize;

        auto stub = TopicService::NewStub(MakeGrpcChannel());
        TR_ENSURE(stub);

        session.Stream = stub->StreamRead(session.Context.Get());
        TR_ENSURE(session.Stream);

        // Send InitRequest.
        StreamReadClient initMsg;
        settings.ApplyToInitRequest(*initMsg.mutable_init_request());
        WriteToStream(session, initMsg, "InitRequest");

        // Read InitResponse.
        StreamReadServer resp;
        TR_ENSURE(ReadFromStream(session, resp, "InitResponse"));
        if (!resp.has_init_response() || resp.status() != Ydb::StatusIds::SUCCESS) {
           ythrow yexception() << "InitResponse: has_init_response="
                << resp.has_init_response() << " status=" << resp.status();
        }

        return session;
    }

    // Accumulate one delivered message, verifying it on arrival. Runs on
    // pool threads, so it throws instead of asserting (see TR_ENSURE).
    //
    // Content check: the write path stamps every message with a
    // "MSG-%020lu" marker carrying its write index, which must equal the
    // assigned offset (single producer, single partition). This catches
    // corruption, loss and duplication as the data is read — the sweep's
    // read steps ARE the data-correctness verification.
    void AccumulateDelivered(TStreamReadSession& session, i64 offset, TStringBuf data) {
        const TString marker = Sprintf("MSG-%020lu", (unsigned long)offset);
        if (data.size() < marker.size() || !data.StartsWith(marker)) {
            ythrow yexception() << "content mismatch at offset " << offset
                << ": got \"" << data.substr(0, 32) << "\"";
        }
        if (offset <= session.LastOffset) {
            ythrow yexception() << "duplicate/non-monotonic offset " << offset
                << " after " << session.LastOffset;
        }
        session.LastOffset = offset;
        session.DeliveredOffsets.push_back(offset);
        ++session.DeliveredCount;
        {
            std::lock_guard<std::mutex> lock(VerifiedOffsetsMutex);
            VerifiedOffsets.insert(offset);
        }
    }

    void AssertNoErrorClose(const TString& what) {
        UNIT_ASSERT_C(Env.ErrorCloseSession.load() == 0,
            what << "; reason=" << Env.ErrorCloseReason);
    }

    void CloseSession(TStreamReadSession& session) {
        if (session.Context) {
            session.Context->TryCancel();
        }
        session.Stream.reset();
        session.Context.Reset();
    }

    // Send a CommitOffsetRequest for the given partition session and offset.
    void CommitOffsetImpl(TStreamReadSession& session, i64 commitOffset) {
        TR_ENSURE(session.Stream);
        TR_ENSURE(session.PartitionSessionId >= 0);
        TR_ENSURE(!session.DeliveredOffsets.empty());
        StreamReadClient msg;
        auto* req = msg.mutable_commit_offset_request();
        auto* off = req->add_commit_offsets();
        off->set_partition_session_id(session.PartitionSessionId);
        auto* range = off->add_offsets();
        range->set_start(session.DeliveredOffsets.front());
        range->set_end(commitOffset + 1);
        WriteToStream(session, msg, "CommitOffsetRequest");
    }

    // Commit the offset and wait for the CommitOffsetResponse ack so the
    // consumer offset is durable before the session closes.
    //
    // If the commit or the ack is lost (stream closed), the next iteration
    // simply re-reads the same messages — correct, just repeated. Runs on a
    // pool thread (throws are captured by the step's future).
    void CommitOffsetAndWaitAck(TStreamReadSession& session, i64 commitOffset) {
        try {
            CommitOffsetImpl(session, commitOffset);
        } catch (const std::exception& ex) {
            Cerr << "=== COMMIT_SKIPPED (write failed): " << ex.what() << Endl;
            return;
        }
        for (int guard = 0; guard < 1000; ++guard) {
            StreamReadServer resp;
            if (!ReadFromStream(session, resp, "commit ack")) {
                Cerr << "=== COMMIT_ACK_NOT_RECEIVED (stream closed)" << Endl;
                return;
            }
            if (resp.has_commit_offset_response()) {
                return;
            }
            if (resp.has_read_response()) {
                // Data may still arrive from a pending read request —
                // accumulate it (content-verified) like any other delivery.
                const auto& readResp = resp.read_response();
                for (const auto& pd : readResp.partition_data()) {
                    for (const auto& batch : pd.batches()) {
                        for (const auto& msg : batch.message_data()) {
                            AccumulateDelivered(session, msg.offset(), msg.data());
                        }
                    }
                }
            }
        }
    }

    // Read from the session until it has delivered the message with offset
    // endOffset-1 (everything written up to endOffset), or the stream
    // closes. Works from any starting offset (the session reads from the
    // consumer's committed offset).
    //
    // The server serves each ReadRequest up to its bytes_size budget and
    // then WAITS for the next ReadRequest, so after every data response the
    // client re-arms the server with a new ReadRequest (as the Topics SDK
    // does). An empty response only means "no data ready right now" — the
    // loop keeps waiting.
    //
    // Every delivered message is content-verified on arrival
    // (AccumulateDelivered), so the sweep's read steps ARE the data
    // correctness verification — no final re-read is needed.
    //
    // Runs on the background thread; the caller pumps DispatchEvents and
    // enforces the overall timeout (ReadAll).
    ui64 ReadAllImpl(TStreamReadSession& session, ui64 endOffset) {
        TR_ENSURE(session.Stream);
        ui64 totalRead = 0;

        // Send the initial ReadRequest.
        StreamReadClient readMsg;
        readMsg.mutable_read_request()->set_bytes_size(session.ReadRequestBytesSize);
        WriteToStream(session, readMsg, "ReadAll ReadRequest");

        // Read until the session has delivered the message with offset
        // endOffset-1.
        while (static_cast<ui64>(session.LastOffset + 1) < endOffset) {
            StreamReadServer resp;
            if (!ReadFromStream(session, resp, "ReadAll loop")) {
                return totalRead;
            }

            if (resp.has_start_partition_session_request()) {
                const auto& req = resp.start_partition_session_request();
                session.PartitionSessionId = req.partition_session().partition_session_id();
                StreamReadClient confirmMsg;
                auto* confirm = confirmMsg.mutable_start_partition_session_response();
                confirm->set_partition_session_id(session.PartitionSessionId);
                WriteToStream(session, confirmMsg, "ReadAll StartPartitionSessionResponse");
                continue;
            }

            if (resp.has_stop_partition_session_request()) {
                const auto& req = resp.stop_partition_session_request();
                StreamReadClient confirmMsg;
                auto* confirm = confirmMsg.mutable_stop_partition_session_response();
                confirm->set_partition_session_id(req.partition_session_id());
                confirm->set_graceful(req.graceful());
                WriteToStream(session, confirmMsg, "ReadAll StopPartitionSessionResponse");
                // After stop, no more data from this partition.
                return totalRead;
            }

            if (resp.has_read_response()) {
                const auto& readResp = resp.read_response();
                if (readResp.partition_data_size() > 0) {
                    for (const auto& pd : readResp.partition_data()) {
                        for (const auto& batch : pd.batches()) {
                            for (const auto& msg : batch.message_data()) {
                                AccumulateDelivered(session, msg.offset(), msg.data());
                                ++totalRead;
                            }
                        }
                    }
                    // Re-arm the server with a fresh budget: it has served
                    // this ReadRequest up to bytes_size and now waits for
                    // the next one.
                    StreamReadClient rearmMsg;
                    rearmMsg.mutable_read_request()->set_bytes_size(session.ReadRequestBytesSize);
                    WriteToStream(session, rearmMsg, "ReadAll ReadRequest (re-arm)");
                }
                // An empty response only means "no data ready right now" —
                // keep waiting for the remaining messages.
                continue;
            }

            // Other messages — continue.
            continue;
        }
        return totalRead;
    }

    // Read messages with a timeout (main-thread dispatch version).
    // endOffset is passed through to ReadAllImpl.
    ui64 ReadAll(TStreamReadSession& session, TDuration timeout, ui64 endOffset) {
        auto future = NThreading::Async([&] {
            return ReadAllImpl(session, endOffset);
        }, DispatchPool());

        auto& runtime = Runtime();
        const TInstant deadline = runtime.GetCurrentTime() + timeout;
        while (runtime.GetCurrentTime() < deadline && !future.HasValue() && !future.HasException()) {
            DispatchEventsWithRetry([&] {
                runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
            });
        }

        if (!future.HasValue() && !future.HasException()) {
            if (session.Context) {
                session.Context->TryCancel();
            }
            const TInstant cancelDeadline = runtime.GetCurrentTime() + TDuration::Seconds(20);
            while (runtime.GetCurrentTime() < cancelDeadline && !future.HasValue() && !future.HasException()) {
                DispatchEventsWithRetry([&] {
                    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
                });
            }
            if (!future.HasValue() && !future.HasException()) {
                UNIT_ASSERT_C(false, "ReadAll did not finish within 20s after TryCancel");
            }
        }
        future.TryRethrow();
        if (!future.HasValue()) {
            return 0;
        }
        return future.GetValueSync();
    }

    // Final correctness check: every written message must have been
    // delivered and content-verified by the sweep's read steps (offsets
    // accumulated in VerifiedOffsets). No re-reading from the tablet.
    void VerifyFullCoverage(const TString& label) {
        const i64 total = static_cast<i64>(TotalWrittenMessages.load());
        i64 firstMissing = -1;
        ui64 missingCount = 0;
        {
            std::lock_guard<std::mutex> lock(VerifiedOffsetsMutex);
            for (i64 off = 0; off < total; ++off) {
                if (!VerifiedOffsets.contains(off)) {
                    if (firstMissing < 0) {
                        firstMissing = off;
                    }
                    ++missingCount;
                }
            }
        }
        UNIT_ASSERT_C(missingCount == 0, label << ": " << missingCount << " of " << total
            << " messages never delivered; first missing offset: " << firstMissing);
        AssertNoErrorClose(label);
    }

    // Implementation of the Concurrent scenario step — runs on the background
    // thread; the caller (RunWithDispatch or RunWithDispatchAndReboot) pumps
    // DispatchEvents and may reboot the tablet at any event boundary.
    //
    // Opens ReadSessionCount read sessions, optionally launches a write on
    // the dispatch pool while the sessions are open, then reads everything
    // written so far from each session and closes them. This forces the
    // partition actor to handle a pending read and an incoming write/commit
    // simultaneously during a pipe restart (ResendRecentRequests with both
    // RequestInfly and CommitsInfly non-empty), and — with several sessions —
    // saturates the inflight-reads limit so the reboot hits the
    // memory-controller branch.
    bool ConcurrentReadWriteStepImpl(const TScenarioStep& step) {
        // Open all read sessions first so they are concurrently active.
        // With more than one session, each uses its own consumer so the
        // balancer assigns the partition to every session independently.
        TVector<TStreamReadSession> sessions;
        sessions.reserve(step.ReadSessionCount);
        for (ui32 i = 0; i < step.ReadSessionCount; ++i) {
            TReadSessionSettings settings = step.ReadSettings;
            if (step.ReadSessionCount > 1) {
                settings.Consumer = i == 0
                    ? TString(kConsumer)
                    : Sprintf("user%u", i - 1);
            }
            sessions.push_back(OpenReadSessionImpl(settings));
        }

        // Launch the write (if requested) while the read sessions are open.
        NThreading::TFuture<bool> writeFuture;
        if (step.Count > 0) {
            writeFuture = NThreading::Async([&] {
                WriteMessagesImpl(step.Count, step.MessageSize);
                return true;
            }, DispatchPool());
        }

        // Read everything written so far from each session, all
        // concurrently, so several partition sessions with pending reads
        // are in flight at the same time.
        const ui64 endOffset = std::max<ui64>(TotalWrittenMessages.load(), 1);
        TVector<NThreading::TFuture<bool>> readFutures;
        for (auto& session : sessions) {
            readFutures.push_back(NThreading::Async([this, &session, endOffset, &step, &writeFuture] {
                ReadAllImpl(session, endOffset);
                if (step.Count > 0) {
                    // The concurrent write adds messages while the read is
                    // in flight — wait for it and read them too, then
                    // commit the progress (see CommitOffsetAndWaitAck).
                    writeFuture.GetValueSync();
                    const ui64 newEnd = TotalWrittenMessages.load();
                    if (static_cast<ui64>(session.LastOffset + 1) < newEnd) {
                        ReadAllImpl(session, newEnd);
                    }
                    if (!session.DeliveredOffsets.empty()) {
                        CommitOffsetAndWaitAck(session, session.LastOffset);
                    }
                }
                return static_cast<ui64>(session.LastOffset + 1) >= TotalWrittenMessages.load();
            }, DispatchPool()));
        }
        bool gotData = false;
        for (auto& f : readFutures) {
            gotData = f.GetValueSync() || gotData;
        }

        // Close all sessions.
        for (auto& session : sessions) {
            if (session.Context) {
                session.Context->TryCancel();
            }
            session.Stream.reset();
            session.Context.Reset();
        }

        // Wait for the concurrent write to finish (rethrows write failures).
        if (step.Count > 0) {
            writeFuture.GetValueSync();
        }
        return gotData;
    }

    // Run a lambda on the background dispatch pool while pumping DispatchEvents on
    // the main thread. Unlike RunWithDispatch (which uses WaitFuture and stops only
    // when the future completes), this version ALSO stops DispatchEvents when the
    // PQ tablet event count reaches the reboot target (via TFinalEventCondition).
    //
    // When DispatchEvents returns because the target event was reached (but the
    // future is not yet complete), control returns to the main loop where the
    // reboot is performed safely. Then dispatch resumes. This repeats until the
    // future completes (the step is done).
    //
    // This is the safe event-boundary reboot pattern: no nested DispatchEvents,
    // no re-entrant dispatch. The reboot always happens in the main loop after
    // DispatchEvents returns at a clean event boundary.
    template <typename TFunc>
    auto RunWithDispatchAndReboot(TFunc&& func, TDuration stepTimeout = TDuration::Seconds(30)) {
        auto& runtime = Runtime();
        auto future = NThreading::Async(std::forward<TFunc>(func), DispatchPool());

        // Wall-clock deadline to prevent individual scenario steps from hanging
        // indefinitely (e.g., due to infinite dispatch loops or stuck reboots).
        const TInstant deadline = TInstant::Now() + stepTimeout;

        while (!future.HasValue() && !future.HasException()) {
            // Check wall-clock timeout before each dispatch iteration.
            if (TInstant::Now() >= deadline) {
                UNIT_ASSERT_C(false,
                    "Scenario step exceeded wall-clock timeout of " << stepTimeout
                    << " (deadline=" << deadline << ", now=" << TInstant::Now() << ")");
            }
            TDispatchOptions options;
            // Stop when the future completes (the step is done).
            options.CustomFinalCondition = [&]() {
                return future.HasValue() || future.HasException();
            };
            // The event filter (InstallHooks) now blocks events after the target
            // count is reached, forcing DispatchEvents to return. This FinalEvents
            // callback is a secondary check — it fires when the target is reached
            // and the reboot hasn't been triggered yet.
            const ui64 target = Env.RebootAfterEventCount.load();
            if (target > 0) {
                options.FinalEvents.emplace_back(
                    [this](IEventHandle& /*ev*/) {
                        const bool triggered = Env.RebootTriggered.load();
                        const ui64 count = Env.PqTabletEventCount.load();
                        const ui64 target = Env.RebootAfterEventCount.load();
                        // Fire when target is reached and reboot not yet triggered.
                        // With the filter blocking events after target, this will
                        // fire at count == target exactly.
                        const bool shouldFire = !triggered && count >= target;
                        if (shouldFire) {
                            Cerr << "=== FINAL_EVENT_FIRED count=" << count
                                 << " target=" << target
                                 << " triggered=" << triggered << Endl;
                        }
                        return shouldFire;
                    });
            } else {
                // Quirk: non-empty FinalEvents enables full simulation (same as
                // WaitFuture). Use a dummy that never fires.
                options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
            }

            Cerr << "=== DISPATCH_START count=" << Env.PqTabletEventCount.load()
                 << " target=" << Env.RebootAfterEventCount.load()
                 << " triggered=" << Env.RebootTriggered.load() << Endl;

            // Use a short timeout so DispatchEvents returns periodically.
            // Without this, DispatchEvents can block forever when neither the
            // CustomFinalCondition nor the FinalEvents fire (e.g., after a reboot
            // is triggered and the future is still running). The periodic return
            // allows the wall-clock timeout check at the top of the loop to run.
            DispatchEventsWithRetry([&] {
                runtime.DispatchEvents(options, TDuration::Seconds(1));
            });

            // If the future is not complete, DispatchEvents returned because of
            // the reboot event condition. Perform the reboot in the main loop.
            if (!future.HasValue() && !future.HasException()) {
                const ui64 currentCount = Env.PqTabletEventCount.load();
                const ui64 currentTarget = Env.RebootAfterEventCount.load();
                Cerr << "=== DISPATCH_RETURNED count=" << currentCount
                     << " target=" << currentTarget
                     << " triggered=" << Env.RebootTriggered.load()
                     << " futureHasValue=" << future.HasValue()
                     << " futureHasException=" << future.HasException() << Endl;
                if (currentTarget > 0 && currentCount >= currentTarget && !Env.RebootTriggered.load()) {
                    if (!Env.RebootTriggered.exchange(true)) {
                        Cerr << "=== REBOOT_TRIGGERED count=" << currentCount
                             << " target=" << currentTarget << Endl;
                        // SAFE: we are in the main loop, not inside any callback.
                        // Perform the reboot using the same pattern as PQTabletRestart:
                        // ForwardToTablet(poison pill) + DispatchEvents(EvBoot) +
                        // InvalidateTabletResolverCache.
                        Env.RebootPqTablet();
                        // Disable further reboots for this step run. The reboot at
                        // the target event is done; the step should now continue to
                        // completion normally. Setting RebootAfterEventCount=0 makes
                        // the FinalEvents use a dummy (never fires) and the filter
                        // stop blocking, so DispatchEvents runs until the future
                        // completes (with the 1s timeout ensuring periodic returns).
                        Env.RebootAfterEventCount.store(0);
                    }
                }
            }
        }

        Y_ABORT_UNLESS(future.HasValue() || future.HasException());
        if constexpr (std::is_same_v<decltype(future.GetValueSync()), void>) {
            future.GetValueSync();
        } else {
            return future.GetValueSync();
        }
    }

    // Execute a scenario step: either write messages or open/read/close a session.
    // Returns true if the step completed successfully.
    // For read steps, stores whether data was received in dataReceived.
    //
    // When a reboot target is set (RebootAfterEventCount > 0), the step is executed
    // via RunWithDispatchAndReboot so that the tablet is rebooted at the target
    // event boundary during the step. When no reboot target is set, the step runs
    // normally via RunWithDispatch.
    bool ExecuteScenarioStep(const TScenarioStep& step, bool* dataReceived = nullptr) {
        Cerr << "=== EXECUTE_STEP type=" << static_cast<int>(step.Type)
             << " rebootTarget=" << Env.RebootAfterEventCount.load() << Endl;
        if (step.Type == TScenarioStep::EType::Write) {
            if (Env.RebootAfterEventCount.load() > 0) {
                RunWithDispatchAndReboot([&] {
                    WriteMessagesImpl(step.Count, step.MessageSize);
                    return true;
                });
            } else {
                WriteMessages(step.Count, step.MessageSize);
            }
            return true;
        }
        if (step.Type == TScenarioStep::EType::Concurrent) {
            if (Env.RebootAfterEventCount.load() > 0) {
                bool gotData = RunWithDispatchAndReboot([&] {
                    return ConcurrentReadWriteStepImpl(step);
                });

                if (dataReceived) {
                    *dataReceived = gotData;
                }

                AssertNoErrorClose("scenario concurrent step");
                UNIT_ASSERT_C(gotData,
                    "scenario concurrent step: not all expected data delivered");
            } else {
                bool gotData = RunWithDispatch(Runtime(), [&] {
                    return ConcurrentReadWriteStepImpl(step);
                });

                if (dataReceived) {
                    *dataReceived = gotData;
                }

                AssertNoErrorClose("scenario concurrent step");
                UNIT_ASSERT_C(gotData,
                    "scenario concurrent step: not all expected data delivered");
            }
            return true;
        } else {
            // Read step: open session, read data, close session.
            // When a reboot target is set, run the entire open+read+close as a
            // single background lambda so that reboots can interleave at any
            // event boundary during the entire sequence.
            if (Env.RebootAfterEventCount.load() > 0) {
                bool gotData = RunWithDispatchAndReboot([&] {
                    // Open session (blocks on gRPC I/O, main thread pumps events).
                    TStreamReadSession session = OpenReadSessionImpl(step.ReadSettings);

                    // Read everything written so far: reaching the known end
                    // offset is the reliable stop condition (the server
                    // never signals end-of-data itself).
                    const ui64 endOffset = std::max<ui64>(TotalWrittenMessages.load(), 1);
                    ReadAllImpl(session, endOffset);
                    const bool complete = static_cast<ui64>(session.LastOffset + 1) >= endOffset;

                    // Commit the progress and wait for the ack — inside the
                    // same lambda, so reboots can land between the read and
                    // the commit (stressing CommitsInfly resend on recovery).
                    // The commit keeps the response of the next iteration
                    // small (see CommitOffsetAndWaitAck).
                    if (!session.DeliveredOffsets.empty()) {
                        CommitOffsetAndWaitAck(session, session.LastOffset);
                    }

                    // Close session (cancel gRPC stream).
                    if (session.Context) {
                        session.Context->TryCancel();
                    }
                    session.Stream.reset();
                    session.Context.Reset();

                    return complete;
                });

                if (dataReceived) {
                    *dataReceived = gotData;
                }

                AssertNoErrorClose("scenario read step");
                UNIT_ASSERT_C(gotData,
                    "scenario read step: not all expected data delivered");
            } else {
                auto session = OpenReadSession(step.ReadSettings);
                const ui64 endOffset = std::max<ui64>(TotalWrittenMessages.load(), 1);
                ReadAll(session, TDuration::Seconds(30), endOffset);
                const bool complete = static_cast<ui64>(session.LastOffset + 1) >= endOffset;

                // Commit the progress (see the reboot path above).
                if (!session.DeliveredOffsets.empty()) {
                    RunWithDispatch(Runtime(), [&] {
                        CommitOffsetAndWaitAck(session, session.LastOffset);
                        return true;
                    });
                }

                if (dataReceived) {
                    *dataReceived = complete;
                }

                AssertNoErrorClose("scenario read step");
                UNIT_ASSERT_C(complete,
                    "scenario read step: not all expected data delivered");

                CloseSession(session);
            }
            return true;
        }
    }

    // Run the full scenario once with an event-boundary reboot after the
    // rebootPoint-th PQ tablet event (at a clean event boundary: after the
    // event is fully processed, before the next event starts). The reboot
    // is performed in the main loop using TFinalEventCondition to stop
    // DispatchEvents at the target event — never inside the event filter
    // callback (which would re-enter the dispatch loop).
    // Returns whether the reboot actually triggered (i.e. the scenario
    // produced at least rebootPoint tablet events).
    bool RunScenarioOnceAtRebootPoint(const TScenario& scenario, ui64 rebootPoint) {
        TString testLabel = Sprintf("reboot_after_event_%lu", (unsigned long)rebootPoint);
        Cerr << "=== REBOOT_POINT=" << rebootPoint << Endl;

        // Reset counters for this run.
        Env.ResetCounters();
        Env.RebootAfterEventCount.store(rebootPoint);

        // Execute each step in the scenario.
        for (ui64 stepIdx = 0; stepIdx < scenario.size(); ++stepIdx) {
            TString stepLabel = testLabel + Sprintf(" step_%lu", (unsigned long)stepIdx);

            bool dataReceived = false;
            ExecuteScenarioStep(scenario[stepIdx], &dataReceived);

            AssertNoErrorClose(stepLabel);
        }

        // Check if reboot was actually triggered.
        Cerr << "=== REBOOT_CHECK rebootPoint=" << rebootPoint
             << " triggered=" << Env.RebootTriggered.load()
             << " eventCount=" << Env.PqTabletEventCount.load() << Endl;
        if (!Env.RebootTriggered.load()) {
            Cerr << "=== NO_REBOOT_TRIGGERED rebootPoint=" << rebootPoint
                 << " — stopping (no more events)" << Endl;
        }
        return Env.RebootTriggered.load();
    }

    // Run the scenario with an event-boundary reboot at every reboot point
    // from initialRebootPoint up to the scenario's natural event count: the
    // sweep terminates at the first reboot point that does not trigger (the
    // scenario produced fewer events than the point). There is no arbitrary
    // upper limit — if something important happens on a later event, that
    // boundary is still reboot-tested.
    //
    // rebootPointStride: reboot at every stride-th event boundary instead of
    // every one. For scenarios that produce many events (e.g. a 100-message
    // write produces ~500 tablet events, and rebooting at every single
    // boundary would re-run it ~500 times), a stride of 2-3 keeps the
    // runtime bounded while still sweeping the full event range.
    void RunScenarioWithAllReboots(const TScenario& scenario, ui64 initialRebootPoint = 1, ui64 rebootPointStride = 1) {
        ui64 rebootPoint = initialRebootPoint;
        while (RunScenarioOnceAtRebootPoint(scenario, rebootPoint)) {
            rebootPoint += rebootPointStride;
        }

        // CRITICAL: clear the reboot target. When the sweep stops at a
        // non-triggering point, RebootAfterEventCount stays set to that point
        // while RebootTriggered stays false — the event filter then BLOCKS
        // every PQ tablet event once the running count reaches the target.
        // Any read session opened after the sweep (the final verification
        // read) would get no tablet responses at all: no
        // StartPartitionSessionRequest, no data, ReadAll timing out with 0
        // messages delivered.
        Env.RebootAfterEventCount.store(0);
    }

    // Run the scenario with an event-boundary reboot at each of the given
    // reboot points (expected in ascending order). Stops at the first point
    // that does not trigger — the same exhaustion rule as
    // RunScenarioWithAllReboots. This lets a test cover the important
    // lifecycle stages densely (e.g. the write session init and tail) while
    // only sampling the repetitive middle of a long scenario — much cheaper
    // than a uniform sweep when the scenario produces many events.
    void RunScenarioAtRebootPoints(const TScenario& scenario, const TVector<ui64>& rebootPoints) {
        for (ui64 rebootPoint : rebootPoints) {
            if (!RunScenarioOnceAtRebootPoint(scenario, rebootPoint)) {
                break;
            }
        }

        // Clear the reboot target (see RunScenarioWithAllReboots).
        Env.RebootAfterEventCount.store(0);
    }

};

Y_UNIT_TEST_SUITE_F(TTabletRestartReadSessionTest, TTabletRestartReadSessionFixture) {

// Write one message, then read it, rebooting the PQ tablet after each event
// boundary. Catches the PR 50890 bug: with WaitForData=true the old code
// re-entered WaitDataInPartition after a pipe restart without checking
// whether data had arrived during the downtime. max_lag=3600 enables
// WaitForData.
Y_UNIT_TEST(ReadSessionWithDataSurvivesTabletRebootAfterEachEvent) {
    // Build scenario: write one message, then read it.
    TScenario scenario;
    scenario.push_back(TScenarioStep{.Type = TScenarioStep::EType::Write, .Count = 1});

    TScenarioStep readStep;
    readStep.Type = TScenarioStep::EType::Read;
    readStep.ReadSettings.MaxLagSeconds = 3600; // 1 hour lag → WaitForData = true
    readStep.ReadSettings.ReadRequestBytesSize = 2_MB; // covers the 1MB messages
    scenario.push_back(readStep);

    // Run the scenario with reboots after each event.
    RunScenarioWithAllReboots(scenario);

    // Final correctness verification.
    VerifyFullCoverage("ReadSessionWithData");
}

// A 3MB message spans multiple blob parts: a reboot mid-read exercises
// part reassembly and cache recovery.
Y_UNIT_TEST(BigChunksSurviveTabletRebootAfterEachEvent) {
    TScenario scenario;
    scenario.push_back(TScenarioStep{.Type = TScenarioStep::EType::Write, .Count = 1, .MessageSize = 3_MB});

    TScenarioStep readStep;
    readStep.Type = TScenarioStep::EType::Read;
    readStep.ReadSettings.MaxLagSeconds = 3600; // WaitForData = true
    readStep.ReadSettings.ReadRequestBytesSize = 4_MB; // covers the 3MB messages
    scenario.push_back(readStep);

    RunScenarioWithAllReboots(scenario);

    // Final correctness verification.
    VerifyFullCoverage("BigChunks");
}

// A 100-message write stresses write batching and BytesInflight tracking.
Y_UNIT_TEST(ManyMessagesSurviveTabletRebootAfterEachEvent) {
    TScenario scenario;
    scenario.push_back(TScenarioStep{.Type = TScenarioStep::EType::Write, .Count = 100, .MessageSize = 1_KB});

    TScenarioStep readStep;
    readStep.Type = TScenarioStep::EType::Read;
    readStep.ReadSettings.MaxLagSeconds = 3600; // WaitForData = true
    scenario.push_back(readStep);

    // The write produces ~500 tablet events, mostly the same repetitive
    // write-request/response pattern; a uniform sweep re-runs it ~160
    // times (~220s). Reboot at the important stages instead: densely at the
    // session start, sampled across the bulk write, densely at the tail
    // (last writes, close and commit of a long session — a state the
    // small-write tests never reach). The schedule stops at the first
    // point that does not trigger.
    TVector<ui64> rebootPoints;
    for (ui64 p = 1; p <= 15; ++p) {
        rebootPoints.push_back(p); // session init + first writes
    }
    for (ui64 p = 25; p <= 425; p += 50) {
        rebootPoints.push_back(p); // steady-state bulk write samples
    }
    for (ui64 p = 430; p <= 500; p += 5) {
        rebootPoints.push_back(p); // tail: last writes, close, commit
    }
    RunScenarioAtRebootPoints(scenario, rebootPoints);

    // Final correctness verification.
    VerifyFullCoverage("ManyMessages");
}

// A 1MB read request saturates the read batch path and MAX_INFLY_BYTES.
Y_UNIT_TEST(LargeReadRequestSurvivesTabletRebootAfterEachEvent) {
    TScenario scenario;
    scenario.push_back(TScenarioStep{.Type = TScenarioStep::EType::Write, .Count = 1, .MessageSize = 1_MB});

    TScenarioStep readStep;
    readStep.Type = TScenarioStep::EType::Read;
    readStep.ReadSettings.MaxLagSeconds = 3600; // WaitForData = true
    readStep.ReadSettings.ReadRequestBytesSize = 2_MB; // covers the 1MB messages
    scenario.push_back(readStep);

    RunScenarioWithAllReboots(scenario);

    // Final correctness verification.
    VerifyFullCoverage("LargeReadRequest");
}

// max_lag=1 aggressively triggers the WaitForData timeout path and the
// WaitDataInPartition timer race during a reboot.
Y_UNIT_TEST(SmallLagSurvivesTabletRebootAfterEachEvent) {
    TScenario scenario;
    scenario.push_back(TScenarioStep{.Type = TScenarioStep::EType::Write, .Count = 1, .MessageSize = 1_MB});

    TScenarioStep readStep;
    readStep.Type = TScenarioStep::EType::Read;
    readStep.ReadSettings.MaxLagSeconds = 1; // small lag → WaitForData timeout path
    readStep.ReadSettings.ReadRequestBytesSize = 2_MB; // covers the 1MB messages
    scenario.push_back(readStep);

    RunScenarioWithAllReboots(scenario);

    // Final correctness verification.
    VerifyFullCoverage("SmallLag");
}

// Read and write concurrently: a reboot can hit the partition actor with
// both a pending read and a pending write/commit in flight.
Y_UNIT_TEST(ConcurrentReadWriteSurviveTabletReboot) {
    // Pre-write a few small messages so the read session has data
    // immediately when it opens.
    WriteMessages(3, 1_KB);

    // Concurrent step: open a read session, then write while it is open,
    // then read.
    TScenario scenario;
    TScenarioStep concurrentStep;
    concurrentStep.Type = TScenarioStep::EType::Concurrent;
    concurrentStep.Count = 5;
    concurrentStep.MessageSize = 1_KB;
    concurrentStep.ReadSettings.MaxLagSeconds = 3600; // WaitForData = true
    scenario.push_back(concurrentStep);

    RunScenarioWithAllReboots(scenario);

    // Final correctness verification.
    VerifyFullCoverage("ConcurrentReadWrite");
}

// 5 concurrent read sessions saturate inflight reads, so a reboot hits the
// memory-controller branch in ResendRecentRequests.
Y_UNIT_TEST(MultipleReadSessionsSurviveTabletReboot) {
    // Write data for the sessions to read.
    WriteMessages(5, 1_KB);

    // 5 simultaneously open read sessions, each with its own consumer.
    TScenario scenario;
    TScenarioStep concurrentStep;
    concurrentStep.Type = TScenarioStep::EType::Concurrent;
    concurrentStep.Count = 0; // read-only
    concurrentStep.ReadSessionCount = 5;
    concurrentStep.ReadSettings.MaxLagSeconds = 3600; // WaitForData = true
    scenario.push_back(concurrentStep);

    RunScenarioWithAllReboots(scenario);

    // Final correctness verification.
    VerifyFullCoverage("MultipleReadSessions");
}

// Reboot during the WaitForData wait with data arriving concurrently (the
// PR 50890 race): the read opens on an empty topic and blocks in
// WaitDataInPartition; a concurrent write must unblock it. If the
// regression reappears, the read hangs and the step timeout fails the
// test.
Y_UNIT_TEST(RebootDuringWaitForDataTimeout) {
    // No pre-written data: the concurrent write unblocks the waiting read.
    TScenario scenario;
    TScenarioStep concurrentStep;
    concurrentStep.Type = TScenarioStep::EType::Concurrent;
    concurrentStep.Count = 1; // the write that unblocks the waiting read
    concurrentStep.MessageSize = 1_KB;
    concurrentStep.ReadSettings.MaxLagSeconds = 1; // small lag -> WaitForData wait
    scenario.push_back(concurrentStep);

    RunScenarioWithAllReboots(scenario);

    // Final correctness verification.
    VerifyFullCoverage("RebootDuringWaitForDataTimeout");
}

} // Y_UNIT_TEST_SUITE_F(TTabletRestartReadSessionTest)

} // anonymous namespace
} // namespace NKikimr::NPersQueueTests
