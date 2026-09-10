#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/core/base/tablet_pipe.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/retry_policy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/thread/pool.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>

using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

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

/// Per-test configuration for the SDK read sessions of a Concurrent step.
struct TReadSessionSettings {
    // If set (> 0), max_lag will be configured on the topic read settings
    // (triggers the WaitForData path on the server). 0 means no WaitForData.
    ui64 MaxLagSeconds = 0;
};

/// A single step in a scenario: a concurrent write+read.
struct TScenarioStep {
    enum class EType {
        Concurrent,  // Open ReadSessionCount SDK read sessions, write Count
                     // messages while they are open, then wait for each
                     // session to deliver everything written
    };

    EType Type = EType::Concurrent;
    ui64 Count = 1;           // number of messages to write
    ui64 MessageSize = 1_MB;  // size of each message in bytes
    TReadSessionSettings ReadSettings;  // read session settings
    // Number of read sessions to open simultaneously.
    // When > 1, session i uses consumer "user" + (i-1) so each session gets
    // an independent partition assignment.
    ui32 ReadSessionCount = 1;
};

/// A scenario is a sequence of steps (write/read operations).
using TScenario = TVector<TScenarioStep>;

// Decode a TEvPersQueue event type to a short human-readable name.
// Offsets are relative to TEvPersQueue::EvRequest (see core/persqueue/events/global.h).
const char* PqEventTypeName(ui32 type) {
    const ui64 base = NKikimr::TEvPersQueue::EvRequest;
    if (type < base || type >= NKikimr::TEvPersQueue::EvEnd) {
        return "NON_PQ";
    }
    switch (type - base) {
        case 0:   return "EvRequest";
        case 1:   return "EvUpdateConfig";
        case 2:   return "EvUpdateConfigResponse";
        case 3:   return "EvOffsets";
        case 4:   return "EvOffsetsResponse";
        case 5:   return "EvDropTablet";
        case 6:   return "EvDropTabletResult";
        case 7:   return "EvStatus";
        case 8:   return "EvStatusResponse";
        case 9:   return "EvHasDataInfo";
        case 10:  return "EvHasDataInfoResponse";
        case 11:  return "EvPartitionClientInfo";
        case 12:  return "EvPartitionClientInfoResponse";
        case 13:  return "EvUpdateBalancerConfig";
        case 14:  return "EvRegisterReadSession";
        case 15:  return "EvLockPartition";
        case 16:  return "EvReleasePartition";
        case 17:  return "EvPartitionReleased";
        case 18:  return "EvDescribe";
        case 19:  return "EvDescribeResponse";
        case 20:  return "EvGetReadSessionsInfo";
        case 21:  return "EvReadSessionsInfoResponse";
        case 22:  return "EvWakeupClient";
        case 23:  return "EvUpdateACL";
        case 24:  return "EvCheckACL";
        case 25:  return "EvCheckACLResponse";
        case 26:  return "EvError";
        case 27:  return "EvGetPartitionIdForWrite";
        case 28:  return "EvGetPartitionIdForWriteResponse";
        case 29:  return "EvReportPartitionError";
        case 30:  return "EvProposeTransaction";
        case 31:  return "EvProposeTransactionResult";
        case 32:  return "EvCancelTransactionProposal";
        case 33:  return "EvPeriodicTopicStats";
        case 34:  return "EvGetPartitionsLocation";
        case 35:  return "EvGetPartitionsLocationResponse";
        case 36:  return "EvReadingPartitionFinished";
        case 37:  return "EvReadingPartitionStarted";
        case 38:  return "EvOffloadStatus";
        case 39:  return "EvBalancingSubscribe";
        case 40:  return "EvBalancingUnsubscribe";
        case 41:  return "EvBalancingSubscribeNotify";
        case 42:  return "EvPartitionUpdateReadMetrics";
        case 43:  return "EvCheckMessageDeduplicationRequest";
        case 44:  return "EvCheckMessageDeduplicationResponse";
        case 256: return "EvResponse";
        case 512: return "EvInternalEvents";
        default:  return "EvUnknown";
    }
}

struct TTabletRestartReadSessionEnv {
    std::unique_ptr<::NPersQueue::TTestServer> Server;
    TString Endpoint;
    ui64 PqTabletId = 0;
    ui64 PqrbTabletId = 0;
    // Actor IDs of the PQ tablet and the balancer tablet (they change after
    // each reboot). Resolved once at startup and then re-tracked by the event
    // filter on every TEvTablet::EvBoot (the boot event's recipient is the new
    // tablet actor). Used to address the injected poison pill at the event
    // boundary.
    TActorId PqTabletActorId;
    TActorId PqrbActorId;

    // Any TEvCloseSession with ErrorCode != OK.
    // Teardown DropHooks() clears the observer before gRPC cancel, so shutdown noise is ignored.
    std::atomic<ui64> ErrorCloseSession{0};
    TString ErrorCloseReason;

    // TEvPartitionReady delivered to read session actor (proves WaitForData was reset).
    std::atomic<ui64> PartitionReadyCount{0};

    // Reboot verification counter — tracked via the event observer.
    // TabletBootCount increments on each TEvTablet::EvBoot event, proving
    // that a new tablet instance booted after an injected poison pill.
    std::atomic<ui64> TabletBootCount{0};

    // Count of TEvPersQueue::TEvRequest and TEvPersQueue::TEvResponse events flowing
    // to/from the PQ tablet. Used by the event-filter-based reboot loop.
    std::atomic<ui64> PqTabletEventCount{0};

    // Count of TEvPersQueue events flowing to/from the balancer tablet (PqrbTabletId).
    // Used for balancer-reboot event-boundary detection.
    std::atomic<ui64> PqBalancerEventCount{0};

    // Per-event-type counters for the current run (reset each reboot point).
    // Logged with names so the event sequence is easy to read after a failure.
    THashMap<TString, ui64> PqEventTypeCount;

    // The reboot point (target event) for the current scenario run. Kept separate
    // from RebootAfterEventCount, which is cleared to 0 after the reboot fires, so
    // logging can still report the original reboot point afterwards.
    std::atomic<ui64> RebootPoint{0};

    // Event count at which to trigger the reboot (0 = no reboot, 1 = reboot after 1st event, etc.).
    std::atomic<ui64> RebootAfterEventCount{0};
    // Whether the reboot has already been triggered in the current test run.
    std::atomic<bool> RebootTriggered{false};

    // Which tablet to reboot (PqTabletId or PqrbTabletId), set by the event
    // filter at the boundary based on which pipe the boundary event flows
    // through. The main loop reads this to call RebootTablet() for the
    // correct party.
    std::atomic<ui64> RebootTargetTabletId{0};

    // Set by the main loop after RebootTablet() completes. The event filter
    // drops all TEvPersQueue events while RebootTriggered && !RebootCompleted,
    // simulating the tablet being unavailable during the death/reboot window
    // (events sent to a dead tablet are lost, exactly as in production).
    std::atomic<bool> RebootCompleted{false};

    // Count of TEvPersQueue events dropped during the reboot window
    // (RebootTriggered && !RebootCompleted). Used for debug logging so we
    // can see exactly which events are blocked while the tablet is dead.
    std::atomic<ui64> DroppedInRebootCount{0};

    // Reboot switches: control which tablet(s) to reboot at the event boundary.
    // DoRebootPqTablet: reboot the partition tablet (PqTabletId).
    // DoRebootPqrbTablet: reboot the balancer tablet (PqrbTabletId).
    // When both are true, the reboot targets exactly the party whose event is
    // at the boundary.
    bool DoRebootPqTablet = true;
    bool DoRebootPqrbTablet = false;

    // Pipe actors (client- and server-side) whose target tablet is the PQ tablet
    // (PqTabletId). Populated by TEvClientConnected observed in the event filter.
    // Used to classify whether a counted event flows through the PQ tablet's
    // pipe (so a reboot at its boundary is realistic).
    THashSet<TActorId> PqTabletPipeActors;

    // Pipe actors (client- and server-side) whose target tablet is the balancer
    // tablet (PqrbTabletId). Populated by TEvClientConnected observed in the
    // event filter. Used to classify whether a counted event flows through the
    // balancer's pipe.
    THashSet<TActorId> PqBalancerPipeActors;

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
            Server->AnnoyingClient->SetNoConfigMode();

            // No-config mode: only need Root + /PQ.
            Server->AnnoyingClient->InitRootScheme();
            Server->AnnoyingClient->MkDir("/Root", "PQ");

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

        // The event filter counts TEvPersQueue events and, at the reboot
        // boundary, DROPS the boundary event and signals the main loop to
        // reboot the correspondent party (the partition tablet or the balancer,
        // depending on which pipe the boundary event flows through).
        //
        // HOW IT WORKS:
        // 1. The filter classifies each TEvPersQueue event as flowing through
        //    the partition tablet's pipe (isPqTabletEvent) or the balancer's
        //    pipe (isBalancerEvent), based on the pipe actor sets populated
        //    from TEvClientConnected events.
        // 2. When the effective event count reaches the reboot target, the
        //    filter DROPS the boundary event (return true) and sets
        //    RebootTriggered + RebootTargetTabletId. The boundary event is
        //    lost — exactly as a production tablet death loses in-flight
        //    events.
        // 3. The filter does NOT drop events while the reboot is in progress.
        //    In production a tablet death is not "silent dropping" — the
        //    tablet process dies, its pipe dies, and the client receives
        //    TEvClientDestroyed, which is what triggers the client's own
        //    retry/reconnect logic. Silently dropping an event (especially
        //    EvRequest) would strand the sender waiting forever for a response
        //    that never arrives.
        // 4. The main loop's CustomFinalCondition detects RebootTriggered,
        //    stops DispatchEvents, and calls RebootTablet() — the standard
        //    test-framework reboot that sends a poison pill through the tablet
        //    resolver (proper death path), waits for EvBoot (ensuring the
        //    tablet reboots), invalidates the resolver cache (so clients
        //    reconnect to the new instance), and waits for scheduled events.
        //    During RebootTablet()'s internal dispatch, events that reach the
        //    dead tablet fail naturally (pipe disconnected), and events that
        //    arrive after EvBoot reach the new instance — exactly as in
        //    production.
        // 5. After RebootTablet() completes, the main loop sets
        //    RebootCompleted=true (only to avoid re-triggering the reboot).
        //    New pipes connect to the rebooted tablet, and the SDK's retry
        //    logic recovers — exactly as in production.
        //
        // WHY NOT REPLACE WITH A POISON PILL (previous approach): the poison
        // pill kills the tablet actor but doesn't go through the tablet
        // resolver, so the launcher may not detect the death and may not
        // reboot the tablet. The pipe doesn't die (the pipe client actor is
        // still alive), so the sender gets no TEvClientDestroyed notification.
        // The tablet resolver cache stays stale, so clients keep resolving to
        // the dead actor. This caused infinite pipe restart loops, hangs, and
        // lost messages.
        //
        // WHY NOT JUST DROP THE EVENT AND KEEP THE PIPE ALIVE: dropping the
        // event while the pipe stays alive has no production counterpart. The
        // sender would keep using the live pipe and the protocol state would
        // diverge. The fix addresses this by dropping ALL subsequent events
        // until the reboot completes — the tablet is effectively dead during
        // that window, and the pipe fails when the sender tries to use it
        // (because RebootTablet invalidates the resolver cache and the tablet
        // actor is replaced).
        runtime.SetEventFilter([this](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            // Track pipe actors (client + server) whose target tablet is the PQ
            // tablet or the balancer tablet. TEvClientConnected is delivered to
            // the pipe owner once the pipe is established and carries both actor
            // IDs together with the target TabletId.
            if (auto* connected = ev->CastAsLocal<TEvTabletPipe::TEvClientConnected>()) {
                if (connected->TabletId == PqTabletId) {
                    PqTabletPipeActors.insert(connected->ClientId);
                    PqTabletPipeActors.insert(connected->ServerId);
                }
                if (connected->TabletId == PqrbTabletId) {
                    PqBalancerPipeActors.insert(connected->ClientId);
                    PqBalancerPipeActors.insert(connected->ServerId);
                }
                return false; // pipe connect notifications are never counted
            }

            // Track the current actor IDs of the PQ tablet and the balancer
            // tablet: TEvTablet::EvBoot is delivered to the NEW tablet actor
            // after every (re)boot, so its recipient is the up-to-date actor ID
            // the poison pill must be addressed to.
            if (ev->Type == TEvTablet::EvBoot) {
                if (auto* boot = ev->CastAsLocal<TEvTablet::TEvBoot>()) {
                    if (boot->TabletID == PqTabletId) {
                        PqTabletActorId = ev->GetRecipientRewrite();
                    } else if (boot->TabletID == PqrbTabletId) {
                        PqrbActorId = ev->GetRecipientRewrite();
                    }
                }
                return false;
            }

            // Only the TEvPersQueue event space (EvRequest..EvEnd) is counted: these
            // are the events that flow through the PQ tablet pipes and can be
            // interrupted by a tablet reboot in a real system. All other events
            // (system events like Bootstrap, internal actor events, etc.) are never
            // interrupted by a tablet reboot, so they are never counted.
            const bool isPqRequestOrResponse =
                ev->Type >= TEvPersQueue::EvRequest && ev->Type < TEvPersQueue::EvEnd;
            if (!isPqRequestOrResponse) {
                return false; // let the event through, don't count it
            }

            // If neither reboot switch is set, no reboot is expected — skip
            // counting and blocking entirely.
            if (!DoRebootPqTablet && !DoRebootPqrbTablet) {
                return false;
            }

            // IMPORTANT: the filter does NOT drop events while a reboot is in
            // progress. In production, a tablet death is not simulated by
            // "silently dropping" events — the tablet process dies, so its pipe
            // dies too, and the client receives TEvClientDestroyed, which is
            // what triggers the client's own retry/reconnect logic. Silently
            // dropping an event (especially EvRequest, the write request) would
            // strand the sender waiting forever for a response that never
            // arrives, because the sender never learns the pipe died.
            //
            // RebootTablet() reproduces the production death path exactly:
            // poison pill through the tablet resolver kills the tablet, the
            // pipe dies (clients get TEvClientDestroyed and retry), EvBoot is
            // waited for, and the resolver cache is invalidated so retries
            // reconnect to the NEW tablet instance. Events that arrive during
            // this window fail naturally against the dead tablet — no filter
            // intervention needed.

            // Determine which tablet this event is flowing to/from.
            // An event is "for the PQ tablet" if its sender or recipient is a
            // PQ tablet pipe actor or the PQ tablet actor itself. Similarly for
            // the balancer tablet.
            const auto& dest = ev->GetRecipientRewrite();
            const auto& src = ev->Sender;
            const bool isPqTabletEvent =
                PqTabletPipeActors.contains(dest) || PqTabletPipeActors.contains(src)
                || dest == PqTabletActorId || src == PqTabletActorId;
            const bool isBalancerEvent =
                PqBalancerPipeActors.contains(dest) || PqBalancerPipeActors.contains(src);

            // Count events per tablet. When both switches are set, the target
            // is the sum of both counters (total events across both pipes).
            // When only one switch is set, only that tablet's counter matters.
            if (DoRebootPqTablet && isPqTabletEvent) {
                ++PqTabletEventCount;
            }
            if (DoRebootPqrbTablet && isBalancerEvent) {
                ++PqBalancerEventCount;
            }

            // The effective count for boundary detection:
            // - Both switches: sum of both counters
            // - Only tablet: PQ tablet counter
            // - Only balancer: balancer counter
            const ui64 effectiveCount =
                (DoRebootPqTablet && DoRebootPqrbTablet)
                    ? (PqTabletEventCount.load() + PqBalancerEventCount.load())
                    : (DoRebootPqTablet ? PqTabletEventCount.load() : PqBalancerEventCount.load());

            const char* name = PqEventTypeName(ev->Type);
            ++PqEventTypeCount[name];
            // Debug: log the first few counted PQ events for troubleshooting.
            static constexpr ui64 DebugLogLimit = 100;
            const ui64 target = RebootAfterEventCount.load();
            if (target > 0 && effectiveCount <= DebugLogLimit) {
                Cerr << "=== EVENT_FILTER effCount=" << effectiveCount
                     << " pqCount=" << PqTabletEventCount.load()
                     << " balCount=" << PqBalancerEventCount.load()
                     << " target=" << target
                     << " ev=" << name << "(#" << PqEventTypeCount[name] << ")"
                     << " type=" << ev->Type
                     << " dest=" << dest
                     << " src=" << src
                     << " isPq=" << isPqTabletEvent
                     << " isBal=" << isBalancerEvent << Endl;
            }

            // Reboot at the event boundary: DROP the boundary event and
            // signal the main loop to reboot the correspondent party — the
            // partition tablet (if the boundary event flows through the PQ
            // tablet's pipe) or the balancer (if it flows through the
            // balancer's pipe). The boundary event is lost — exactly as a
            // production tablet death loses the events that are in flight at
            // that moment. The main loop calls RebootTablet() which kills the
            // tablet through the tablet resolver (proper death path), waits
            // for EvBoot (ensuring reboot), and invalidates the resolver cache.
            if (target > 0 && effectiveCount >= target && !RebootTriggered.load()) {
                RebootTriggered.store(true);
                RebootCompleted.store(false);
                // Classify which tablet to reboot based on which pipe the
                // boundary event flows through. This is the essential design:
                // reboot the partition tablet when the boundary event is on
                // the partition tablet's pipe, reboot the balancer when the
                // boundary event is on the balancer's pipe.
                const ui64 tabletToReboot = isPqTabletEvent ? PqTabletId
                                          : isBalancerEvent ? PqrbTabletId
                                                            : 0;
                RebootTargetTabletId.store(tabletToReboot);
                Cerr << "=== REBOOT_BOUNDARY effCount=" << effectiveCount
                     << " pqCount=" << PqTabletEventCount.load()
                     << " balCount=" << PqBalancerEventCount.load()
                     << " target=" << target << " ev=" << name
                     << " isPq=" << isPqTabletEvent
                     << " isBal=" << isBalancerEvent
                     << " tabletToReboot=" << tabletToReboot << Endl;
                // DROP the boundary event — it's lost in the "reboot".
                // Subsequent events are also dropped (by the RebootTriggered
                // && !RebootCompleted check above) until the main loop completes
                // RebootTablet() and sets RebootCompleted.
                return true;
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

    void ResetCounters() {
        PqTabletEventCount.store(0);
        PqBalancerEventCount.store(0);
        PqEventTypeCount.clear();
        RebootPoint.store(0);
        PartitionReadyCount.store(0);
        ErrorCloseSession.store(0);
        ErrorCloseReason.clear();
        RebootTriggered.store(false);
        RebootTargetTabletId.store(0);
        RebootCompleted.store(false);
        DroppedInRebootCount.store(0);
        PqTabletPipeActors.clear();
        PqBalancerPipeActors.clear();
        TabletBootCount.store(0);
        // The tablet actor IDs change after each reboot (RebootTablet kills
        // the actor via the tablet resolver; the launcher boots a new one).
        // Re-resolve them here so the event filter tracks the current instances
        // for event classification. (A previous rebootPoint iteration may have
        // rebooted the tablets, giving them new actor IDs.)
        PqTabletActorId = ResolveTablet(Runtime(), PqTabletId, /*nodeIndex=*/0, /*sysTablet=*/false);
        PqrbActorId = ResolveTablet(Runtime(), PqrbTabletId, /*nodeIndex=*/0, /*sysTablet=*/false);
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
    // SeqNos delivered (and content-verified) across all SDK read sessions
    // of the test. Every message is stamped with an explicit SeqNo equal to
    // its global write index, so coverage is tracked by SeqNo (stable across
    // tablet reboots, unlike offsets which may be re-assigned on retry).
    // Filled by the SDK read handlers on their own threads, hence the mutex.
    THashSet<i64> VerifiedSeqNos;
    std::mutex VerifiedSeqNosMutex;

    // --- max_lag timing tracking ---
    // When MaxLagSeconds > 0, the partition tablet skips messages older than
    // max_lag (measured in simulated time via ctx.Now()). To predict which
    // messages could have been skipped, we record the simulated time at key
    // points during each scenario step execution. A message written at
    // simulated time W can be permanently skipped if a read session attempts
    // to read it at simulated time R where R - W > MaxLagSeconds. This happens
    // when balancer reboots delay read session recovery beyond the lag window.
    struct TStepTiming {
        i64 FirstSeqNo = 0;       // first SeqNo written in this step (1-based)
        i64 LastSeqNo = 0;        // last SeqNo written in this step
        TInstant WriteStartTime;  // simulated time at step start (before write)
        TInstant StepEndTime;     // simulated time at step end (after reads)
    };
    TVector<TStepTiming> StepTimings;
    ui64 ScenarioMaxLagSeconds = 0;  // max_lag setting from the scenario (0 = none)

    // Offset to start reading from in the current run. Each reboot-point run
    // should only read messages written in that run, not from offset 0. This
    // is set to the total messages written before the current run starts, so
    // the read sessions skip past old messages via StartPartitionSessionResponse.read_offset.
    i64 RunStartOffset = 0;

    void SetUp(NUnitTest::TTestContext&) override {
        Env.Start();
        Env.InstallHooks();
        TotalWrittenMessages.store(0);
        VerifiedSeqNos.clear();
        StepTimings.clear();
        ScenarioMaxLagSeconds = 0;
        RunStartOffset = 0;
    }

    void TearDown(NUnitTest::TTestContext&) override {
        TearDownGrpcAndServer(Env);
    }

    NActors::TTestActorRuntime& Runtime() {
        return Env.Runtime();
    }

    // Implementation of WriteMessages — the body that runs on the background thread.
    // Separated so that RunWithDispatchAndReboot can run it while interleaving
    // reboots at event boundaries.
    //
    // Pure gRPC write via Ydb::Topic::V1::TopicService::StreamWrite — the
    // same protocol the SDK speaks internally, but WITHOUT the SDK: the
    // SDK runs its own real threads whose interaction with the simulated
    // actor runtime (UseRealThreads=false) is unreliable. The raw stream
    // works directly against the server's gRPC proxy actors, which are
    // served by the main thread's DispatchEvents pump.
    //
    // The whole write session (init + writes + acks) is retried with a fresh
    // gRPC stream on failure — the same client-side retry pattern as
    // TPQDataWriter::WaitWritePQServiceInitialization. A tablet reboot breaks
    // the stream mid-write; the retry re-inits and re-sends. The server
    // deduplicates by (producer, SeqNo), so re-sending already persisted
    // messages is safe.
    void WriteMessagesImpl(ui64 count, ui64 messageSize) {
        // Large messages (1MB each) exceed gRPC's default 4MB limits when
        // several arrive in one response; raise both limits.
        grpc::ChannelArguments args;
        args.SetMaxReceiveMessageSize(64 * 1024 * 1024);
        args.SetMaxSendMessageSize(64 * 1024 * 1024);
        auto channel = grpc::CreateCustomChannel(
            Env.Endpoint, grpc::InsecureChannelCredentials(), args);
        auto stub = Ydb::Topic::V1::TopicService::NewStub(channel);

        using FClient = Ydb::Topic::StreamWriteMessage::FromClient;
        using FServer = Ydb::Topic::StreamWriteMessage::FromServer;

        // Prebuild the message bodies once: a fixed marker prefix followed
        // by the global message index, which is also the explicit SeqNo of
        // the message (1-based: the server rejects seq_no == 0). Using an
        // explicit SeqNo means the server deduplicates retries by
        // (producer, SeqNo) after a tablet reboot, and the read path can
        // strictly verify that the delivered message content matches its
        // SeqNo — independent of the offset the tablet happened to assign
        // (offsets can diverge from the write index after a reboot + retry
        // re-assigns it).
        const ui64 baseIndex = TotalWrittenMessages.load();
        TVector<TString> bodies;
        bodies.reserve(count);
        for (ui64 i = 0; i < count; ++i) {
            const ui64 seqNo = baseIndex + i + 1;
            TString data = Sprintf("MSG-%020lu", (unsigned long)seqNo);
            // Pad to the requested message size (keep the marker at the start).
            if (data.size() < messageSize) {
                data.resize(messageSize, 'x');
            }
            bodies.push_back(std::move(data));
        }

        // Retry the whole write session on failure (stream break, deadline,
        // non-SUCCESS status). Bounded so the outer wall-clock timeout in
        // RunWithDispatchAndReboot still fires on a permanently broken path.
        for (ui32 attempt = 1;; ++attempt) {
            try {
                grpc::ClientContext context;
                context.set_deadline(
                    std::chrono::system_clock::now() + std::chrono::seconds(10));
                context.AddMetadata("x-ydb-database", "/" + Env.Server->ServerSettings.DomainName);
                auto stream = stub->StreamWrite(&context);
                TR_ENSURE(stream);

                // 1. Init request.
                FClient req;
                req.mutable_init_request()->set_path(kTopicPath);
                req.mutable_init_request()->set_producer_id("src");
                req.mutable_init_request()->set_message_group_id("src");
                TR_ENSURE(stream->Write(req));

                FServer resp;
                TR_ENSURE(stream->Read(&resp));
                if (resp.status() != Ydb::StatusIds::SUCCESS) {
                    ythrow yexception() << "write init failed: status=" << resp.status()
                        << " issues=" << resp.ShortDebugString();
                }
                TR_ENSURE(resp.has_init_response());

                // 2. Write all messages (one per WriteRequest, RAW codec).
                for (ui64 i = 0; i < count; ++i) {
                    req.Clear();
                    auto* wr = req.mutable_write_request();
                    wr->set_codec(Ydb::Topic::CODEC_RAW);
                    auto* msg = wr->add_messages();
                    msg->set_seq_no(baseIndex + i + 1);
                    msg->set_data(bodies[i]);
                    msg->set_uncompressed_size(bodies[i].size());
                    TR_ENSURE(stream->Write(req));
                }

                // 3. Read the acks (one WriteResponse per WriteRequest).
                for (ui64 i = 0; i < count; ++i) {
                    TR_ENSURE(stream->Read(&resp));
                    if (resp.status() != Ydb::StatusIds::SUCCESS) {
                        ythrow yexception() << "write ack failed at message " << i
                            << ": status=" << resp.status()
                            << " issues=" << resp.ShortDebugString();
                    }
                    TR_ENSURE(resp.has_write_response());
                    TR_ENSURE(resp.write_response().acks_size() == 1);
                }

                stream->WritesDone();
                const auto status = stream->Finish();
                if (!status.ok()) {
                    ythrow yexception() << "write finish failed: " << status.error_message();
                }

                TotalWrittenMessages.fetch_add(count);
                return;
            } catch (const std::exception& e) {
                Cerr << "=== WRITE_RETRY attempt=" << attempt
                     << " error=" << e.what() << Endl;
                if (attempt >= 1000) {
                    ythrow yexception() << "write failed after " << attempt
                        << " attempts: " << e.what();
                }
                Sleep(TDuration::MilliSeconds(100));
            }
        }
    }

    void AssertNoErrorClose(const TString& what) {
        UNIT_ASSERT_C(Env.ErrorCloseSession.load() == 0,
            what << "; reason=" << Env.ErrorCloseReason);
    }

    // Final correctness check: every written message must have been
    // delivered and content-verified by the sweep's read steps (SeqNos
    // accumulated in VerifiedSeqNos). No re-reading from the tablet.
    //
    // When MaxLagSeconds > 0, the partition tablet skips messages older than
    // max_lag (measured in simulated time via ctx.Now() - maxTimeLagMs in
    // GetReadFrom). Balancer reboots delay read session recovery, and if the
    // delay exceeds max_lag, previously-unread messages become "too old" and
    // are permanently skipped. This is expected behavior, not a bug. So when
    // max_lag is set, we predict which messages could have been skipped based
    // on the simulated time recorded during each step, and validate that:
    //   1. Every missing message was eligible for skipping (old enough).
    //   2. Every non-skippable message was delivered.
    //   3. The set of missing messages is a subset of the skippable set.
    void VerifyFullCoverage(const TString& label) {
        // Only check messages from the current reboot-point run: each run
        // writes messages with SeqNos (RunStartOffset+1 .. TotalWrittenMessages),
        // and the read sessions start from RunStartOffset via read_offset.
        // Messages from previous runs are not re-read or re-verified.
        const i64 firstSeqNo = RunStartOffset + 1;
        const i64 total = static_cast<i64>(TotalWrittenMessages.load());
        const i64 runTotal = total - RunStartOffset;  // messages in this run

        // Collect missing SeqNos (only for the current run).
        THashSet<i64> missing;
        {
            std::lock_guard<std::mutex> lock(VerifiedSeqNosMutex);
            for (i64 seqNo = firstSeqNo; seqNo <= total; ++seqNo) {
                if (!VerifiedSeqNos.contains(seqNo)) {
                    missing.insert(seqNo);
                }
            }
        }

        if (ScenarioMaxLagSeconds == 0) {
            // No max_lag: every message must be delivered.
            i64 firstMissing = missing.empty() ? -1 : *missing.begin();
            UNIT_ASSERT_C(missing.empty(), label << ": " << missing.size() << " of " << runTotal
                << " messages never delivered; first missing SeqNo: " << firstMissing);
            AssertNoErrorClose(label);
            return;
        }

        // max_lag > 0: predict which messages could have been skipped.
        // A message written in step S (at simulated time ~WriteStartTime) can
        // be skipped if a later step S' starts reading at a simulated time
        // more than MaxLagSeconds after the message's write time. Since the
        // partition tablet uses ctx.Now() - maxTimeLagMs as the read timestamp
        // threshold, any message with WriteTimestamp < ctx.Now() - max_lag is
        // skipped. We approximate the write timestamp by the step's start time
        // and the read time by the next step's start time (or the current
        // step's end time for the last step).
        //
        // Note: this is a conservative over-approximation. Not all messages
        // in a skippable step will actually be skipped — if some read
        // sessions deliver them before the lag window expires, they survive.
        // The validation checks that every missing message is in the skippable
        // set (no unexpected losses) and that the missing count is reasonable.
        const TDuration maxLag = TDuration::Seconds(ScenarioMaxLagSeconds);
        THashSet<i64> skippable;
        for (size_t i = 0; i < StepTimings.size(); ++i) {
            const auto& st = StepTimings[i];
            if (st.LastSeqNo < st.FirstSeqNo) {
                continue; // no messages written in this step
            }
            // The earliest time a read session from a LATER step could
            // attempt to read these messages. For step i, the next step's
            // start time is the earliest "late read" opportunity. If there
            // is no next step, use this step's end time.
            TInstant lateReadTime = st.StepEndTime;
            if (i + 1 < StepTimings.size()) {
                lateReadTime = StepTimings[i + 1].WriteStartTime;
            }
            // If the late read time exceeds the write time + max_lag, messages
            // from this step are eligible for skipping.
            if (lateReadTime - st.WriteStartTime > maxLag) {
                for (i64 seqNo = st.FirstSeqNo; seqNo <= st.LastSeqNo; ++seqNo) {
                    skippable.insert(seqNo);
                }
            }
        }

        // Validate: every missing message must be in the skippable set.
        TVector<i64> unexpectedMissing;
        for (i64 seqNo : missing) {
            if (!skippable.contains(seqNo)) {
                unexpectedMissing.push_back(seqNo);
            }
        }

        // Log the timing analysis for diagnostics.
        Cerr << "=== MAX_LAG_ANALYSIS label=" << label
             << " maxLagSeconds=" << ScenarioMaxLagSeconds
             << " runTotal=" << runTotal
             << " firstSeqNo=" << firstSeqNo
             << " missing=" << missing.size()
             << " skippable=" << skippable.size()
             << " unexpectedMissing=" << unexpectedMissing.size()
             << Endl;
        for (size_t i = 0; i < StepTimings.size(); ++i) {
            const auto& st = StepTimings[i];
            TInstant lateReadTime = st.StepEndTime;
            if (i + 1 < StepTimings.size()) {
                lateReadTime = StepTimings[i + 1].WriteStartTime;
            }
            Cerr << "  step[" << i << "] seqNos=" << st.FirstSeqNo << "-" << st.LastSeqNo
                 << " writeStart=" << st.WriteStartTime
                 << " stepEnd=" << st.StepEndTime
                 << " lateRead=" << lateReadTime
                 << " age=" << (lateReadTime - st.WriteStartTime)
                 << " skippable=" << (lateReadTime - st.WriteStartTime > maxLag ? "YES" : "no")
                 << Endl;
        }
        if (!missing.empty()) {
            Cerr << "  missing SeqNos:";
            for (i64 seqNo : missing) {
                Cerr << " " << seqNo;
            }
            Cerr << Endl;
        }

        UNIT_ASSERT_C(unexpectedMissing.empty(),
            label << ": " << unexpectedMissing.size() << " of " << missing.size()
            << " missing messages were NOT eligible for max_lag skipping"
            << " (maxLagSeconds=" << ScenarioMaxLagSeconds << ")"
            << "; first unexpected missing SeqNo: "
            << (unexpectedMissing.empty() ? -1 : unexpectedMissing[0]));

        // Also validate that the missing count does not exceed the skippable
        // count (sanity bound — all missing must be within the skippable set).
        UNIT_ASSERT_C(missing.size() <= skippable.size(),
            label << ": missing=" << missing.size() << " > skippable=" << skippable.size()
            << " (maxLagSeconds=" << ScenarioMaxLagSeconds << ")");

        AssertNoErrorClose(label);
    }

    // Shared state of one pure-gRPC read session (see GrpcReadSessionLoop).
    struct TGrpcReadSessionState {
        TString Consumer;
        std::atomic<i64> LastSeqNo{0};        // highest SeqNo delivered (1-based)
        std::atomic<ui64> DeliveredCount{0};  // messages delivered (incl. redelivered)
        std::mutex Mutex;                     // guards Ctx / Exception
        std::shared_ptr<grpc::ClientContext> Ctx; // current attempt's context (for TryCancel)
        std::exception_ptr Exception;         // set on fatal (content mismatch)
    };

    // Implementation of the Concurrent scenario step — runs on the background
    // thread; the caller (RunWithDispatch or RunWithDispatchAndReboot) pumps
    // DispatchEvents on the main thread and may reboot the tablet at any
    // event boundary. IMPORTANT: this code MUST NOT dispatch events itself
    // (no RunWithDispatch / WaitPromise here) — it runs inside the outer
    // dispatch loop; it may only block (sleep/poll) while the main thread
    // pumps the actor system.
    //
    // Pure gRPC read sessions via Ydb::Topic::V1::TopicService::StreamRead —
    // the same protocol the SDK speaks internally, but WITHOUT the SDK: the
    // SDK runs its own real threads whose interaction with the simulated
    // actor runtime (UseRealThreads=false) is unreliable. Each read session
    // runs on its own dispatch-pool thread and blocks on the raw gRPC stream;
    // the main thread's DispatchEvents pump serves the server side.
    //
    // Opens ReadSessionCount read sessions (each with its own consumer so
    // the balancer assigns the partition independently), optionally launches
    // a write while they are open, then waits for each session to deliver
    // everything written so far. Every delivered message is content-verified
    // on arrival (same invariants as AccumulateDelivered).
    bool ConcurrentReadWriteStepImpl(const TScenarioStep& step) {
        TVector<std::unique_ptr<TGrpcReadSessionState>> states;
        states.reserve(step.ReadSessionCount);
        for (ui32 i = 0; i < step.ReadSessionCount; ++i) {
            auto st = std::make_unique<TGrpcReadSessionState>();
            st->Consumer = (step.ReadSessionCount > 1 && i > 0)
                ? Sprintf("user%u", i - 1)
                : TString(kConsumer);
            states.push_back(std::move(st));
        }

        // The total number of messages that will exist after this step's
        // write completes; each read session finishes once it has delivered
        // that many (SeqNos are global and 1-based).
        const i64 expectedTotal =
            static_cast<i64>(TotalWrittenMessages.load() + step.Count);
        std::atomic<bool> abort{false};

        // Launch the read sessions first so they are concurrently active.
        // Pass RunStartOffset so the read sessions skip past messages from
        // previous reboot-point runs via StartPartitionSessionResponse.read_offset.
        TVector<NThreading::TFuture<bool>> readFutures;
        for (ui32 i = 0; i < step.ReadSessionCount; ++i) {
            TGrpcReadSessionState* st = states[i].get();
            const ui32 maxLagSeconds = step.ReadSettings.MaxLagSeconds;
            const i64 readOffset = RunStartOffset;
            readFutures.push_back(NThreading::Async(
                [this, st, expectedTotal, maxLagSeconds, readOffset, &abort] {
                    GrpcReadSessionLoop(st, expectedTotal, maxLagSeconds, readOffset, abort);
                    return true;
                }, DispatchPool()));
        }

        // Launch the write (if requested) while the read sessions are open.
        // It runs on the dispatch pool; the main thread serves its requests
        // while pumping events in the outer loop.
        NThreading::TFuture<bool> writeFuture;
        if (step.Count > 0) {
            writeFuture = NThreading::Async([&] {
                WriteMessagesImpl(step.Count, step.MessageSize);
                return true;
            }, DispatchPool());
        }

        // Wait (blocking on this pool thread — the MAIN thread keeps
        // pumping DispatchEvents in the outer RunWithDispatchAndReboot
        // loop, serving the gRPC requests and performing the event-boundary
        // reboots) until every session has delivered everything written so
        // far and the concurrent write (if any) has finished. The 25s
        // deadline is a diagnostics bound; the outer wall-clock timeout in
        // RunWithDispatchAndReboot is the real backstop.
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(25);
        bool gotData = false;
        while (true) {
            bool writeDone = (step.Count == 0) || writeFuture.HasValue() || writeFuture.HasException();
            const ui64 totalNow = TotalWrittenMessages.load();
            bool allSessionsDone = true;
            for (auto& st : states) {
                if (st->LastSeqNo.load() < static_cast<i64>(totalNow)) {
                    allSessionsDone = false;
                }
            }
            if (writeDone && allSessionsDone) {
                gotData = true;
                break;
            }
            if (TInstant::Now() >= deadline) {
                for (size_t i = 0; i < states.size(); ++i) {
                    Cerr << "=== GRPC_READ_SESSION_TIMEOUT session=" << i
                         << " consumer=" << states[i]->Consumer
                         << " lastSeqNo=" << states[i]->LastSeqNo.load()
                         << " delivered=" << states[i]->DeliveredCount.load()
                         << " totalNow=" << totalNow << Endl;
                }
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }

        // Finish the read sessions. On the SUCCESS path no cancellation is
        // needed: a session that delivered everything exits on its own (its
        // loop condition LastSeqNo >= expectedTotal is already false), so
        // the threads are simply joined — no stream write ever fails due to
        // our actions. Cancellation (TryCancel to unblock a stuck stream
        // Read) is applied ONLY on the deadline path, where the step has
        // already failed and the session state is only needed for logging.
        abort.store(true);
        if (!gotData) {
            for (auto& st : states) {
                std::lock_guard<std::mutex> lock(st->Mutex);
                if (st->Ctx) {
                    st->Ctx->TryCancel();
                }
            }
        }
        for (auto& fut : readFutures) {
            fut.GetValueSync();
        }
        if (gotData) {
            // Success: a session exception here is a real defect
            // (content-verification failure) — rethrow it.
            for (auto& st : states) {
                std::lock_guard<std::mutex> lock(st->Mutex);
                if (st->Exception) {
                    std::rethrow_exception(st->Exception);
                }
            }
        } else {
            // Deadline path: the step already failed (the
            // GRPC_READ_SESSION_TIMEOUT lines above carry the diagnosis).
            // Log any session exceptions — including spurious check failures
            // caused by our own TryCancel — instead of masking the timeout.
            for (auto& st : states) {
                std::lock_guard<std::mutex> lock(st->Mutex);
                if (st->Exception) {
                    Cerr << "=== GRPC_READ_SESSION_EXCEPTION consumer="
                         << st->Consumer << " error=";
                    try {
                        std::rethrow_exception(st->Exception);
                    } catch (const std::exception& e) {
                        Cerr << e.what();
                    } catch (...) {
                        Cerr << "unknown";
                    }
                    Cerr << Endl;
                }
            }
        }

        // Wait for the concurrent write to finish (rethrows write failures).
        if (step.Count > 0) {
            writeFuture.GetValueSync();
        }

        return gotData;
    }

    // Body of one pure-gRPC read session (runs on its own dispatch-pool
    // thread). Reconnects with a fresh stream whenever the current one ends
    // (e.g. the balancer or partition tablet rebooted and the server closed
    // the session) until the session has delivered expectedTotal messages or
    // the step sets abort. Commits offsets as it goes so a reconnect resumes
    // from the committed position, not from zero.
    void GrpcReadSessionLoop(TGrpcReadSessionState* st, i64 expectedTotal,
                             ui32 maxLagSeconds, i64 readOffset,
                             std::atomic<bool>& abort)
    {
        while (!abort.load() && st->LastSeqNo.load() < expectedTotal) {
            bool delivered = false;
            try {
                delivered = GrpcReadSessionAttempt(st, expectedTotal, maxLagSeconds, readOffset, abort);
            } catch (...) {
                std::lock_guard<std::mutex> lock(st->Mutex);
                if (!st->Exception) {
                    st->Exception = std::current_exception();
                }
                return;
            }
            if (delivered || abort.load()) {
                return;
            }
            // The stream ended before everything was delivered (e.g. the
            // balancer or partition tablet rebooted): pause briefly and
            // reconnect with a fresh session, resuming from the committed
            // offset.
            Sleep(TDuration::MilliSeconds(100));
        }
    }

    // One read-session attempt: a fresh StreamRead gRPC stream from init to
    // stream end. Returns true when the session has delivered expectedTotal
    // messages or the step aborted; returns false when the stream ended
    // before that (transient — the caller reconnects). Throws only on fatal
    // errors (content-verification failure).
    bool GrpcReadSessionAttempt(TGrpcReadSessionState* st, i64 expectedTotal,
                                 ui32 maxLagSeconds, i64 readOffset,
                                 std::atomic<bool>& abort)
    {
        grpc::ChannelArguments args;
        args.SetMaxReceiveMessageSize(64 * 1024 * 1024);
        args.SetMaxSendMessageSize(64 * 1024 * 1024);
        auto channel = grpc::CreateCustomChannel(
            Env.Endpoint, grpc::InsecureChannelCredentials(), args);
        auto stub = Ydb::Topic::V1::TopicService::NewStub(channel);

        using FClient = Ydb::Topic::StreamReadMessage::FromClient;
        using FServer = Ydb::Topic::StreamReadMessage::FromServer;
        constexpr i64 FlowControlBytes = 100 * 1024 * 1024;

        auto ctx = std::make_shared<grpc::ClientContext>();
        ctx->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
        ctx->AddMetadata("x-ydb-database", "/" + Env.Server->ServerSettings.DomainName);
        {
            std::lock_guard<std::mutex> lock(st->Mutex);
            st->Ctx = ctx;
        }
        auto stream = stub->StreamRead(ctx.get());
        TR_ENSURE(stream);

        // 1. Init request.
        FClient req;
        auto* init = req.mutable_init_request();
        auto* ts = init->add_topics_read_settings();
        ts->set_path(kTopicPath);
        if (maxLagSeconds > 0) {
            ts->mutable_max_lag()->set_seconds(maxLagSeconds);
        }
        init->set_consumer(st->Consumer);
        TR_ENSURE(stream->Write(req));

        FServer resp;
        TR_ENSURE(stream->Read(&resp));
        if (resp.status() != Ydb::StatusIds::SUCCESS) {
            return false; // transient (e.g. mid-reboot) — reconnect
        }
        TR_ENSURE(resp.has_init_response());

        // 2. Signal readiness for data (flow-control window).
        req.Clear();
        req.mutable_read_request()->set_bytes_size(FlowControlBytes);
        TR_ENSURE(stream->Write(req));

        // 3. Process server messages until the session has delivered
        // everything, the stream ends (reboot), or the step aborts.
        i64 partitionSessionId = -1;
        i64 commitStart = 0; // start of the uncommitted offset range
        while (!abort.load() && st->LastSeqNo.load() < expectedTotal) {
            if (!stream->Read(&resp)) {
                return false; // stream ended — reconnect
            }
            if (resp.status() != Ydb::StatusIds::SUCCESS) {
                return false; // session-level error — reconnect
            }
            switch (resp.server_message_case()) {
                case FServer::kStartPartitionSessionRequest: {
                    const auto& start = resp.start_partition_session_request();
                    partitionSessionId = start.partition_session().partition_session_id();
                    commitStart = start.committed_offset();
                    FClient r;
                    auto* respMsg = r.mutable_start_partition_session_response();
                    respMsg->set_partition_session_id(partitionSessionId);
                    // Skip past messages from previous reboot-point runs: set
                    // read_offset so the server starts delivering from the
                    // current run's first message, not from offset 0.
                    if (readOffset > commitStart) {
                        respMsg->set_read_offset(readOffset);
                        commitStart = readOffset;
                    }
                    TR_ENSURE(stream->Write(r));
                    // Re-signal readiness for this partition.
                    FClient rr;
                    rr.mutable_read_request()->set_bytes_size(FlowControlBytes);
                    TR_ENSURE(stream->Write(rr));
                    break;
                }
                case FServer::kReadResponse: {
                    const auto& readResp = resp.read_response();
                    i64 maxOffset = commitStart - 1;
                    for (const auto& pd : readResp.partition_data()) {
                        partitionSessionId = pd.partition_session_id();
                        for (const auto& batch : pd.batches()) {
                            for (const auto& md : batch.message_data()) {
                                // Content-verify every message on arrival:
                                // the write path stamps each message with
                                // "MSG-%020lu" carrying its explicit SeqNo, so
                                // the content must match the message's own
                                // SeqNo (NOT the offset — the offset can
                                // diverge from the write index after a
                                // reboot + retry re-assigns it). Redelivery
                                // (at-least-once after reconnect) is allowed
                                // and counted, but never breaks monotonic
                                // max tracking.
                                const i64 seqNo = md.seq_no();
                                const TString data(md.data());
                                const TString marker =
                                    Sprintf("MSG-%020lu", (unsigned long)seqNo);
                                if (data.size() < marker.size() || !data.StartsWith(marker)) {
                                    ythrow yexception() << "content mismatch at SeqNo "
                                        << seqNo << ": got \""
                                        << data.substr(0, 32) << "\"";
                                }
                                if (seqNo > st->LastSeqNo.load()) {
                                    st->LastSeqNo.store(seqNo);
                                }
                                st->DeliveredCount.fetch_add(1);
                                {
                                    std::lock_guard<std::mutex> lock(VerifiedSeqNosMutex);
                                    VerifiedSeqNos.insert(seqNo);
                                }
                                maxOffset = md.offset();
                            }
                        }
                    }
                    // Commit the processed range so a reconnect after a
                    // reboot resumes from the committed offset.
                    if (partitionSessionId >= 0 && maxOffset >= commitStart) {
                        FClient c;
                        auto* co = c.mutable_commit_offset_request()->add_commit_offsets();
                        co->set_partition_session_id(partitionSessionId);
                        auto* range = co->add_offsets();
                        range->set_start(commitStart);
                        range->set_end(maxOffset + 1);
                        TR_ENSURE(stream->Write(c));
                        commitStart = maxOffset + 1;
                    }
                    // Replenish the flow-control window.
                    FClient more;
                    more.mutable_read_request()->set_bytes_size(FlowControlBytes);
                    TR_ENSURE(stream->Write(more));
                    break;
                }
                case FServer::kStopPartitionSessionRequest: {
                    FClient r;
                    r.mutable_stop_partition_session_response()
                        ->set_partition_session_id(
                            resp.stop_partition_session_request().partition_session_id());
                    TR_ENSURE(stream->Write(r));
                    break;
                }
                default:
                    break; // commit acks, status updates — ignore
            }
        }
        return true; // delivered everything (or aborted)
    }

    // Run a lambda on the background dispatch pool while pumping DispatchEvents
    // on the main thread until the future completes. When the event filter
    // detects the reboot boundary, it drops the boundary event (the in-flight
    // event lost at the crash moment) and sets RebootTriggered +
    // RebootTargetTabletId. The CustomFinalCondition stops DispatchEvents, and
    // the main loop calls RebootTablet() — the standard test-framework reboot
    // that sends a poison pill through the tablet resolver (proper death path),
    // waits for EvBoot (ensuring the tablet reboots), invalidates the resolver
    // cache (so clients reconnect to the new instance), and waits for scheduled
    // events.
    //
    // This reproduces production: the tablet dies (poison pill via resolver),
    // its pipe dies with it (clients receive TEvClientDestroyed and retry),
    // events that reach the dead tablet during the reboot window fail
    // naturally, the tablet reboots (EvBoot waited for), and the SDK's retry
    // logic reconnects to the new instance and recovers. No event filtering is
    // needed during the reboot window — the natural pipe death does the work.
    template <typename TFunc>
    auto RunWithDispatchAndReboot(TFunc&& func, TDuration stepTimeout = TDuration::Minutes(2)) {
        auto& runtime = Runtime();
        auto future = NThreading::Async(std::forward<TFunc>(func), DispatchPool());

        // Wall-clock deadline to prevent individual scenario steps from hanging
        // indefinitely (e.g., due to infinite dispatch loops or stuck reboots).
        const TInstant deadline = TInstant::Now() + stepTimeout;

        while (!future.HasValue() && !future.HasException()) {
            // Check wall-clock timeout before each dispatch iteration.
            if (TInstant::Now() >= deadline) {
                // Flush per-event-type counters before throwing so the failure
                // that matters most (a hung step) still has a readable summary.
                DumpEventCounterSummary(Env.RebootPoint.load());
                UNIT_ASSERT_C(false,
                    "Scenario step exceeded wall-clock timeout of " << stepTimeout
                    << " (deadline=" << deadline << ", now=" << TInstant::Now() << ")");
            }
            TDispatchOptions options;
            // Stop when the future completes (the step is done) OR when a
            // reboot is PENDING (the filter set RebootTriggered and the main
            // loop has not performed the reboot yet). Once RebootCompleted is
            // set, dispatch MUST resume pumping events: the server-side
            // actors (KV storage, partition, balancer) only progress while
            // DispatchEvents runs, and the gRPC clients block until the
            // server responds. Stopping on RebootTriggered alone would freeze
            // the whole actor system after the first boundary.
            options.CustomFinalCondition = [&]() {
                return future.HasValue() || future.HasException()
                    || (Env.RebootTriggered.load() && !Env.RebootCompleted.load());
            };
            // Quirk: non-empty FinalEvents enables full simulation (same as
            // WaitFuture). Use a dummy that never fires.
            options.FinalEvents.emplace_back([](IEventHandle&) { return false; });

            // Use a short timeout so DispatchEvents returns periodically.
            // Without this, DispatchEvents can block forever when the
            // CustomFinalCondition does not fire (e.g., a hung step). The
            // periodic return allows the wall-clock timeout check at the top
            // of the loop to run.
            DispatchEventsWithRetry([&] {
                runtime.DispatchEvents(options, TDuration::Seconds(1));
            });

            // If the reboot boundary was hit, reboot the tablet now from the
            // main thread (which CAN dispatch, unlike the event filter).
            // RebootTablet() reproduces the production death path: the poison
            // pill kills the tablet, the pipe dies (clients get
            // TEvClientDestroyed), EvBoot is waited for, and the resolver cache
            // is invalidated so clients reconnect to the new instance. Events
            // during this window fail naturally — no filter dropping needed.
            if (Env.RebootTriggered.load() && !Env.RebootCompleted.load()
                && !future.HasValue() && !future.HasException())
            {
                const ui64 tabletId = Env.RebootTargetTabletId.load();
                if (tabletId != 0) {
                    const TInstant rebootStart = TInstant::Now();
                    Cerr << "=== REBOOT_TABLET tabletId=" << tabletId
                         << " start=" << rebootStart << Endl;
                    auto sender = runtime.AllocateEdgeActor();
                    // RebootTablet: sends poison pill via tablet resolver
                    // (proper death path), waits for EvBoot (ensuring reboot),
                    // invalidates resolver cache (clients reconnect to the new
                    // instance), waits for scheduled events.
                    RebootTablet(runtime, tabletId, sender,
                                 /*nodeIndex=*/0, /*sysTablet=*/false);
                    const TInstant rebootEnd = TInstant::Now();
                    Cerr << "=== REBOOT_COMPLETED tabletId=" << tabletId
                         << " elapsedMs=" << (rebootEnd - rebootStart).MilliSeconds()
                         << " bootCount=" << Env.TabletBootCount.load() << Endl;
                } else {
                    Cerr << "=== REBOOT_TABLET tabletId=0 (boundary event was"
                            " neither PQ nor balancer!)" << Endl;
                }
                // Mark the reboot complete so the loop doesn't re-trigger it.
                Env.RebootCompleted.store(true);
            }
        }

        Y_ABORT_UNLESS(future.HasValue() || future.HasException());
        if constexpr (std::is_same_v<decltype(future.GetValueSync()), void>) {
            future.GetValueSync();
        } else {
            return future.GetValueSync();
        }
    }

    // Execute a scenario step (a concurrent write+read). Returns true if the
    // step completed successfully; stores whether all expected data was
    // delivered in dataReceived.
    //
    // When a reboot target is set (RebootAfterEventCount > 0), the step is
    // executed via RunWithDispatchAndReboot so that the tablet is rebooted at
    // the target event boundary during the step. When no reboot target is set,
    // the step runs normally via RunWithDispatch.
    bool ExecuteScenarioStep(const TScenarioStep& step, bool* dataReceived = nullptr) {
        Cerr << "=== EXECUTE_STEP type=" << static_cast<int>(step.Type)
             << " rebootTarget=" << Env.RebootAfterEventCount.load() << Endl;

        // Record the simulated time at step start and the SeqNo range that
        // will be written in this step. The write timestamp of each message
        // is approximately the step's start time (the partition tablet stamps
        // WriteTimestampMS from ctx.Now() at write time). This is used by
        // VerifyFullCoverage to predict which messages could have been
        // skipped due to max_lag.
        TStepTiming timing;
        timing.FirstSeqNo = static_cast<i64>(TotalWrittenMessages.load()) + 1;
        timing.LastSeqNo = static_cast<i64>(TotalWrittenMessages.load() + step.Count);
        timing.WriteStartTime = Runtime().GetCurrentTime();

        bool gotData;
        if (Env.RebootAfterEventCount.load() > 0) {
            gotData = RunWithDispatchAndReboot([&] {
                return ConcurrentReadWriteStepImpl(step);
            });
        } else {
            gotData = RunWithDispatch(Runtime(), [&] {
                return ConcurrentReadWriteStepImpl(step);
            });
        }

        // Record the simulated time at step end (after all reads completed).
        timing.StepEndTime = Runtime().GetCurrentTime();
        StepTimings.push_back(timing);

        if (dataReceived) {
            *dataReceived = gotData;
        }

        AssertNoErrorClose("scenario concurrent step");
        UNIT_ASSERT_C(gotData,
            "scenario concurrent step: not all expected data delivered");
        return true;
    }

    // Run the full scenario once with an event-boundary reboot at the
    // rebootPoint-th counted event: the event filter drops that event and
    // signals the main loop to call RebootTablet() for the correspondent
    // party (the partition tablet or the balancer, depending on which pipe
    // the boundary event flows through). The tablet dies via the tablet
    // resolver (proper death path), the launcher reboots it, and the SDK's
    // retry logic recovers.
    // Returns whether the reboot actually triggered (i.e. the scenario
    // produced at least rebootPoint counted events).
    bool RunScenarioOnceAtRebootPoint(const TScenario& scenario, ui64 rebootPoint) {
        TString testLabel = Sprintf("reboot_after_event_%lu", (unsigned long)rebootPoint);
        Cerr << "=== REBOOT_POINT=" << rebootPoint << Endl;

        // Reset counters for this run.
        Env.ResetCounters();
        Env.RebootPoint.store(rebootPoint);
        Env.RebootAfterEventCount.store(rebootPoint);

        // Reset per-run verification state: each reboot-point run is
        // independent. The read sessions start from RunStartOffset (set via
        // StartPartitionSessionResponse.read_offset) so they only read
        // messages written in THIS run, not old messages from previous runs.
        // TotalWrittenMessages keeps accumulating (so SeqNos stay unique
        // across runs), but VerifiedSeqNos and StepTimings are per-run.
        RunStartOffset = static_cast<i64>(TotalWrittenMessages.load());
        {
            std::lock_guard<std::mutex> lock(VerifiedSeqNosMutex);
            VerifiedSeqNos.clear();
        }
        StepTimings.clear();

        // Execute each step in the scenario.
        for (ui64 stepIdx = 0; stepIdx < scenario.size(); ++stepIdx) {
            TString stepLabel = testLabel + Sprintf(" step_%lu", (unsigned long)stepIdx);

            bool dataReceived = false;
            ExecuteScenarioStep(scenario[stepIdx], &dataReceived);

            AssertNoErrorClose(stepLabel);
        }

        // Verify coverage for THIS run (VerifiedSeqNos and StepTimings are
        // per-run, reset at the start of each reboot-point run). With
        // max_lag, this predicts which messages could have been skipped due
        // to balancer reboot delays and validates that only those are missing.
        if (ScenarioMaxLagSeconds > 0) {
            VerifyFullCoverage(Sprintf("reboot_after_event_%lu", (unsigned long)rebootPoint));
        }

        // Check if reboot was actually triggered.
        Cerr << "=== REBOOT_CHECK rebootPoint=" << rebootPoint
             << " triggered=" << Env.RebootTriggered.load()
             << " eventCount=" << Env.PqTabletEventCount.load()
             << " bootCount=" << Env.TabletBootCount.load() << Endl;

        // Dump per-event-type counters (sorted by count desc) so the event
        // distribution for this reboot point is visible even on timeout.
        DumpEventCounterSummary(rebootPoint);

        if (Env.RebootTriggered.load()) {
            // Sanity check: RebootTablet() must have killed the tablet and
            // the launcher must have rebooted it (a new EvBoot was observed).
            // The step could not have completed otherwise — the SDK recovery
            // requires the tablet to be back — but verify explicitly.
            UNIT_ASSERT_C(Env.TabletBootCount.load() > 0,
                "rebootPoint=" << rebootPoint << ": RebootTablet was called "
                << "but no tablet boot was observed");
        } else {
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
        // Auto-detect max_lag from the scenario steps: if any step uses
        // MaxLagSeconds > 0, enable max_lag-aware coverage verification so
        // VerifyFullCoverage predicts skippable messages instead of
        // asserting full coverage. Tests only set MaxLagSeconds in the
        // scenario step; the framework handles the rest.
        ScenarioMaxLagSeconds = DetectMaxLagSeconds(scenario);

        ui64 rebootPoint = initialRebootPoint;
        while (RunScenarioOnceAtRebootPoint(scenario, rebootPoint)) {
            rebootPoint += rebootPointStride;
        }

        // Clear the reboot target so nothing after the sweep (e.g. the final
        // verification) injects poison pills.
        Env.RebootAfterEventCount.store(0);
    }

    // Auto-detect max_lag from the scenario steps: if any step uses
    // MaxLagSeconds > 0, enable max_lag-aware coverage verification so
    // VerifyFullCoverage predicts skippable messages instead of
    // asserting full coverage. Tests only set MaxLagSeconds in the
    // scenario step; the framework handles the rest.
    ui64 DetectMaxLagSeconds(const TScenario& scenario) const {
        ui64 maxLag = 0;
        for (const auto& step : scenario) {
            maxLag = Max(maxLag, step.ReadSettings.MaxLagSeconds);
        }
        return maxLag;
    }

    // Build a sampled reboot-point list: dense coverage of the first
    // `denseCount` event boundaries (1, 2, ..., denseCount), then every
    // `stride`-th boundary after that (denseCount+stride, denseCount+2*stride,
    // ...). This keeps the early lifecycle stages — where the most interesting
    // recovery logic happens — fully covered while bounding total runtime
    // for scenarios that produce many events. The sweep terminates naturally
    // when RunScenarioOnceAtRebootPoint encounters a reboot point that does
    // not trigger (the scenario produced fewer events than the point), so
    // the upper bound only needs to be large enough to reach the last event.
    TVector<ui64> MakeSampledRebootPoints(ui64 denseCount, ui64 stride, ui64 maxPoint = 10000) const {
        TVector<ui64> points;
        for (ui64 p = 1; p <= denseCount && p <= maxPoint; ++p) {
            points.push_back(p);
        }
        for (ui64 p = denseCount + stride; p <= maxPoint; p += stride) {
            points.push_back(p);
        }
        return points;
    }

    // Run the scenario with an event-boundary reboot at each of the given
    // reboot points (expected in ascending order). Stops at the first point
    // that does not trigger — the same exhaustion rule as
    // RunScenarioWithAllReboots. This lets a test cover the important
    // lifecycle stages densely (e.g. the write session init and tail) while
    // only sampling the repetitive middle of a long scenario — much cheaper
    // than a uniform sweep when the scenario produces many events.
    void RunScenarioAtRebootPoints(const TScenario& scenario, const TVector<ui64>& rebootPoints) {
        ScenarioMaxLagSeconds = DetectMaxLagSeconds(scenario);
        for (ui64 rebootPoint : rebootPoints) {
            if (!RunScenarioOnceAtRebootPoint(scenario, rebootPoint)) {
                break;
            }
        }

        // Clear the reboot target (see RunScenarioWithAllReboots).
        Env.RebootAfterEventCount.store(0);
    }

private:
    void DumpEventCounterSummary(ui64 rebootPoint) const {
        Cerr << "=== EVENT_COUNTER_SUMMARY rebootPoint=" << rebootPoint << Endl;
        TVector<std::pair<TString, ui64>> sorted;
        sorted.reserve(Env.PqEventTypeCount.size());
        for (const auto& [n, c] : Env.PqEventTypeCount) {
            sorted.emplace_back(n, c);
        }
        std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) { return a.second > b.second; });
        for (const auto& [n, c] : sorted) {
            Cerr << "    " << n << " x" << c << Endl;
        }
    }

};

Y_UNIT_TEST_SUITE_F(TTabletRestartReadSessionTest, TTabletRestartReadSessionFixture) {

// Combined stress test: reboot the partition tablet only at event boundaries.
// The scenario combines following stress factors:
// - Concurrent write+read: 8 messages written while 5 SDK read sessions
//   (each with its own consumer) are open and reading
// - Large chunks (1MB messages spanning multiple blob parts)
// - Small lag (MaxLagSeconds=1 to trigger WaitForData timeout path)
// At each event boundary the boundary event is dropped and the main loop
// calls RebootTablet() for the partition tablet: the tablet dies via the
// tablet resolver (as a production tablet death with the event in flight),
// the launcher reboots it, and the SDK's retry logic must recover.
Y_UNIT_TEST(RebootTabletOnlyCombinedStress) {
    // Configure: reboot only the PQ tablet, not the balancer.
    Env.DoRebootPqTablet = true;
    Env.DoRebootPqrbTablet = false;

    // Single concurrent step: write 5 messages while 5 sessions read.
    TScenario scenario;
    TScenarioStep step;
    step.Type = TScenarioStep::EType::Concurrent;
    step.Count = 5;
    step.MessageSize = 512_KB;
    step.ReadSessionCount = 5;
    step.ReadSettings.MaxLagSeconds = 1; // small lag → WaitForData timeout path
    scenario.push_back(step);

    // Run the scenario with reboots at sampled event boundaries: dense
    // coverage of the first 30 events, then every 3rd event after that.
    // This bounds runtime while still sweeping the full event range.
    RunScenarioAtRebootPoints(scenario, MakeSampledRebootPoints(30, 3));

    // Final correctness verification.
    VerifyFullCoverage("RebootTabletOnlyCombinedStress");
}

// Combined stress test: reboot the balancer tablet only at event boundaries.
// The scenario combines following stress factors:
// - Concurrent write+read: 8 messages written while 5 SDK read sessions
//   (each with its own consumer) are open and reading
// - Large chunks (1MB messages spanning multiple blob parts)
// - Small lag (MaxLagSeconds=1 to trigger WaitForData timeout path)
// At each event boundary the boundary event is dropped and the main loop
// calls RebootTablet() for the balancer: the balancer dies via the tablet
// resolver, the launcher reboots it, and the read session actor's
// ProcessBalancerDead handler must recover by re-registering the session
// with the new balancer.
Y_UNIT_TEST(RebootBalancerOnlyCombinedStress) {
    // Configure: reboot only the balancer, not the PQ tablet.
    Env.DoRebootPqTablet = false;
    Env.DoRebootPqrbTablet = true;

    // Single concurrent step: write 5 messages while 5 sessions read.
    TScenario scenario;
    TScenarioStep step;
    step.Type = TScenarioStep::EType::Concurrent;
    step.Count = 5;
    step.MessageSize = 512_KB;
    step.ReadSessionCount = 5;
    step.ReadSettings.MaxLagSeconds = 1; // small lag → WaitForData timeout path
    scenario.push_back(step);

    // Run the scenario with reboots after each event boundary.
    // RunScenarioWithAllReboots auto-detects MaxLagSeconds from the scenario
    // and enables max_lag-aware coverage verification accordingly.
    RunScenarioWithAllReboots(scenario);

    // Final correctness verification (max_lag-aware).
    VerifyFullCoverage("RebootBalancerOnlyCombinedStress");
}

// Combined stress test: reboot both the partition tablet and the balancer
// tablet at event boundaries. The effective count is the sum of both tablet
// and balancer event counters. The reboot targets exactly the party whose
// event is at the boundary.
// The scenario combines following stress factors:
// - Concurrent write+read: 8 messages written while 5 SDK read sessions
//   (each with its own consumer) are open and reading
// - Large chunks (1MB messages spanning multiple blob parts)
// - Small lag (MaxLagSeconds=1 to trigger WaitForData timeout path)
// At each event boundary the boundary event is dropped and the main loop
// calls RebootTablet() for whichever party's event is at the boundary
// (the partition tablet or the balancer, based on which pipe the event
// flows through).
Y_UNIT_TEST(RebootBothTabletsCombinedStress) {
    // Configure: reboot both the PQ tablet and the balancer.
    Env.DoRebootPqTablet = true;
    Env.DoRebootPqrbTablet = true;

    // Single concurrent step: write 5 messages while 5 sessions read.
    TScenario scenario;
    TScenarioStep step;
    step.Type = TScenarioStep::EType::Concurrent;
    step.Count = 5;
    step.MessageSize = 512_KB;
    step.ReadSessionCount = 5;
    step.ReadSettings.MaxLagSeconds = 1; // small lag → WaitForData timeout path
    scenario.push_back(step);

    // Run the scenario with reboots at sampled event boundaries: dense
    // coverage of the first 30 events, then every 3rd event after that.
    // This bounds runtime while still sweeping the full event range.
    RunScenarioAtRebootPoints(scenario, MakeSampledRebootPoints(30, 3));

    // Final correctness verification.
    VerifyFullCoverage("RebootBothTabletsCombinedStress");
}

} // Y_UNIT_TEST_SUITE_F(TTabletRestartReadSessionTest)

} // anonymous namespace
} // namespace NKikimr::NPersQueueTests
