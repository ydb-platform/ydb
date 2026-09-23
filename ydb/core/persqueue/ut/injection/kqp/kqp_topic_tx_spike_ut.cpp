#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/vector.h>
#include <util/system/env.h>
#include <util/thread/pool.h>

#include <optional>

using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NYdb::NQuery;

namespace NKikimr::NPQ {
namespace {

constexpr const char* kTopic = "spike-topic";
constexpr const char* kConsumer = "spike-consumer";
constexpr const char* kSourceId = "spike-src";

// Same idea as MLP RunWithDispatch: SDK/gRPC blocks on a worker thread while the
// test thread pumps simulated-time DispatchEvents until the future completes.
IThreadPool& SpikeDispatchPool() {
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
    auto future = NThreading::Async(std::forward<TFunc>(func), SpikeDispatchPool());
    return static_cast<NKikimr::TTestActorRuntime&>(runtime).WaitFuture(std::move(future));
}

struct TSpikeServer {
    std::unique_ptr<::NPersQueue::TTestServer> Server;

    NActors::TTestActorRuntime& GetRuntime() {
        return *Server->CleverServer->GetRuntime();
    }

    TDriverConfig MakeDriverConfig() const {
        TDriverConfig config;
        config.SetEndpoint(Server->Endpoint);
        config.SetDatabase("/Root");
        config.SetAuthToken("root@builtin");
        config.SetLog(std::make_unique<TStreamLogBackend>(&Cerr));
        // Required for SDK under UseRealThreads=false (see MLP CreatePipeTopic).
        config.SetDiscoveryMode(EDiscoveryMode::Async);
        return config;
    }
};

// Begin + 2 writes + WRITTEN_IN_TX acks. Built before reboot observers are installed.
struct TPreparedTx {
    TDriver Driver;
    std::shared_ptr<IWriteSession> WriteSession;
    std::optional<TSession> QuerySession;
    std::optional<TTransaction> Tx;

    explicit TPreparedTx(TDriverConfig config)
        : Driver(std::move(config))
    {
    }
};

struct TSpikeEnv;
std::unique_ptr<TPreparedTx> PrepareBeginAndWrites(TSpikeEnv& env);
ui64 ResolvePqTabletId(TSpikeEnv& env, ui32 partition = 0);

struct TSpikeEnv {
    std::unique_ptr<TSpikeServer> Setup;
    std::unique_ptr<TPreparedTx> Prepared;
    ui64 PqTabletId = 0;

    void Prepare(
        const TString& dispatchName,
        std::function<void(TTestActorRuntime&)> setup,
        bool& activeZone)
    {
        activeZone = false;
        Cerr << Endl
             << "====== KQP_SPIKE BEGIN dispatch=" << dispatchName
             << " ======" << Endl;

        auto settings = TTopicSdkTestSetup::MakeServerSettings();
        settings.SetUseRealThreads(false);
        settings.SetNodeCount(1);

        Setup = std::make_unique<TSpikeServer>();
        Setup->Server = std::make_unique<::NPersQueue::TTestServer>(settings, /*start=*/false);
        Setup->Server->StartServer(/*doClientInit=*/false, TString("/Root"));

        auto& runtime = Setup->GetRuntime();
        runtime.SetScheduledLimit(100'000);

        // Simulated time starts at 0; sync so timeouts/retention behave.
        runtime.UpdateCurrentTime(TInstant::Now());
        Setup->Server->AnnoyingClient->SetNoConfigMode();

        // Supportive SDK work MUST run before RunTestWithReboots observers /
        // CollapsedTimeScheduledEventsSelector: under that selector WaitFuture for
        // SDK calls livelocks (seen as 900s timeout mid-CreateTopic).
        RunWithDispatch(runtime, [&] {
            Setup->Server->AnnoyingClient->FullInit();
            return true;
        });

        RunWithDispatch(runtime, [&] {
            TDriver driver(Setup->MakeDriverConfig());
            TTopicClient topicClient(driver);
            auto create = topicClient.CreateTopic(
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
            UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
            driver.Stop(true);
            return true;
        });

        PqTabletId = ResolvePqTabletId(*this);
        Cerr << "====== KQP_SPIKE prep Begin/Write (before observers) ======" << Endl;
        Prepared = PrepareBeginAndWrites(*this);

        // Install reboot/pipe observers after supportive setup; before Commit zone.
        setup(runtime);
    }
};

// Hive assigns the first PQ tablet at FakeHiveTablets under FakeHive (prod TTestEnv).
// TTestServer uses real Hive — tablet ids are stable per fresh server but not FakeHiveTablets.
// Resolve via scheme cache after CreateTopic; warm up once for RunTestWithReboots vectors.
ui64 ResolvePqTabletId(TSpikeEnv& env, ui32 partition) {
    auto& runtime = env.Setup->GetRuntime();
    const auto edge = runtime.AllocateEdgeActor();

    auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
    navigate->DatabaseName = "/Root";
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath("/Root/" + TString(kTopic));
    entry.SyncVersion = true;
    entry.ShowPrivatePath = true;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
    navigate->ResultSet.push_back(std::move(entry));
    navigate->Cookie = 12345;

    runtime.Send(MakeSchemeCacheID(), edge,
        new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()),
        0, true);
    auto response = runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySetResult>();
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Request->Cookie, 12345u);
    UNIT_ASSERT_VALUES_EQUAL(response->Request->ErrorCount, 0u);

    auto& front = response->Request->ResultSet.front();
    UNIT_ASSERT(front.PQGroupInfo);
    for (const auto& p : front.PQGroupInfo->Description.GetPartitions()) {
        if (p.GetPartitionId() == partition) {
            Cerr << "====== KQP_SPIKE resolved PQ tablet=" << p.GetTabletId()
                 << " partition=" << partition << " ======" << Endl;
            return p.GetTabletId();
        }
    }
    UNIT_FAIL("unknown partition");
    return 0;
}

ui64 WarmupResolvePqTabletId() {
    // Lightweight: no Begin/Write — open write sessions crash on TTestServer teardown.
    TSpikeEnv env;

    Cerr << Endl << "====== KQP_SPIKE BEGIN dispatch=warmup-resolve ======" << Endl;

    auto settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetUseRealThreads(false);
    settings.SetNodeCount(1);

    env.Setup = std::make_unique<TSpikeServer>();
    env.Setup->Server = std::make_unique<::NPersQueue::TTestServer>(settings, /*start=*/false);
    env.Setup->Server->StartServer(/*doClientInit=*/false, TString("/Root"));

    auto& runtime = env.Setup->GetRuntime();
    runtime.SetScheduledLimit(100'000);
    runtime.UpdateCurrentTime(TInstant::Now());
    env.Setup->Server->AnnoyingClient->SetNoConfigMode();

    RunWithDispatch(runtime, [&] {
        env.Setup->Server->AnnoyingClient->FullInit();
        return true;
    });

    RunWithDispatch(runtime, [&] {
        TDriver driver(env.Setup->MakeDriverConfig());
        TTopicClient topicClient(driver);
        auto create = topicClient.CreateTopic(
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
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
        driver.Stop(true);
        return true;
    });

    return ResolvePqTabletId(env);
}

// Prefer resolved PQ tablet; Coordinator is the real plan tablet under TTestServer.
TVector<ui64> SpikeRebootTablets(ui64 pqTabletId) {
    return {
        pqTabletId,
        TTestTxConfig::Coordinator,
    };
}

std::unique_ptr<TPreparedTx> PrepareBeginAndWrites(TSpikeEnv& env) {
    auto prepared = std::make_unique<TPreparedTx>(env.Setup->MakeDriverConfig());
    RunWithDispatch(env.Setup->GetRuntime(), [&] {
        TTopicClient topicClient(prepared->Driver);

        TQueryClient queryClient(prepared->Driver);
        auto sessionResult = queryClient.GetSession().ExtractValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        prepared->QuerySession.emplace(sessionResult.GetSession());

        auto begin = prepared->QuerySession->BeginTransaction(TTxSettings()).ExtractValueSync();
        UNIT_ASSERT_C(begin.IsSuccess(), begin.GetIssues().ToString());
        prepared->Tx.emplace(begin.GetTransaction());

        prepared->WriteSession = topicClient.CreateWriteSession(
            TWriteSessionSettings()
                .Path(kTopic)
                .ProducerId(kSourceId)
                .MessageGroupId(kSourceId));

        auto waitToken = [&]() -> TContinuationToken {
            for (;;) {
                auto ev = prepared->WriteSession->GetEvent(/*block=*/true);
                UNIT_ASSERT(ev);
                if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*ev)) {
                    return std::move(ready->ContinuationToken);
                }
                if (auto* closed = std::get_if<TSessionClosedEvent>(&*ev)) {
                    UNIT_FAIL("write session closed: " << closed->DebugString());
                }
            }
        };

        auto writeOne = [&](const std::string& payload) {
            auto token = waitToken();
            TWriteMessage msg{payload};
            msg.Tx(*prepared->Tx);
            prepared->WriteSession->Write(std::move(token), std::move(msg));
        };

        writeOne("m1");
        writeOne("m2");

        ui32 inTxAcks = 0;
        while (inTxAcks < 2) {
            auto ev = prepared->WriteSession->GetEvent(/*block=*/true);
            UNIT_ASSERT(ev);
            if (auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&*ev)) {
                for (const auto& ack : acks->Acks) {
                    if (ack.State == TWriteSessionEvent::TWriteAck::EES_WRITTEN_IN_TX) {
                        ++inTxAcks;
                    }
                }
            } else if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*ev)) {
                Y_UNUSED(ready);
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*ev)) {
                UNIT_FAIL("write session closed while waiting acks: " << closed->DebugString());
            }
        }
        return true;
    });
    return prepared;
}

bool CommitPreparedTx(TSpikeEnv& env, TPreparedTx& prepared) {
    return RunWithDispatch(env.Setup->GetRuntime(), [&] {
        auto commit = prepared.Tx->Commit().ExtractValueSync();
        if (!commit.IsSuccess()) {
            Cerr << "====== KQP_SPIKE Commit failed: "
                 << commit.GetIssues().ToString() << " ======" << Endl;
            return false;
        }
        prepared.WriteSession->Close(TDuration::Seconds(5));
        prepared.Driver.Stop(true);
        return true;
    });
}

void CleanupPreparedTx(TSpikeEnv& env, TPreparedTx& prepared) {
    try {
        RunWithDispatch(env.Setup->GetRuntime(), [&] {
            if (prepared.WriteSession) {
                prepared.WriteSession->Close(TDuration::MilliSeconds(100));
            }
            prepared.Driver.Stop(true);
            return true;
        });
    } catch (...) {
    }
}

// Active zone = CommitTx only. Begin/Write are prepared before observers.
// Note: retries that need a fresh Begin/Write after observers are installed may
// livelock under CollapsedTime; Trace should succeed on the first Commit.
void CommitOnlyScenario(TSpikeEnv& env, bool& activeZone) {
    UNIT_ASSERT(env.Prepared);

    for (ui32 attempt = 0; attempt < 15; ++attempt) {
        try {
            env.Setup->GetRuntime().ResetScheduledCount();
            activeZone = false;

            if (!env.Prepared) {
                Cerr << "====== KQP_SPIKE attempt=" << attempt
                     << " re-prep Begin/Write (after observers; may livelock) ======" << Endl;
                env.Prepared = PrepareBeginAndWrites(env);
            }

            Cerr << "====== KQP_SPIKE attempt=" << attempt
                 << " enter activeZone (CommitTx) ======" << Endl;
            activeZone = true;
            const bool ok = CommitPreparedTx(env, *env.Prepared);
            activeZone = false;

            if (ok) {
                Cerr << "====== KQP_SPIKE attempt=" << attempt
                     << " Commit ok ======" << Endl;
                env.Prepared.reset();
                return;
            }

            Cerr << "====== KQP_SPIKE attempt=" << attempt
                 << " Commit not success, retry ======" << Endl;
            CleanupPreparedTx(env, *env.Prepared);
            env.Prepared.reset();
        } catch (const NActors::TSchedulingLimitReachedException&) {
            activeZone = false;
            Cerr << "====== KQP_SPIKE attempt=" << attempt
                 << " scheduling limit ======" << Endl;
            env.Prepared.reset();
        } catch (const NActors::TEmptyEventQueueException&) {
            activeZone = false;
            Cerr << "====== KQP_SPIKE attempt=" << attempt
                 << " empty event queue ======" << Endl;
            env.Prepared.reset();
        } catch (const yexception& ex) {
            activeZone = false;
            Cerr << "====== KQP_SPIKE attempt=" << attempt
                 << " retry: " << ex.what() << " ======" << Endl;
            env.Prepared.reset();
        }
    }
    UNIT_FAIL("CommitOnlyScenario: retries exhausted");
}

void RunSpikeInjection(
    std::function<void(
        const TVector<ui64>&,
        std::function<TTestActorRuntime::TEventFilter()>,
        std::function<void(const TString&, std::function<void(TTestActorRuntime&)>, bool&)>)> runner,
    ui64 pqTabletId)
{
    TInitialEventsFilter filter;
    runner(
        SpikeRebootTablets(pqTabletId),
        [&]() {
            return filter.Prepare({TabletPipe, NPDisk, KeyValue, PQ});
        },
        [&](const TString& dispatchName,
            std::function<void(TTestActorRuntime&)> setup,
            bool& activeZone) {
            TSpikeEnv env;
            activeZone = false;
            env.Prepare(dispatchName, setup, activeZone);
            CommitOnlyScenario(env, activeZone);
            Cerr << "====== KQP_SPIKE END dispatch=" << dispatchName
                 << " ======" << Endl;
        });
}

Y_UNIT_TEST_SUITE(TKqpTopicTxSpikeTests) {

// Baseline: KQP topic write-tx under UseRealThreads=false + RunWithDispatch.
Y_UNIT_TEST(SmokeBeginWriteCommitWithoutRealThreads) {
    TSpikeEnv env;
    bool activeZone = false;
    env.Prepare(INITIAL_TEST_DISPATCH_NAME, [](TTestActorRuntime&) {}, activeZone);
    CommitOnlyScenario(env, activeZone);
}

// Trace pass only (FAST_UT): validates activeZone plumbing around CommitTx.
Y_UNIT_TEST(CommitWithTabletRebootsTrace) {
    SetEnv("FAST_UT", "1");
    const ui64 pqTabletId = WarmupResolvePqTabletId();
    RunSpikeInjection([](const auto& tabletIds, auto filterFactory, auto testFunc) {
        RunTestWithReboots(tabletIds, filterFactory, testFunc);
    }, pqTabletId);
}

// One early reboot on the real PQ tablet (skips Trace; selectedReboot=0).
// Warmup discovers Hive-assigned tablet id (stable across fresh TTestServer boots).
Y_UNIT_TEST(CommitWithOnePqTabletReboot) {
    const ui64 pqTabletId = WarmupResolvePqTabletId();
    TInitialEventsFilter filter;
    const TVector<ui64> tablets{pqTabletId};
    RunTestWithReboots(
        tablets,
        [&]() {
            return filter.Prepare({TabletPipe, NPDisk, KeyValue, PQ});
        },
        [&](const TString& dispatchName,
            std::function<void(TTestActorRuntime&)> setup,
            bool& activeZone) {
            TSpikeEnv env;
            activeZone = false;
            env.Prepare(dispatchName, setup, activeZone);
            UNIT_ASSERT_VALUES_EQUAL(env.PqTabletId, pqTabletId);
            CommitOnlyScenario(env, activeZone);
            Cerr << "====== KQP_SPIKE END dispatch=" << dispatchName
                 << " ======" << Endl;
        },
        /*selectedReboot=*/0,
        /*selectedTablet=*/pqTabletId);
}

// Trace pass only for pipe resets.
// Disabled for now: Commit succeeds, but TTestServer teardown under pipe-reset
// observers SIGSEGVs (same class of gRPC/infly shutdown issues as open write sessions).
// Y_UNIT_TEST(CommitWithPipeResetsTrace) {
//     SetEnv("FAST_UT", "1");
//     const ui64 pqTabletId = WarmupResolvePqTabletId();
//     RunSpikeInjection([](const auto& tabletIds, auto filterFactory, auto testFunc) {
//         RunTestWithPipeResets(tabletIds, filterFactory, testFunc);
//     }, pqTabletId);
// }

} // Y_UNIT_TEST_SUITE(TKqpTopicTxSpikeTests)

} // namespace
} // namespace NKikimr::NPQ
