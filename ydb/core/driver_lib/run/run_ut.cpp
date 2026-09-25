#include "run.h"
#include "config_helpers.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/util/affinity.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/system/event.h>

#include <array>
#include <atomic>
#include <functional>

Y_UNIT_TEST_SUITE(XdsBootstrapConfigInitializer) {

using namespace NKikimr;

class TTestKikimrRunner : public TKikimrRunner {
    TTestKikimrRunner() = default;

    void InitializeXdsBootstrapConfig(NKikimrConfig::TAppConfig& appConfig) {
        TKikimrRunner::InitializeXdsBootstrapConfig(TKikimrRunConfig(appConfig));
    }

public:
    static void InitXdsBootstrapConfig(NKikimrConfig::TAppConfig& appConfig) {
        TTestKikimrRunner runner;
        runner.InitializeXdsBootstrapConfig(appConfig);
    }
};

const TString XDS_BOOTSTRAP_ENV = "GRPC_XDS_BOOTSTRAP";
const TString XDS_BOOTSTRAP_CONFIG_ENV = "GRPC_XDS_BOOTSTRAP_CONFIG";

Y_UNIT_TEST(CanNotSetEnvIfXdsBootstrapConfigIsAbsent) {
    NKikimrConfig::TAppConfig appConfig;
    TTestKikimrRunner::InitXdsBootstrapConfig(appConfig);
    TString jsonXdsBootstrapConfig = GetEnv(XDS_BOOTSTRAP_CONFIG_ENV);
    UNIT_ASSERT_STRINGS_EQUAL_C(jsonXdsBootstrapConfig, "", "The checked value: " + jsonXdsBootstrapConfig);
}

Y_UNIT_TEST(CanSetGrpcXdsBootstrapConfigEnv) {
    NKikimrConfig::TAppConfig appConfig;
    auto* xdsBootstrapConfig = appConfig.MutableGRpcConfig()->MutableXdsBootstrap();
    auto* xdsServers = xdsBootstrapConfig->AddXdsServers();
    xdsServers->SetServerUri("xds-provider.bootstrap.my-company.net:18000");
    *xdsServers->AddServerFeatures() = "xds_v3";
    auto* channelCreds = xdsServers->AddChannelCreds();
    channelCreds->SetType("insecure");
    channelCreds->SetConfig("{\"k1\": \"v1\", \"k2\": \"v2\"}");
    auto* node = xdsBootstrapConfig->MutableNode();
    node->SetId("dc-000-host");
    node->SetCluster("testing");
    node->SetMeta("{\"service\": \"ydb\"}");
    node->MutableLocality()->SetZone("test-zone");

    TTestKikimrRunner::InitXdsBootstrapConfig(appConfig);
    const TString expectedJson = R"({"node":{"cluster":"testing","locality":{"zone":"test-zone"},"metadata":{"service":"ydb"},"id":"dc-000-host"},"xds_servers":[{"channel_creds":[{"config":{"k2":"v2","k1":"v1"},"type":"insecure"}],"server_uri":"xds-provider.bootstrap.my-company.net:18000","server_features":["xds_v3"]}]})";
    TString jsonXdsBootstrapConfig = GetEnv(XDS_BOOTSTRAP_CONFIG_ENV);
    UNIT_ASSERT_STRINGS_EQUAL_C(jsonXdsBootstrapConfig, expectedJson, "The checked value: " + jsonXdsBootstrapConfig);
}

Y_UNIT_TEST(CanSetGrpcXdsBootstrapConfigEnvWithSomeNumberOfXdsServers) {
    NKikimrConfig::TAppConfig appConfig;
    auto* xdsBootstrapConfig = appConfig.MutableGRpcConfig()->MutableXdsBootstrap();
    {
        auto* xdsServers = xdsBootstrapConfig->AddXdsServers();
        xdsServers->SetServerUri("xds-provider-000.bootstrap.my-company.net:18000");
        *xdsServers->AddServerFeatures() = "xds_v3";
        auto* channelCreds = xdsServers->AddChannelCreds();
        channelCreds->SetType("insecure");
        channelCreds->SetConfig("{\"k1\": \"v1\", \"k2\": \"v2\"}");
    }
    {
        auto* xdsServers = xdsBootstrapConfig->AddXdsServers();
        xdsServers->SetServerUri("xds-provider-001.bootstrap.my-company.net:18000");
        *xdsServers->AddServerFeatures() = "xds_v3";
        auto* channelCreds = xdsServers->AddChannelCreds();
        channelCreds->SetType("secure");
        channelCreds->SetConfig("{\"k1\": \"v11\", \"k2\": \"v21\"}");
    }
    auto* node = xdsBootstrapConfig->MutableNode();
    node->SetId("dc-000-host");
    node->SetCluster("testing");
    node->SetMeta("{\"service\": \"ydb\"}");
    node->MutableLocality()->SetZone("test-zone");

    TTestKikimrRunner::InitXdsBootstrapConfig(appConfig);
    const TString expectedJson = R"({"node":{"cluster":"testing","locality":{"zone":"test-zone"},"metadata":{"service":"ydb"},"id":"dc-000-host"},"xds_servers":[{"channel_creds":[{"config":{"k2":"v2","k1":"v1"},"type":"insecure"}],"server_uri":"xds-provider-000.bootstrap.my-company.net:18000","server_features":["xds_v3"]},{"channel_creds":[{"config":{"k2":"v21","k1":"v11"},"type":"secure"}],"server_uri":"xds-provider-001.bootstrap.my-company.net:18000","server_features":["xds_v3"]}]})";
    TString jsonXdsBootstrapConfig = GetEnv(XDS_BOOTSTRAP_CONFIG_ENV);
    UNIT_ASSERT_STRINGS_EQUAL_C(jsonXdsBootstrapConfig, expectedJson, "The checked value: " + jsonXdsBootstrapConfig);
}

Y_UNIT_TEST(CanNotSetGrpcXdsBootstrapConfigEnvIfVariableAlreadySet) {
    NKikimrConfig::TAppConfig appConfig;
    auto* xdsBootstrapConfig = appConfig.MutableGRpcConfig()->MutableXdsBootstrap();
    auto* xdsServers = xdsBootstrapConfig->AddXdsServers();
    xdsServers->SetServerUri("xds-provider.bootstrap.my-company.net:18000");
    *xdsServers->AddServerFeatures() = "xds_v3";
    auto* channelCreds = xdsServers->AddChannelCreds();
    channelCreds->SetType("insecure");
    channelCreds->SetConfig("{\"k1\": \"v1\", \"k2\": \"v2\"}");
    auto* node = xdsBootstrapConfig->MutableNode();
    node->SetId("dc-000-host");
    node->SetCluster("testing");
    node->SetMeta("{\"service\": \"ydb\"}");
    node->MutableLocality()->SetZone("test-zone");

    SetEnv(XDS_BOOTSTRAP_CONFIG_ENV, "{xds bootstrap config already set}");

    TTestKikimrRunner::InitXdsBootstrapConfig(appConfig);
    TString jsonXdsBootstrapConfig = GetEnv(XDS_BOOTSTRAP_CONFIG_ENV);
    UNIT_ASSERT_STRINGS_EQUAL_C(jsonXdsBootstrapConfig, "{xds bootstrap config already set}", "The checked value: " + jsonXdsBootstrapConfig);
}

} // XdsBootstrapConfigInitializer

Y_UNIT_TEST_SUITE(ActorSystemConfigHelpers) {

using namespace NKikimr;
using TExecutorConfig = NKikimrConfig::TActorSystemConfig::TExecutor;

TExecutorConfig* AddExecutor(NKikimrConfig::TActorSystemConfig& config, TExecutorConfig::EType type,
        const TString& name = {}) {
    auto* executor = config.AddExecutor();
    executor->SetType(type);
    if (!name.empty()) {
        executor->SetName(name);
    }
    return executor;
}

void AssertCpuMask(const TCpuMask& actual, const TString& expectedCpuList) {
    const TCpuMask expected(expectedCpuList);
    UNIT_ASSERT_C((actual - expected).IsEmpty(), "actual affinity contains unexpected CPUs");
    UNIT_ASSERT_C((expected - actual).IsEmpty(), "actual affinity is missing expected CPUs");
}

void AssertCpuMasksEqual(const TCpuMask& actual, const TCpuMask& expected) {
    UNIT_ASSERT_C((actual - expected).IsEmpty(), "actual affinity contains unexpected CPUs");
    UNIT_ASSERT_C((expected - actual).IsEmpty(), "actual affinity is missing expected CPUs");
}

const NActors::TBasicExecutorPoolConfig* FindBasicPool(
        const NActors::TCpuManagerConfig& config, ui32 poolId) {
    for (const auto& pool : config.Basic) {
        if (pool.PoolId == poolId) {
            return &pool;
        }
    }
    return nullptr;
}

Y_UNIT_TEST(ExecutorPriorityConfiguration) {
    class TCallbackActor : public NActors::TActorBootstrapped<TCallbackActor> {
    public:
        explicit TCallbackActor(std::function<void()> callback)
            : Callback(std::move(callback))
        {}

        void Bootstrap() {
            Callback();
            PassAway();
        }

    private:
        std::function<void()> Callback;
    };

    // Exercise omitted, false and true per-pool settings in the same system.
    // Priority selection must not depend on the Batch role or the pool name.
    for (const int batchSetting : {-1, 0, 1, 2}) {
        for (const bool waker : {false, true}) {
            NKikimrConfig::TActorSystemConfig config;
            if (batchSetting >= 0) {
                config.SetBatchExecutor(batchSetting);
            }
            for (ui32 poolId = 0; poolId < 3; ++poolId) {
                auto* pool = config.AddExecutor();
                pool->SetType(NKikimrConfig::TActorSystemConfig::TExecutor::BASIC);
                pool->SetName(poolId == 2 ? "Background" : "Batch");
                pool->SetThreads(1);
                pool->SetMinThreads(1);
                pool->SetMaxThreads(1);
                pool->SetSpinThreshold(0);
                pool->SetEnableWaker(waker);
                if (poolId != 0) {
                    pool->SetUsePriority(poolId == 2);
                }
            }

            struct TObservation {
                TManualEvent Started, Release;
                std::array<TManualEvent, 3> Done;
                std::array<ui32, 3> Order{Max<ui32>(), Max<ui32>(), Max<ui32>()};
                std::atomic<ui32> Next{0};
            };
            // Observations outlive shutdown, including assertion failures.
            std::array<TObservation, 3> observations;
            auto setup = MakeHolder<NActors::TActorSystemSetup>();
            setup->NodeId = 1;
            setup->CpuManager.Shared.United = config.GetUseUnitedPool();
            NActorSystemConfigHelpers::AddExecutorPools(setup->CpuManager, config, nullptr);
            setup->Scheduler.Reset(NActors::CreateSchedulerThread(
                NActorSystemConfigHelpers::CreateSchedulerConfig(config.GetScheduler())));
            NActors::TActorSystem actorSystem(setup);
            actorSystem.Start();
            Y_DEFER {
                for (auto& obs : observations) {
                    obs.Release.Signal();
                }
                actorSystem.Stop();
            };
            const auto pools = actorSystem.GetBasicExecutorPools();
            UNIT_ASSERT_VALUES_EQUAL(pools.size(), observations.size());
            for (auto* pool : pools) {
                const bool priority = pool->PoolId == 2;
                NActors::TExecutorPoolStats poolStats;
                TVector<NActors::TExecutorThreadStats> threadStats;
                pool->GetCurrentStats(poolStats, threadStats);
                UNIT_ASSERT_VALUES_EQUAL(poolStats.HasPriorityActivationQueues, priority);

                auto& [started, release, done, order, next] = observations[pool->PoolId];
                actorSystem.Register(new TCallbackActor([&started, &release] {
                    started.Signal();
                    release.Wait();
                }), NActors::TMailboxType::HTSwap, pool->PoolId);
                UNIT_ASSERT_C(started.WaitT(TDuration::Seconds(5)), "configured worker did not start");
                for (ui32 i = 0; i < 3; ++i) {
                    THolder<NActors::IActor> actor(new TCallbackActor([&next, &order, &done, i] {
                        order[i] = next.fetch_add(1);
                        done[i].Signal();
                    }));
                    if (i == 2) {
                        actor->SetMailboxPriority(NActors::EMailboxPriority::High);
                    }
                    actorSystem.Register(actor.Release(), NActors::TMailboxType::HTSwap, pool->PoolId);
                }
                release.Signal();
                for (auto& event : done) {
                    UNIT_ASSERT_C(event.WaitT(TDuration::Seconds(5)), "configured pool did not drain");
                }
                UNIT_ASSERT_VALUES_EQUAL(order[2], priority ? 0 : 2);
                UNIT_ASSERT_VALUES_EQUAL(order[0], priority ? 1 : 0);
                UNIT_ASSERT_VALUES_EQUAL(order[1], priority ? 2 : 1);
            }
        }
    }
}

Y_UNIT_TEST(HarmonizerNeedyCpuWindow) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* defaultExecutor = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    defaultExecutor->SetThreads(1);
    defaultExecutor->SetMaxThreads(2);

    auto* configuredExecutor = AddExecutor(systemConfig, TExecutorConfig::BASIC, "User");
    configuredExecutor->SetThreads(1);
    configuredExecutor->SetMaxThreads(2);
    configuredExecutor->SetHarmonizerNeedyCpuWindowSeconds(30);

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[0].HarmonizerNeedyCpuWindowSeconds, 1);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic[1].HarmonizerNeedyCpuWindowSeconds, 30);
}

Y_UNIT_TEST(BasicAndIoExecutorsWithoutPlacementUseCurrentAffinity) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* basic = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    basic->SetThreads(1);

    auto* io = AddExecutor(systemConfig, TExecutorConfig::IO, "IO");
    io->SetThreads(1);

    TAffinity currentAffinity;
    currentAffinity.Current();
    const TCpuMask expectedAffinity = currentAffinity;

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.Basic.size(), 1);
    AssertCpuMasksEqual(cpuManager.Basic.front().Affinity, expectedAffinity);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO.size(), 1);
    AssertCpuMasksEqual(cpuManager.IO.front().Affinity, expectedAffinity);
}

Y_UNIT_TEST(BlobStorageAndInterconnectSessionExecutorsReferencePoolLists) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    for (const TString& name : {"System", "BS0", "BS1", "ICSession"}) {
        auto* executor = AddExecutor(systemConfig, TExecutorConfig::BASIC, name);
        executor->SetThreads(1);
    }

    systemConfig.AddBlobStorageExecutor(1);
    systemConfig.AddBlobStorageExecutor(2);
    systemConfig.AddInterconnectSessionExecutor(3);
    systemConfig.AddInterconnectSessionExecutor(2);

    UNIT_ASSERT_VALUES_EQUAL(
        NActorSystemConfigHelpers::GetBlobStorageExecutorPoolIds(systemConfig),
        (TVector<ui32>{1, 2}));
    UNIT_ASSERT_VALUES_EQUAL(
        NActorSystemConfigHelpers::GetInterconnectSessionExecutorPoolIds(systemConfig),
        (TVector<ui32>{3, 2}));
}

Y_UNIT_TEST(ExecutorPoolListsAreEmptyByDefault) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* system = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    system->SetThreads(1);

    UNIT_ASSERT(NActorSystemConfigHelpers::GetBlobStorageExecutorPoolIds(systemConfig).empty());
    UNIT_ASSERT(NActorSystemConfigHelpers::GetInterconnectSessionExecutorPoolIds(systemConfig).empty());
}

Y_UNIT_TEST(BasicExecutorsUsePlacementAffinityWithoutAffectingOtherPools) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* system = AddExecutor(systemConfig, TExecutorConfig::BASIC, "System");
    system->SetThreads(2);

    auto* firstBlobStorage = AddExecutor(systemConfig, TExecutorConfig::BASIC, "BS0");
    firstBlobStorage->SetThreads(3);
    firstBlobStorage->SetPlacement(0);

    auto* secondBlobStorage = AddExecutor(systemConfig, TExecutorConfig::BASIC, "BS1");
    secondBlobStorage->SetThreads(3);
    secondBlobStorage->SetPlacement(1);

    auto* io = AddExecutor(systemConfig, TExecutorConfig::IO, "IO");
    io->SetThreads(1);

    systemConfig.AddBlobStorageExecutor(1);
    systemConfig.AddBlobStorageExecutor(2);

    TCpuTopology cpuTopology;
    cpuTopology.AllCpus = TCpuMask(TString("0-7"));
    cpuTopology.PlacementGroups = {
        {.Id = 10, .Cpus = TCpuMask(TString("0-1"))},
        {.Id = 20, .Cpus = TCpuMask(TString("2-3"))},
    };

    TAffinity currentAffinity;
    currentAffinity.Current();
    const TCpuMask expectedRegularPoolAffinity = currentAffinity;

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr, cpuTopology);

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.GetExecutorsCount(), 4);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.PingInfoByPool.size(), 4);

    const auto* systemPool = FindBasicPool(cpuManager, 0);
    UNIT_ASSERT(systemPool);
    AssertCpuMasksEqual(systemPool->Affinity, expectedRegularPoolAffinity);

    const auto* firstBlobStoragePool = FindBasicPool(cpuManager, 1);
    UNIT_ASSERT(firstBlobStoragePool);
    UNIT_ASSERT_VALUES_EQUAL(firstBlobStoragePool->PoolName, "BS0");
    UNIT_ASSERT_VALUES_EQUAL(firstBlobStoragePool->Threads, 3);
    AssertCpuMask(firstBlobStoragePool->Affinity, "0-1");

    const auto* secondBlobStoragePool = FindBasicPool(cpuManager, 2);
    UNIT_ASSERT(secondBlobStoragePool);
    UNIT_ASSERT_VALUES_EQUAL(secondBlobStoragePool->PoolName, "BS1");
    UNIT_ASSERT_VALUES_EQUAL(secondBlobStoragePool->Threads, 3);
    AssertCpuMask(secondBlobStoragePool->Affinity, "2-3");

    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(cpuManager.IO[0].PoolId, 3);
    AssertCpuMasksEqual(cpuManager.IO[0].Affinity, expectedRegularPoolAffinity);
}

Y_UNIT_TEST(MultipleBasicExecutorsCanSharePlacement) {
    NKikimrConfig::TActorSystemConfig systemConfig;

    auto* blobStorage = AddExecutor(systemConfig, TExecutorConfig::BASIC, "BS");
    blobStorage->SetThreads(1);
    blobStorage->SetPlacement(1);

    auto* interconnectSession = AddExecutor(systemConfig, TExecutorConfig::BASIC, "ICSession");
    interconnectSession->SetThreads(1);
    interconnectSession->SetPlacement(1);

    TCpuTopology cpuTopology;
    cpuTopology.AllCpus = TCpuMask(TString("0-3"));
    cpuTopology.PlacementGroups = {
        {.Id = 0, .Cpus = TCpuMask(TString("0-1"))},
        {.Id = 1, .Cpus = TCpuMask(TString("2-3"))},
    };

    NActors::TCpuManagerConfig cpuManager;
    NActorSystemConfigHelpers::AddExecutorPools(cpuManager, systemConfig, nullptr, cpuTopology);

    const auto* blobStoragePool = FindBasicPool(cpuManager, 0);
    const auto* interconnectSessionPool = FindBasicPool(cpuManager, 1);
    UNIT_ASSERT(blobStoragePool);
    UNIT_ASSERT(interconnectSessionPool);
    AssertCpuMask(blobStoragePool->Affinity, "2-3");
    AssertCpuMask(interconnectSessionPool->Affinity, "2-3");
}

} // ActorSystemConfigHelpers

Y_UNIT_TEST_SUITE(GrpcConfigurationInitializer) {
    class TTestKikimrRunner : public NKikimr::TKikimrRunner {
    public:
        using TKikimrRunner::InitializeGRpc;

        bool IsGrpcEnabled() const {
            return EnabledGrpcService;
        }
    };

    Y_UNIT_TEST(EnabledBeforeFactoryRuns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableGRpcConfig()->SetStartGRpcProxy(true);
        TTestKikimrRunner runner;
        runner.InitializeGRpc(NKikimr::TKikimrRunConfig(appConfig));
        UNIT_ASSERT(runner.IsGrpcEnabled());
    }

    Y_UNIT_TEST(DisabledWithoutGrpcConfig) {
        NKikimrConfig::TAppConfig appConfig;
        TTestKikimrRunner runner;
        runner.InitializeGRpc(NKikimr::TKikimrRunConfig(appConfig));
        UNIT_ASSERT(!runner.IsGrpcEnabled());
    }

    Y_UNIT_TEST(DisabledByGrpcConfig) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableGRpcConfig()->SetStartGRpcProxy(false);
        TTestKikimrRunner runner;
        runner.InitializeGRpc(NKikimr::TKikimrRunConfig(appConfig));
        UNIT_ASSERT(!runner.IsGrpcEnabled());
    }
}
