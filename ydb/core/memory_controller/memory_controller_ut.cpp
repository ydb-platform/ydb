#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <memory_controller.h>
#include <memory_controller_config.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tablet_flat/shared_cache_counters.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/general_cache/service/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <util/generic/scope.h>

#include <limits>

#ifdef _linux_
#include <sys/resource.h>
#endif

namespace NKikimr::NMemory {

using namespace Tests;
using namespace NSharedCache;

namespace {

void UpsertRows(TServer::TPtr server, TActorId sender, ui32 keyFrom = 0, ui32 keyTo = 2000) {
    TString query = "UPSERT INTO `/Root/table-1` (key, value) VALUES ";
    for (auto key : xrange(keyFrom, keyTo)) {
        if (key != keyFrom)
            query += ", ";
        query += "(" + ToString(key) + ", " + ToString(key) + ") ";
    }
    ExecSQL(server, sender, query);
}

// Bare-runtime tests need a provider they can drive without a whole server
struct TFixedProcessMemoryInfoProvider : public IProcessMemoryInfoProvider {
    TProcessMemoryInfo Get() const override {
        return ProcessMemoryInfo;
    }

    TProcessMemoryInfo ProcessMemoryInfo{0_MB, 0_MB, {}, {}, {}, {}};
};

NKikimrConfig::TMemoryControllerConfig MakeBudgetConfig(ui64 hardLimit = 100_MB) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(hardLimit);
    config.SetTargetUtilizationBytes(hardLimit);
    config.SetSoftLimitBytes(hardLimit);
    config.SetMemTableMinBytes(0);
    config.SetMemTableMaxBytes(0);
    return config;
}

NKikimrConfig::TMemoryControllerConfig MakeCacheAndQueryConfig(ui64 cacheMin, ui64 cacheMax, ui64 queryLimit) {
    auto config = MakeBudgetConfig();
    config.SetSharedCacheMinBytes(cacheMin);
    config.SetSharedCacheMaxBytes(cacheMax);
    config.SetQueryExecutionLimitBytes(queryLimit);
    return config;
}

NKikimrConfig::TMemoryControllerConfig MakeSoftPressureConfig(ui64 queryLimit = 120_MB) {
    auto config = MakeCacheAndQueryConfig(20_MB, 40_MB, queryLimit);
    config.SetTargetUtilizationBytes(50_MB);
    config.SetSoftLimitBytes(75_MB);
    config.SetMemTableMinBytes(1_MB);
    config.SetMemTableMaxBytes(1_MB);
    return config;
}

// A memory controller on a bare runtime, driven through the provider and edge actors
struct TControllerFixture {
    TTestBasicRuntime Runtime;
    TIntrusivePtr<TFixedProcessMemoryInfoProvider> Provider = MakeIntrusive<TFixedProcessMemoryInfoProvider>();
    TIntrusivePtr<::NMonitoring::TDynamicCounters> CountersRoot = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TActorId MemoryController;

    explicit TControllerFixture(const NKikimrConfig::TMemoryControllerConfig& config) {
        Runtime.Initialize(TAppPrepare().Unwrap());
        MemoryController = Runtime.Register(CreateMemoryController(
            TDuration::Seconds(1), TIntrusivePtr<IProcessMemoryInfoProvider>(Provider), config, TResourceBrokerConfig{}, CountersRoot));
        Runtime.EnableScheduleForActor(MemoryController);
        NActors::TDispatchOptions bootstrapOptions;
        bootstrapOptions.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(bootstrapOptions);
        Counters = GetServiceCounters(CountersRoot, "utils")->GetSubgroup("component", "memory_controller");
    }

    i64 Counter(const TString& name, bool derivative = false) const {
        return Counters->GetCounter(name, derivative)->Val();
    }

    TIntrusivePtr<IMemoryConsumer> Register(TActorId registrant, EMemoryConsumerKind kind) {
        Runtime.Send(new IEventHandle(MemoryController, registrant, new TEvConsumerRegister(kind)));
        return Runtime.GrabEdgeEvent<TEvConsumerRegistered>(registrant)->Get()->Consumer;
    }

    void Tick() {
        Runtime.SimulateSleep(TDuration::Seconds(2));
    }

    void RequestLimit(TActorId registrant, EMemoryConsumerKind kind, ui64 desired) {
        Runtime.Send(new IEventHandle(MemoryController, registrant, new TEvConsumerDemand(kind, desired)));
    }
};

class TWithMemoryControllerServer : public TServer {
public:
    TWithMemoryControllerServer(const TServerSettings& settings)
        : TServer(settings, false)
    {
        PreInitialize();
        Initialize();
    }

    void PrintCounters() const {
        Cerr << "SharedCache:" << Endl;
        Cerr << "    ActiveBytes = " << SharedPageCacheCounters->ActiveBytes->Val() << Endl;
        Cerr << "    PassiveBytes = " << SharedPageCacheCounters->PassiveBytes->Val() << Endl;
        Cerr << "    ConfigLimitBytes = " << SharedPageCacheCounters->ConfigLimitBytes->Val() << Endl;
        Cerr << "    MemLimitBytes = " << SharedPageCacheCounters->MemLimitBytes->Val() << Endl;
    }

private:
    void PreInitialize() {
        ProcessMemoryInfoProvider = MakeIntrusive<TFixedProcessMemoryInfoProvider>();
        ProcessMemoryInfo = &ProcessMemoryInfoProvider->ProcessMemoryInfo;

        // copy-paste from TMemoryControllerInitializer::InitializeServices
        NMemory::TResourceBrokerConfig resourceBrokerSelfConfig;
        const auto& resourceBrokerConfig = Settings->AppConfig->GetResourceBrokerConfig();
        if (resourceBrokerConfig.HasResourceLimit() && resourceBrokerConfig.GetResourceLimit().HasMemory()) {
            resourceBrokerSelfConfig.LimitBytes = resourceBrokerConfig.GetResourceLimit().GetMemory();
        }
        for (const auto& queue : resourceBrokerConfig.GetQueues()) {
            if (queue.HasLimit() && queue.GetLimit().HasMemory()) {
                resourceBrokerSelfConfig.QueueLimits[queue.GetName()] = queue.GetLimit().GetMemory();
            }
        }
        Cerr << "ResourceBrokerSelfConfig: " << resourceBrokerSelfConfig.ToString() << Endl;

        for (ui32 nodeIndex = 0; nodeIndex < Runtime->GetNodeCount(); ++nodeIndex) {
            Runtime->AddLocalService(MakeMemoryControllerId(nodeIndex),
                TActorSetupCmd(
                    CreateMemoryController(TDuration::Seconds(1), (TIntrusivePtr<IProcessMemoryInfoProvider>)ProcessMemoryInfoProvider,
                        Settings->AppConfig->GetMemoryControllerConfig(), resourceBrokerSelfConfig,
                        Runtime->GetDynamicCounters()),
                    TMailboxType::ReadAsFilled,
                    0),
                nodeIndex);
        }

        SharedPageCacheCounters = MakeHolder<TSharedPageCacheCounters>(GetServiceCounters(Runtime->GetDynamicCounters(), "tablets")->GetSubgroup("type", "S_CACHE"));
        MemoryControllerCounters = GetServiceCounters(Runtime->GetDynamicCounters(), "utils")->GetSubgroup("component", "memory_controller");

        Runtime->SetLogPriority(NKikimrServices::MEMORY_CONTROLLER, NLog::PRI_TRACE);
        Runtime->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NLog::PRI_TRACE);
        Runtime->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NLog::PRI_TRACE);
    }

private:
    TIntrusivePtr<TFixedProcessMemoryInfoProvider> ProcessMemoryInfoProvider;

public:
    THolder<TSharedPageCacheCounters> SharedPageCacheCounters;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> MemoryControllerCounters;
    TProcessMemoryInfo* ProcessMemoryInfo;
};

// Objects carry a byte size so the real cache can be exercised without large allocations.
struct TStartupCachePolicy {
    using TAddress = ui64;
    using TObject = ui64;
    using TSourceId = ui64;
    enum class EConsumer { Undefined };

    struct TSizeCalcer {
        size_t operator()(ui64 bytes) const { return bytes; }
    };

    static TSourceId GetSourceId(TAddress) { return 0; }
    static TString GetCacheName() { return "startup_test"; }
    static EMemoryConsumerKind GetConsumerKind() { return EMemoryConsumerKind::ColumnTablesDataAccessorCache; }
    static std::shared_ptr<NGeneralCache::NSource::IObjectsProcessor<TStartupCachePolicy>> BuildObjectsProcessor(TActorId) {
        return nullptr; // The test injects source results directly.
    }
};

// A killable registrant for the death-detection test; edge actors never die
class TStubRegistrant : public TActorBootstrapped<TStubRegistrant> {
public:
    TStubRegistrant(TActorId controller, EMemoryConsumerKind kind, TIntrusivePtr<IMemoryConsumer>* out)
        : Controller(controller)
        , Kind(kind)
        , Out(out)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(Controller, new TEvConsumerRegister(Kind));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvConsumerRegistered, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    void Handle(TEvConsumerRegistered::TPtr& ev) {
        *Out = ev->Get()->Consumer;
    }

private:
    const TActorId Controller;
    const EMemoryConsumerKind Kind;
    TIntrusivePtr<IMemoryConsumer>* const Out;
};

}

Y_UNIT_TEST_SUITE(TMemoryController) {

Y_UNIT_TEST(GeneralCacheReceivesFirstReductionWhileOverBudget) {
    TControllerFixture fixture(MakeCacheAndQueryConfig(0, 400_MB, 40_MB));
    fixture.Runtime.RegisterService(MakeMemoryControllerId(), fixture.MemoryController);
    const auto waiter = fixture.Runtime.AllocateEdgeActor();
    fixture.Register(waiter, EMemoryConsumerKind::ColumnTablesScanGroupedMemory);

    ui64 initialLimit = 0;
    bool registered = false;
    THashMap<TActorId, ui64> limits;
    auto registrations = fixture.Runtime.AddObserver<TEvConsumerRegister>([&](TEvConsumerRegister::TPtr& ev) {
        initialLimit = ev->Get()->InitialLimitBytes;
    });
    auto acknowledgements = fixture.Runtime.AddObserver<TEvConsumerRegistered>([&](TEvConsumerRegistered::TPtr&) {
        registered = true;
    });
    auto grants = fixture.Runtime.AddObserver<TEvConsumerLimit>([&](TEvConsumerLimit::TPtr& ev) {
        limits[ev->Recipient] = ev->Get()->LimitBytes;
    });
    const auto config = NGeneralCache::NPublic::TConfig::BuildDefault();
    NGeneralCache::NPrivate::TActorCounters counters(TStartupCachePolicy::GetCacheName(), fixture.CountersRoot, config);
    const auto cache = fixture.Runtime.Register(NGeneralCache::CreateService<TStartupCachePolicy>(config, fixture.CountersRoot));
    fixture.Runtime.WaitFor("cache registration", [&] { return registered; });
    UNIT_ASSERT_VALUES_EQUAL(initialLimit, 1_GB);

    THashMap<ui64, ui64> objects;
    for (ui64 i = 0; i < 24; ++i) {
        objects.emplace(i, 5_MB);
    }
    using TSourceEvents = NGeneralCache::NSource::TEvents<TStartupCachePolicy>;
    fixture.Runtime.Send(new IEventHandle(cache, waiter,
        new TSourceEvents::TEvObjectsInfo(0, std::move(objects), {}, {})));
    fixture.Runtime.WaitFor("cache filled before first grant", [&] {
        return counters.GetManager()->CacheSizeBytes->Val() == 120_MB;
    });
    UNIT_ASSERT(!limits.contains(cache));

    fixture.Runtime.WaitFor("startup reduction", [&] { return limits.contains(cache); });
    UNIT_ASSERT_VALUES_EQUAL(limits.at(cache), 10_MB);
    UNIT_ASSERT(!limits.contains(waiter));
    fixture.Runtime.WaitFor("grant after eviction report", [&] { return limits.contains(waiter); });
    UNIT_ASSERT_VALUES_EQUAL(counters.GetManager()->CacheSizeBytes->Val(), 10_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesDataAccessorCache/Consumption"), 10_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersBudgetDeficit"), 0);
    UNIT_ASSERT_VALUES_EQUAL(limits.at(waiter), 20_MB);
}

Y_UNIT_TEST(GroupedMemoryRegistersItsStartupHardLimit) {
    using namespace NOlap::NGroupedMemoryManager;
    struct TCase {
        std::optional<ui64> HardLimit;
        ui64 Buckets;
        ui64 Multiplier;
        ui64 Expected;
    };
    const ui64 max = std::numeric_limits<ui64>::max();
    constexpr ui64 compMultiplier = NOlap::TGlobalLimits::CompGroupedMemoryHardLimitMultiplier;
    for (const auto& test : TVector<TCase>{{{}, 2, 1, max}, {{}, 2, compMultiplier, max},
            {259, 2, 1, 258}, {259, 2, compMultiplier, 65}, {0, 2, compMultiplier, 0},
            {max, 3, compMultiplier, max / compMultiplier + 1}}) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto controller = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeMemoryControllerId(), controller);
        NKikimrConfig::TGroupedMemoryLimiterConfig proto;
        proto.SetCountBuckets(test.Buckets);
        if (test.HardLimit) {
            proto.SetHardMemoryLimit(*test.HardLimit);
        }
        NOlap::NGroupedMemoryManager::TConfig config;
        UNIT_ASSERT(config.DeserializeFromProto(proto));
        const auto kind = test.Multiplier == compMultiplier
            ? EMemoryConsumerKind::ColumnTablesCompGroupedMemory : EMemoryConsumerKind::ColumnTablesScanGroupedMemory;
        runtime.Register(new TMemoryLimiterActor(config, "Test", runtime.GetDynamicCounters(), kind, test.Multiplier));
        const auto registration = runtime.GrabEdgeEvent<TEvConsumerRegister>(controller);
        UNIT_ASSERT_VALUES_EQUAL(registration->Get()->InitialLimitBytes, test.Expected);
    }
}

Y_UNIT_TEST(BlobCacheRegistersItsStartupLimit) {
    TTestBasicRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    const auto controller = runtime.AllocateEdgeActor();
    runtime.RegisterService(MakeMemoryControllerId(), controller);
    const auto settings = NBlobCache::TBlobCacheSettings::FromProto(NKikimrConfig::TBlobCacheConfig{});
    runtime.Register(NBlobCache::CreateBlobCache(settings, runtime.GetDynamicCounters()));
    const auto registration = runtime.GrabEdgeEvent<TEvConsumerRegister>(controller);
    UNIT_ASSERT_VALUES_EQUAL(registration->Get()->InitialLimitBytes, settings.MaxCacheDataSize);
}

Y_UNIT_TEST(Counters) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(200_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 200_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/SoftLimit")->Val(), 150_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/TargetUtilization")->Val(), 100_MB);

    server->ProcessMemoryInfo->AnonRss = 44_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 44_MB);

    server->ProcessMemoryInfo->AllocatedMemory = 33_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 33_MB);

    server->ProcessMemoryInfo->CGroupLimit = 1000_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 1000_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 200_MB);

    server->ProcessMemoryInfo->CGroupLimit = 100_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 100_MB);
}

#ifdef _linux_
Y_UNIT_TEST(Counters_MemMapsUnavailable) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_GT(server->MemoryControllerCounters->GetCounter("Stats/MemMapsCount")->Val(), 0);

    rlimit saved;
    UNIT_ASSERT_VALUES_EQUAL(getrlimit(RLIMIT_NOFILE, &saved), 0);
    rlimit noFiles = saved;
    noFiles.rlim_cur = 0;
    UNIT_ASSERT_VALUES_EQUAL(setrlimit(RLIMIT_NOFILE, &noFiles), 0);
    Y_DEFER { setrlimit(RLIMIT_NOFILE, &saved); };

    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/MemMapsCount")->Val(), 0);
}
#endif

Y_UNIT_TEST(Counters_HardLimit) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(1000_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/MemTotal")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 1000_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/SoftLimit")->Val(), 750_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/TargetUtilization")->Val(), 500_MB);

    server->ProcessMemoryInfo->CGroupLimit = 200_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 200_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/MemTotal")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 200_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/SoftLimit")->Val(), 150_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/TargetUtilization")->Val(), 100_MB);
}

Y_UNIT_TEST(Counters_NoHardLimit) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/MemTotal")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 2_GB); // default
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/SoftLimit")->Val(), 1536_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/TargetUtilization")->Val(), 1_GB);

    server->ProcessMemoryInfo->CGroupLimit = 200_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 200_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/MemTotal")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 200_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/SoftLimit")->Val(), 150_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/TargetUtilization")->Val(), 100_MB);

    server->ProcessMemoryInfo->CGroupLimit = {};
    server->ProcessMemoryInfo->MemTotal = 220_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/MemTotal")->Val(), 220_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 220_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/SoftLimit")->Val(), 165_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/TargetUtilization")->Val(), 110_MB);
}

Y_UNIT_TEST(Config_ConsumerLimits) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();

    memoryControllerConfig->SetSharedCacheMinPercent(20);
    memoryControllerConfig->SetSharedCacheMaxPercent(30);
    memoryControllerConfig->SetSharedCacheMinBytes(100_MB);
    memoryControllerConfig->SetSharedCacheMaxBytes(500_MB);

    memoryControllerConfig->SetMemTableMinPercent(10);
    memoryControllerConfig->SetMemTableMaxPercent(20);
    memoryControllerConfig->SetMemTableMinBytes(10_MB);
    memoryControllerConfig->SetMemTableMaxBytes(50_MB);

    memoryControllerConfig->SetQueryExecutionLimitPercent(15);
    memoryControllerConfig->SetQueryExecutionLimitBytes(30_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    server->ProcessMemoryInfo->CGroupLimit = 1000_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/LimitMin")->Val(), 200_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/LimitMax")->Val(), 300_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/LimitMin")->Val(), 50_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/LimitMax")->Val(), 50_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 30_MB);

    server->ProcessMemoryInfo->CGroupLimit = 400_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/LimitMin")->Val(), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/LimitMax")->Val(), 120_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/LimitMin")->Val(), 40_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/LimitMax")->Val(), 50_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 30_MB);

    server->ProcessMemoryInfo->CGroupLimit = 100_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/LimitMin")->Val(), 30_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/LimitMax")->Val(), 30_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/LimitMin")->Val(), 10_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/LimitMax")->Val(), 20_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 15_MB);
}

Y_UNIT_TEST(SharedCache) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(200_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);
    auto [shards, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
    UpsertRows(server, sender);
    CompactTable(runtime, shards[0], tableId1);

    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ConfigLimitBytes->Val(), 32_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), 32_MB);

    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(40_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), 32_MB);

    // Idle fixed grants reserve Hard capacity without reducing cache at Soft.
    server->ProcessMemoryInfo->AllocatedMemory = 4_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(40_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), 32_MB);

    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->ActiveBytes->Val(), static_cast<i64>(32_KB), static_cast<i64>(5_KB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->PassiveBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveBytes->Val(), server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/Consumption")->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 60_MB; // fixed grants leave no Hard capacity for cache
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->MemLimitBytes->Val());

    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveBytes->Val(), 0);
    UNIT_ASSERT_GT(server->SharedPageCacheCounters->PassiveBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->PassiveBytes->Val(), server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/Consumption")->Val());
}

Y_UNIT_TEST(SharedCache_ConfigLimit) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(300_MB);
    serverSettings.AppConfig->MutableSharedCacheConfig()->SetMemoryLimit(50_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ConfigLimitBytes->Val(), 50_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), 50_MB);

    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ConfigLimitBytes->Val(), 50_MB);
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(60_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 60_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ConfigLimitBytes->Val(), 50_MB);
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(27_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->MemLimitBytes->Val());
}

Y_UNIT_TEST(MemTable) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(200_MB);
    // Leave room above fixed grants to exercise MemTable growth and later compaction.
    memoryControllerConfig->SetTargetUtilizationPercent(75);
    memoryControllerConfig->SetSharedCacheMinBytes(0);
    memoryControllerConfig->SetSharedCacheMaxBytes(0);
    memoryControllerConfig->SetMemTableMinPercent(0);
    memoryControllerConfig->SetMemTableMaxPercent(100);
    memoryControllerConfig->SetMemTableMinBytes(100_KB);
    memoryControllerConfig->SetMemTableMaxBytes(10_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);
    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");
    UpsertRows(server, sender);

    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_DOUBLES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Limit")->Val(), static_cast<i64>(10_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_GT(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Consumption")->Val(), static_cast<i64>(100_KB));

    server->ProcessMemoryInfo->AllocatedMemory = 1000_MB;
    // Consumption is refreshed on a controller tick after asynchronous compactions finish.
    runtime.WaitFor("MemTable consumption below limit", [&] {
        return server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Consumption")->Val() <= static_cast<i64>(100_KB);
    }, TDuration::Seconds(10));
    // Hard takes precedence over configured minimums when no capacity remains.
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Limit")->Val(), 0);
    UNIT_ASSERT_LE(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Consumption")->Val(), static_cast<i64>(100_KB));
    UNIT_ASSERT_GT(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Consumption")->Val(), static_cast<i64>(1_KB));
}

Y_UNIT_TEST(ResourceBroker) {
    using namespace NResourceBroker;

    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetQueryExecutionLimitPercent(15);

    auto resourceBrokerConfig = serverSettings.AppConfig->MutableResourceBrokerConfig();
    auto queue = resourceBrokerConfig->AddQueues();
    queue->SetName("queue_cs_scan_read");
    queue->MutableLimit()->SetMemory(13_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    server->ProcessMemoryInfo->CGroupLimit = 1000_MB;
    auto& runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    auto sender = runtime.AllocateEdgeActor();
    auto senderSubscriber = runtime.AllocateEdgeActor();

    InitRoot(server, sender);

    runtime.SimulateSleep(TDuration::Seconds(2));
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue)));
    auto config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(config->Get()->QueueConfig->GetLimit().GetMemory(), 150_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 150_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/ActivitiesLimitBytes")->Val(), 300_MB);

    runtime.SimulateSleep(TDuration::Seconds(2));
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), senderSubscriber, new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue, /*subscribe=*/ true)));
    config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(senderSubscriber);
    UNIT_ASSERT_VALUES_EQUAL(config->Get()->QueueConfig->GetLimit().GetMemory(), 150_MB);

    server->ProcessMemoryInfo->CGroupLimit = 500_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue)));
    config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(config->Get()->QueueConfig->GetLimit().GetMemory(), 75_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 75_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/ActivitiesLimitBytes")->Val(), 150_MB);

    config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(senderSubscriber);
    UNIT_ASSERT_VALUES_EQUAL(config->Get()->QueueConfig->GetLimit().GetMemory(), 75_MB);

    // ensure that other settings are not affected:
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest("queue_cs_scan_read")));
    config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(config->Get()->QueueConfig->GetLimit().GetCpu(), 3);
    UNIT_ASSERT_VALUES_EQUAL(config->Get()->QueueConfig->GetLimit().GetMemory(), 13_MB);
}

Y_UNIT_TEST(ResourceBroker_ConfigLimit) {
    using namespace NResourceBroker;

    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetQueryExecutionLimitPercent(15);

    auto resourceBrokerConfig = serverSettings.AppConfig->MutableResourceBrokerConfig();
    resourceBrokerConfig->MutableResourceLimit()->SetMemory(1000_MB);
    auto queue = resourceBrokerConfig->AddQueues();
    queue->SetName(NLocalDb::KqpResourceManagerQueue);
    queue->MutableLimit()->SetMemory(999_MB);
    queue = resourceBrokerConfig->AddQueues();
    queue->SetName("queue_cs_scan_read");
    queue->MutableLimit()->SetMemory(13_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    server->ProcessMemoryInfo->CGroupLimit = 500_MB;
    auto& runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);

    runtime.SimulateSleep(TDuration::Seconds(2));
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue)));
    auto config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(config->QueueConfig->GetLimit().GetMemory(), 999_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 999_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/ActivitiesLimitBytes")->Val(), 1000_MB);

    server->ProcessMemoryInfo->CGroupLimit = 200_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue)));
    config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(config->QueueConfig->GetLimit().GetMemory(), 999_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 999_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/ActivitiesLimitBytes")->Val(), 1000_MB);

    // ensure that other settings are not affected:
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest("queue_cs_scan_read")));
    config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(config->QueueConfig->GetLimit().GetCpu(), 3);
    UNIT_ASSERT_VALUES_EQUAL(config->QueueConfig->GetLimit().GetMemory(), 13_MB);
}

Y_UNIT_TEST(ResourceBroker_ConfigCS) {
    using namespace NResourceBroker;

    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root").SetUseRealThreads(false);

    const ui64 compactionMemoryLimitPercent = 36;
    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetCompactionLimitPercent(compactionMemoryLimitPercent);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    auto sender = runtime.AllocateEdgeActor();
    InitRoot(server, sender);

    ui64 currentHardMemoryLimit = 1000_MB;
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    runtime.SimulateSleep(TDuration::Seconds(2));

    auto checkMemoryLimit = [&](const TString& queueName, const double coeff) {
        runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest(queueName)));
        auto config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(handle);
        UNIT_ASSERT_DOUBLES_EQUAL_C(
            static_cast<double>(config->QueueConfig->GetLimit().GetMemory()),
            static_cast<double>(currentHardMemoryLimit * coeff * compactionMemoryLimitPercent / 100),
            1_KB,
            queueName << " " << coeff);
    };

    checkMemoryLimit(NLocalDb::ColumnShardCompactionIndexationQueue, ColumnTablesCompactionIndexationQueueFraction);
    checkMemoryLimit(NLocalDb::ColumnShardCompactionTtlQueue, ColumnTablesTtlQueueFraction);
    checkMemoryLimit(NLocalDb::ColumnShardCompactionGeneralQueue, ColumnTablesGeneralQueueFraction);
    checkMemoryLimit(NLocalDb::ColumnShardCompactionNormalizerQueue, ColumnTablesNormalizerQueueFraction);

    Cerr << "Check memory change" << Endl;
    currentHardMemoryLimit = 100_MB;
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    runtime.SimulateSleep(TDuration::Seconds(2));

    checkMemoryLimit(NLocalDb::ColumnShardCompactionIndexationQueue, ColumnTablesCompactionIndexationQueueFraction);
    checkMemoryLimit(NLocalDb::ColumnShardCompactionTtlQueue, ColumnTablesTtlQueueFraction);
    checkMemoryLimit(NLocalDb::ColumnShardCompactionGeneralQueue, ColumnTablesGeneralQueueFraction);
    checkMemoryLimit(NLocalDb::ColumnShardCompactionNormalizerQueue, ColumnTablesNormalizerQueueFraction);
}

Y_UNIT_TEST(GroupedMemoryLimiter_ConfigCS) {
    using namespace NResourceBroker;

    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root").SetUseRealThreads(false);

    const ui64 compactionMemoryLimitPercent = 36;
    const ui64 readExecutionMemoryLimitPercent = 20;
    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetCompactionLimitPercent(compactionMemoryLimitPercent);
    memoryControllerConfig->SetQueryExecutionLimitPercent(readExecutionMemoryLimitPercent);

    ui64 currentHardMemoryLimit = 1000_MB;
    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    auto& runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    auto sender = runtime.AllocateEdgeActor();

    auto counters = runtime.GetAppData().Counters;
    auto compactionCounters = counters->GetSubgroup("module_id", "grouped_memory_limiter")->GetSubgroup("limiter_name", "Comp_0")->GetSubgroup("stage", "general");
    auto scanCounters = counters->GetSubgroup("module_id", "grouped_memory_limiter")->GetSubgroup("limiter_name", "Scan_0")->GetSubgroup("stage", "general");
    auto dedupCounters = counters->GetSubgroup("module_id", "grouped_memory_limiter")->GetSubgroup("limiter_name", "Dedu_0")->GetSubgroup("stage", "general");

    InitRoot(server, sender);

    auto checkMemoryLimits = [&]() {
        using OlapLimits = NKikimr::NOlap::TGlobalLimits;
        const auto issued = [&](const TString& kind) {
            return server->MemoryControllerCounters->GetCounter("Consumer/" + kind + "/Limit")->Val();
        };
        const auto check = [&](const auto& counters, const TString& kind, ui64 multiplier) {
            const auto limit = issued(kind) * multiplier;
            UNIT_ASSERT_DOUBLES_EQUAL(counters->GetCounter("Value/Limit/Soft/Bytes")->Val(),
                limit * OlapLimits::GroupedMemoryLimiterSoftLimitCoefficient, 1_KB);
            UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("Value/Limit/Hard/Bytes")->Val(), limit);
        };
        check(scanCounters, "ColumnTablesScanGroupedMemory", NOlap::NGroupedMemoryManager::TScanMemoryLimiterPolicy::HardLimitMultiplier);
        check(dedupCounters, "ColumnTablesDeduplicationGroupedMemory", NOlap::NGroupedMemoryManager::TDeduplicationMemoryLimiterPolicy::HardLimitMultiplier);
        check(compactionCounters, "ColumnTablesCompGroupedMemory", NOlap::NGroupedMemoryManager::TCompMemoryLimiterPolicy::HardLimitMultiplier);
        // The configured Comp hard ceiling alone is 144% of Hard. Actual grants
        // must be smaller and collectively fit, also after a runtime decrease.
        const ui64 effective = issued("ColumnTablesScanGroupedMemory") + issued("ColumnTablesDeduplicationGroupedMemory")
            + issued("ColumnTablesCompGroupedMemory") * NOlap::NGroupedMemoryManager::TCompMemoryLimiterPolicy::HardLimitMultiplier;
        UNIT_ASSERT(effective <= currentHardMemoryLimit);
        UNIT_ASSERT_LT(ui64(issued("ColumnTablesCompGroupedMemory")), currentHardMemoryLimit * compactionMemoryLimitPercent / 100);
    };

    runtime.SimulateSleep(TDuration::Seconds(2));
    checkMemoryLimits();

    // Check memory decrease
    currentHardMemoryLimit = 500_MB;
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    runtime.SimulateSleep(TDuration::Seconds(2));
    checkMemoryLimits();

    // Check memory increase
    currentHardMemoryLimit = 2000_MB;
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    runtime.SimulateSleep(TDuration::Seconds(2));
    checkMemoryLimits();
}

Y_UNIT_TEST(ColumnShardCaches_Config) {
    using namespace NResourceBroker;

    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root").SetUseRealThreads(false);

    const ui64 sharedCacheMaxPercent = 50;
    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetSharedCacheMaxPercent(sharedCacheMaxPercent);

    ui64 currentHardMemoryLimit = 1000_MB;
    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    auto& runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);
    auto counters = runtime.GetAppData().Counters;
    auto dataAccessorCache = counters->GetSubgroup("module_id", "general_cache")->GetSubgroup("cache_name", "portions_metadata")->GetSubgroup("signals_owner", "manager");
    auto columnDataCache = counters->GetSubgroup("module_id", "general_cache")->GetSubgroup("cache_name", "column_data")->GetSubgroup("signals_owner", "manager");
    auto blobCache = counters->GetSubgroup("type", "BLOB_CACHE");

    auto checkMemoryLimits = [&]() {
        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * ColumnTablesPortionsMetaDataCacheFraction * sharedCacheMaxPercent / 100.0 * ColumnTablesCachesPercentFromShared / 100.0),
            static_cast<double>(NKikimr::NOlap::NStorageOptimizer::IOptimizerPlanner::GetPortionsCacheLimit()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * ColumnTablesColumnTablesDataAccessorCacheFraction * sharedCacheMaxPercent / 100.0 * ColumnTablesCachesPercentFromShared / 100.0),
            static_cast<double>(dataAccessorCache->GetCounter("Value/Cache/SizeLimit/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * ColumnTablesColumnDataCacheFraction * sharedCacheMaxPercent / 100.0 * ColumnTablesCachesPercentFromShared / 100.0),
            static_cast<double>(columnDataCache->GetCounter("Value/Cache/SizeLimit/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * ColumnTablesBlobCacheFraction * sharedCacheMaxPercent / 100.0 * ColumnTablesCachesPercentFromShared / 100.0),
            static_cast<double>(blobCache->GetCounter("MaxSizeBytes")->Val()),
            1_KB);
    };

    runtime.SimulateSleep(TDuration::Seconds(2));
    checkMemoryLimits();

    // Check memory decrease
    currentHardMemoryLimit = 500_MB;
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    runtime.SimulateSleep(TDuration::Seconds(2));
    checkMemoryLimits();

    // Check memory increase
    currentHardMemoryLimit = 2000_MB;
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    runtime.SimulateSleep(TDuration::Seconds(2));
    checkMemoryLimits();
}

Y_UNIT_TEST(IdleFixedLimitKeepsCacheWithinHardBudget) {
    const auto config = MakeCacheAndQueryConfig(0, 50_MB, 120_MB); // scan receives half
    TControllerFixture fixture(config);
    const auto cache = fixture.Runtime.AllocateEdgeActor();
    const auto scan = fixture.Runtime.AllocateEdgeActor();
    fixture.Register(cache, EMemoryConsumerKind::SharedCache);
    fixture.Register(scan, EMemoryConsumerKind::ColumnTablesScanGroupedMemory);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 60_MB);
    UNIT_ASSERT(fixture.Counter("Consumer/SharedCache/Limit") < i64(40_MB));
    UNIT_ASSERT(fixture.Counter("Stats/ConsumersCommitted") <= i64(100_MB));
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersConsumption"), 0);
}

Y_UNIT_TEST(IdleFixedGrantsDoNotCauseSoftPressure) {
    TControllerFixture fixture(MakeSoftPressureConfig()); // scan reserves 60 MB
    fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::SharedCache);
    fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::ColumnTablesScanGroupedMemory);

    // The fixed caps plus MemTable reserve 66 MB. Target is inaccessible,
    // but there is room under Hard for the whole 20 MB cache minimum.
    for (const ui64 other : {0_MB, 4_MB, 10_MB, 20_MB}) {
        fixture.Provider->ProcessMemoryInfo.AllocatedMemory = other;
        fixture.Tick();
        const ui64 cacheLimit = other == 20_MB ? 14_MB : 20_MB;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/Coefficient"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersConsumption"), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ResultingConsumersConsumption"), 21_MB);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/RequiredConsumersBudget"), 86_MB);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), cacheLimit);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 60_MB);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersCommitted"), 66_MB + cacheLimit);
        UNIT_ASSERT(fixture.Counter("Stats/ConsumersCommitted") <= i64(100_MB - other));
    }
}

Y_UNIT_TEST(SameKindUnreleasedUsageTrimsCacheForSibling) {
    TControllerFixture fixture(MakeSoftPressureConfig(128_MB)); // scan shares 64 MB
    const auto cache = fixture.Runtime.AllocateEdgeActor();
    const auto first = fixture.Runtime.AllocateEdgeActor();
    const auto second = fixture.Runtime.AllocateEdgeActor();
    fixture.Register(cache, EMemoryConsumerKind::SharedCache);
    auto firstConsumer = fixture.Register(first, EMemoryConsumerKind::ColumnTablesScanGroupedMemory);
    auto secondConsumer = fixture.Register(second, EMemoryConsumerKind::ColumnTablesScanGroupedMemory);
    THashMap<TActorId, ui64> limits;
    auto observer = fixture.Runtime.AddObserver<TEvConsumerLimit>([&](TEvConsumerLimit::TPtr& ev) {
        limits[ev->Recipient] = ev->Get()->LimitBytes;
    });

    firstConsumer->SetReport({40_MB, 40_MB, 0});
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(limits.at(first), 52_MB);
    UNIT_ASSERT_VALUES_EQUAL(limits.at(second), 12_MB);
    UNIT_ASSERT_VALUES_EQUAL(limits.at(cache), 20_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/RequiredConsumersBudget"), 90_MB);

    // The first consumer still uses 40 MB after its share falls to 24 MB.
    // Charge those bytes separately from the second consumer's 40 MB share.
    secondConsumer->SetReport({0, 72_MB, 0});
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/RequiredConsumersBudget"), 106_MB);
    UNIT_ASSERT_VALUES_EQUAL(limits.at(cache), 14_MB);
    UNIT_ASSERT_VALUES_EQUAL(limits.at(first), 24_MB);
    UNIT_ASSERT_VALUES_EQUAL(limits.at(second), 40_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 64_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersCommitted"), 100_MB);

    // Once that usage is released, the same shares need no cache reduction.
    firstConsumer->SetReport({0, 40_MB, 0});
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/RequiredConsumersBudget"), 90_MB);
    UNIT_ASSERT_VALUES_EQUAL(limits.at(cache), 20_MB);
    UNIT_ASSERT_VALUES_EQUAL(limits.at(first), 24_MB);
    UNIT_ASSERT_VALUES_EQUAL(limits.at(second), 40_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersCommitted"), 90_MB);
}

Y_UNIT_TEST(RealConsumptionStillCausesSoftPressure) {
    TControllerFixture fixture(MakeSoftPressureConfig());
    auto cache = fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::SharedCache);
    auto scan = fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::ColumnTablesScanGroupedMemory);
    fixture.Tick();
    scan->SetConsumption(60_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ResultingConsumersConsumption"), 81_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 14_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 60_MB);

    // Cache usage above its target still contributes to pressure until released.
    cache->SetConsumption(30_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ResultingConsumersConsumption"), 91_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 4_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersCommitted"), 96_MB);

    cache->SetConsumption(4_MB);
    scan->SetConsumption(0);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 20_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersCommitted"), 86_MB);
}

Y_UNIT_TEST(ExternalConsumptionReducesAvailableHardBudget) {
    auto config = MakeSoftPressureConfig();
    config.ClearHardLimitBytes();
    TControllerFixture fixture(config);
    fixture.Provider->ProcessMemoryInfo.MemTotal = 100_MB;
    fixture.Provider->ProcessMemoryInfo.AnonRss = 0;
    fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::SharedCache);
    fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::ColumnTablesScanGroupedMemory);
    for (const ui64 external : {0_MB, 20_MB, 80_MB}) {
        fixture.Provider->ProcessMemoryInfo.MemAvailable = 100_MB - external;
        fixture.Tick();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ExternalConsumption"), external);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"),
            external == 0 ? 20_MB : external == 20_MB ? 14_MB : 0);
        UNIT_ASSERT(fixture.Counter("Stats/ConsumersCommitted") <= i64(100_MB - external));
    }
}

Y_UNIT_TEST(CompactionReservationSurvivesSoftPressureSplit) {
    auto config = MakeSoftPressureConfig(60_MB); // scan reserves 30 MB
    config.SetCompactionLimitBytes(10_MB); // GML can use four times the nominal grant
    TControllerFixture fixture(config);
    fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::SharedCache);
    fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::ColumnTablesScanGroupedMemory);
    fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::ColumnTablesCompGroupedMemory);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/RequiredConsumersBudget"), 96_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 20_MB);

    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 5_MB;
    for (const ui64 hard : {100_MB, 90_MB}) {
        fixture.Provider->ProcessMemoryInfo.CGroupLimit = hard;
        fixture.Tick();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), hard - 81_MB);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesCompGroupedMemory/Limit"), 10_MB);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 30_MB);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersCommitted"), hard - 5_MB);
    }
    // Cache reaches zero before the ledger has to reduce fixed permissions.
    fixture.Provider->ProcessMemoryInfo.CGroupLimit = 70_MB;
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 0);
    UNIT_ASSERT(fixture.Counter("Stats/ConsumersCommitted") <= i64(65_MB));
    UNIT_ASSERT(fixture.Counter("Consumer/ColumnTablesCompGroupedMemory/Limit") < i64(10_MB));
}

Y_UNIT_TEST(CacheConsumptionMustFallBeforeSendingTheFullIncrease) {
    const auto config = MakeCacheAndQueryConfig(50_MB, 50_MB, 100_MB);
    TControllerFixture fixture(config);
    const auto cache = fixture.Runtime.AllocateEdgeActor();
    const auto scan = fixture.Runtime.AllocateEdgeActor();
    auto cacheConsumer = fixture.Register(cache, EMemoryConsumerKind::SharedCache);
    cacheConsumer->SetConsumption(50_MB);
    fixture.Tick();
    size_t scanLimitEvents = 0;
    auto observer = fixture.Runtime.AddObserver<TEvConsumerLimit>([&](TEvConsumerLimit::TPtr& ev) {
        if (ev->Recipient == scan) {
            ++scanLimitEvents;
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->LimitBytes, 50_MB);
        }
    });
    fixture.Register(scan, EMemoryConsumerKind::ColumnTablesScanGroupedMemory);
    fixture.Tick();
    const ui64 cacheLimit = fixture.Counter("Consumer/SharedCache/Limit");
    UNIT_ASSERT(cacheLimit < 50_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 0);
    UNIT_ASSERT_VALUES_EQUAL(scanLimitEvents, 0);

    cacheConsumer->SetConsumption((50_MB + cacheLimit) / 2);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 0);
    UNIT_ASSERT_VALUES_EQUAL(scanLimitEvents, 0);

    cacheConsumer->SetConsumption(cacheLimit);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 50_MB);
    UNIT_ASSERT_GT(scanLimitEvents, 0);
    UNIT_ASSERT(fixture.Counter("Stats/ConsumersCommitted") <= i64(100_MB));
}

Y_UNIT_TEST(MemTableCompactionContinuesWhileItsIncreaseWaits) {
    auto config = MakeBudgetConfig(200_MB);
    config.SetMemTableMinBytes(10_MB);
    config.SetMemTableMaxBytes(20_MB);
    config.SetSharedCacheMinBytes(0);
    config.SetSharedCacheMaxBytes(80_MB);
    TControllerFixture fixture(config);
    fixture.Provider->ProcessMemoryInfo.CGroupLimit = 100_MB;
    const auto cache = fixture.Runtime.AllocateEdgeActor();
    const auto owner = fixture.Runtime.AllocateEdgeActor();
    auto cacheConsumer = fixture.Register(cache, EMemoryConsumerKind::SharedCache);
    fixture.Runtime.Send(new IEventHandle(fixture.MemoryController, owner, new TEvMemTableRegister(7)));
    auto memTable = fixture.Runtime.GrabEdgeEvent<TEvMemTableRegistered>(owner)->Get()->Consumer;
    fixture.Tick();
    const auto previousLimit = fixture.Counter("Consumer/MemTable/Limit");
    UNIT_ASSERT(previousLimit > 0 && previousLimit < i64(20_MB));

    bool compactionRequested = false;
    auto observer = fixture.Runtime.AddObserver<TEvMemTableCompact>([&](TEvMemTableCompact::TPtr& ev) {
        if (ev->Recipient == owner) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Table, 7);
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->ExpectedSize, 40_MB);
            compactionRequested = true;
        }
    });
    // Capacity recovers, but unfreed usage still blocks the desired limit increase.
    fixture.Provider->ProcessMemoryInfo.CGroupLimit = 120_MB;
    cacheConsumer->SetConsumption(90_MB);
    memTable->SetConsumption(40_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/MemTable/Limit"), previousLimit);
    UNIT_ASSERT_GT(fixture.Counter("Stats/ConsumersBudgetDeficit"), 0);
    UNIT_ASSERT(compactionRequested);
}

Y_UNIT_TEST(CompetingDesiredRequestsKeepDefaultConsumersFunded) {
    auto config = MakeCacheAndQueryConfig(40_MB, 40_MB, 80_MB);
    config.SetMemTableMinBytes(10_MB);
    config.SetMemTableMaxBytes(10_MB);
    TControllerFixture fixture(config);
    const auto cache = fixture.Runtime.AllocateEdgeActor();
    const auto first = fixture.Runtime.AllocateEdgeActor();
    const auto second = fixture.Runtime.AllocateEdgeActor();
    auto cacheConsumer = fixture.Register(cache, EMemoryConsumerKind::SharedCache);
    constexpr auto kind = EMemoryConsumerKind::ColumnTablesScanGroupedMemory;
    fixture.Register(first, kind);
    fixture.Register(second, kind);
    cacheConsumer->SetConsumption(40_MB);
    fixture.Tick();
    fixture.RequestLimit(first, kind, 60_MB);
    fixture.RequestLimit(second, kind, 60_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 5_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/MemTable/Limit"), 10_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 40_MB);

    cacheConsumer->SetConsumption(5_MB);
    for (size_t round = 0; round < 3; ++round) {
        fixture.Tick();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 5_MB);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/MemTable/Limit"), 10_MB);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 80_MB);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersCommitted"), 100_MB);
    }
}

Y_UNIT_TEST(ClientDesiredWaitsForFullGrantThenReplacesConfiguredTarget) {
    const auto config = MakeCacheAndQueryConfig(60_MB, 60_MB, 40_MB); // scan default is 20 MB
    TControllerFixture fixture(config);
    const auto cache = fixture.Runtime.AllocateEdgeActor();
    const auto scan = fixture.Runtime.AllocateEdgeActor();
    auto cacheConsumer = fixture.Register(cache, EMemoryConsumerKind::SharedCache);
    auto scanConsumer = fixture.Register(scan, EMemoryConsumerKind::ColumnTablesScanGroupedMemory);
    cacheConsumer->SetConsumption(60_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 20_MB);
    TVector<ui64> scanLimits;
    auto observer = fixture.Runtime.AddObserver<TEvConsumerLimit>([&](TEvConsumerLimit::TPtr& ev) {
        if (ev->Recipient == scan) {
            scanLimits.push_back(ev->Get()->LimitBytes);
        }
    });

    fixture.RequestLimit(scan, EMemoryConsumerKind::ColumnTablesScanGroupedMemory, 60_MB);
    fixture.Tick();
    const ui64 cacheLimit = fixture.Counter("Consumer/SharedCache/Limit");
    UNIT_ASSERT(cacheLimit < 60_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 20_MB);
    UNIT_ASSERT(scanLimits.empty());
    cacheConsumer->SetConsumption((60_MB + cacheLimit) / 2);
    fixture.Tick();
    UNIT_ASSERT(scanLimits.empty());
    cacheConsumer->SetConsumption(cacheLimit);
    fixture.Tick();
    UNIT_ASSERT(!scanLimits.empty());
    for (const auto limit : scanLimits) {
        UNIT_ASSERT_VALUES_EQUAL(limit, 60_MB);
    }
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 60_MB);
    UNIT_ASSERT(fixture.Counter("Stats/ConsumersCommitted") <= i64(100_MB));

    fixture.RequestLimit(scan, EMemoryConsumerKind::ColumnTablesScanGroupedMemory, 30_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(scanLimits.back(), 30_MB);
    fixture.RequestLimit(scan, EMemoryConsumerKind::ColumnTablesScanGroupedMemory, 0);
    scanConsumer->SetReport({.Used = 0, .Demand = 100_MB, .Reclaimable = 0});
    fixture.Tick();
    fixture.Tick(); // neither a report nor another round restores the configured target
    UNIT_ASSERT_VALUES_EQUAL(scanLimits.back(), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 0);
}

Y_UNIT_TEST(ClientDesiredIsScopedToItsRegistration) {
    auto config = MakeBudgetConfig(200_MB);
    config.SetQueryExecutionLimitBytes(80_MB); // two scan clients default to 20 MB each
    TControllerFixture fixture(config);
    const auto first = fixture.Runtime.AllocateEdgeActor();
    const auto second = fixture.Runtime.AllocateEdgeActor();
    const auto stranger = fixture.Runtime.AllocateEdgeActor();
    constexpr auto kind = EMemoryConsumerKind::ColumnTablesScanGroupedMemory;
    fixture.Register(first, kind);
    fixture.Register(second, kind);
    fixture.Tick();
    THashMap<TActorId, ui64> lastLimit;
    auto observer = fixture.Runtime.AddObserver<TEvConsumerLimit>([&](TEvConsumerLimit::TPtr& ev) {
        lastLimit[ev->Recipient] = ev->Get()->LimitBytes;
    });
    fixture.RequestLimit(first, kind, 35_MB);
    fixture.RequestLimit(second, kind, 15_MB);
    fixture.RequestLimit(stranger, kind, 999_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(lastLimit.at(first), 35_MB);
    UNIT_ASSERT_VALUES_EQUAL(lastLimit.at(second), 15_MB);
    UNIT_ASSERT(!lastLimit.contains(stranger));
    fixture.Runtime.Send(new IEventHandle(fixture.MemoryController, first, new TEvConsumerUnregister(kind)));
    fixture.RequestLimit(first, kind, 999_MB); // detached requests cannot recreate a registration
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 15_MB);
    fixture.Register(first, kind);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(lastLimit.at(first), 20_MB);
    UNIT_ASSERT_VALUES_EQUAL(lastLimit.at(second), 15_MB);
}

Y_UNIT_TEST(ImpossibleDesiredKeepsCacheBudgetAndDeadWaiterIsRemoved) {
    const auto config = MakeCacheAndQueryConfig(10_MB, 60_MB, 40_MB);
    TControllerFixture fixture(config);
    fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::SharedCache);
    TIntrusivePtr<IMemoryConsumer> doomedConsumer;
    constexpr auto kind = EMemoryConsumerKind::ColumnTablesScanGroupedMemory;
    const auto doomed = fixture.Runtime.Register(new TStubRegistrant(fixture.MemoryController, kind, &doomedConsumer));
    fixture.Tick();
    UNIT_ASSERT(doomedConsumer);
    const auto cacheLimit = fixture.Counter("Consumer/SharedCache/Limit");
    UNIT_ASSERT(cacheLimit > i64(50_MB));
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 20_MB);
    size_t limitEvents = 0;
    auto observer = fixture.Runtime.AddObserver<TEvConsumerLimit>([&](TEvConsumerLimit::TPtr& ev) {
        if (ev->Recipient == doomed) {
            ++limitEvents;
        }
    });

    fixture.RequestLimit(doomed, kind, 1000_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), cacheLimit);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 20_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumerRegistrantDeaths", true), 0);
    UNIT_ASSERT_VALUES_EQUAL(limitEvents, 0);

    fixture.Runtime.Send(new IEventHandle(doomed, TActorId(), new TEvents::TEvPoison));
    fixture.Runtime.SimulateSleep(TDuration::Seconds(3));
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumerRegistrantDeaths", true), 1);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Limit"), 0);
    UNIT_ASSERT_VALUES_EQUAL(limitEvents, 0); // runtime liveness, not a disguised limit update
    doomedConsumer->SetConsumption(1000_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesScanGroupedMemory/Consumption"), 0);
}

Y_UNIT_TEST(ConsumerReportDegradedCoefficient) {
    // Min != Max for SharedCache, so the searched coefficient is what turns its limit
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    config.SetSharedCacheMinBytes(20_MB);
    config.SetSharedCacheMaxBytes(60_MB);
    config.SetMemTableMinBytes(10_MB);
    config.SetMemTableMaxBytes(10_MB);
    config.SetQueryExecutionLimitBytes(50_MB);
    TControllerFixture fixture(config);

    const TActorId sender = fixture.Runtime.AllocateEdgeActor();
    auto sharedCacheConsumer = fixture.Register(sender, EMemoryConsumerKind::SharedCache);
    fixture.Runtime.Send(new IEventHandle(fixture.MemoryController, sender, new TEvMemTableRegister(1)));
    auto memTableConsumer = fixture.Runtime.GrabEdgeEvent<TEvMemTableRegistered>(sender)->Get()->Consumer;
    sharedCacheConsumer->SetConsumption(30_MB);
    memTableConsumer->SetConsumption(7_MB);

    // Nothing else allocated: the whole elastic range fits, so the search saturates at 1 - 2^-20
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/Coefficient"), 999999046);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 62914520);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Runtime.GrabEdgeEvent<TEvConsumerLimit>(sender)->Get()->LimitBytes, 62914520);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Reservation"), 31457240);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/MemTable/Limit"), 10_MB);

    // Foreign allocation leaves less than the minimums demand: the search bottoms out and the limit is the minimum
    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 120_MB;
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/Coefficient"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 20_MB);
    // The lower limit is recorded immediately; usage above it stays charged.
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Reservation"), 0);

    // The target also reserves the 7.5 MB portions-cache allowance: 100 - 53 - 10 - 7.5 = 29.5 MB.
    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 90_MB;
    fixture.Tick();
    UNIT_ASSERT_DOUBLES_EQUAL(fixture.Counter("Stats/Coefficient") / 1e9, (29.5 - 20.0) / 40.0, 1e-5);
    UNIT_ASSERT_DOUBLES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit") / double(1_MB), 29.5, 0.01);
    UNIT_ASSERT_DOUBLES_EQUAL(fixture.Counter("Consumer/SharedCache/Reservation") / double(1_MB), 0.0, 0.01);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/MemTable/Limit"), 10_MB);
}

Y_UNIT_TEST(ConsumerReportClamp) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    TControllerFixture fixture(config);
    auto consumer = fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::ColumnTablesBlobCache);

    // An invalid report that breaks both invariants is clamped where MC reads it
    consumer->SetReport({.Used = 100_MB, .Demand = 10_MB, .Reclaimable = 500_MB});
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesBlobCache/Consumption"), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesBlobCache/Demand"), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesBlobCache/Reclaimable"), 100_MB);
}

Y_UNIT_TEST(SetConsumptionForwardsDegradedReport) {
    struct TRecorder : public IMemoryConsumer {
        TConsumerReport Last;

        void SetReport(TConsumerReport report) override {
            Last = report;
        }
    };

    auto recorder = MakeIntrusive<TRecorder>();
    recorder->SetConsumption(42);
    UNIT_ASSERT_VALUES_EQUAL(recorder->Last.Used, 42);
    UNIT_ASSERT_VALUES_EQUAL(recorder->Last.Demand, 42);
    UNIT_ASSERT_VALUES_EQUAL(recorder->Last.Reclaimable, 0);
}

Y_UNIT_TEST(ConsumerKindSumsRegistrants) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    // Min == Max pins the kind limit to 60 MB, so the shares are exact regardless of the coefficient
    config.SetSharedCacheMinBytes(60_MB);
    config.SetSharedCacheMaxBytes(60_MB);
    TControllerFixture fixture(config);

    const TActorId first = fixture.Runtime.AllocateEdgeActor();
    const TActorId second = fixture.Runtime.AllocateEdgeActor();
    auto firstConsumer = fixture.Register(first, EMemoryConsumerKind::SharedCache);
    auto secondConsumer = fixture.Register(second, EMemoryConsumerKind::SharedCache);
    UNIT_ASSERT(firstConsumer.Get() != secondConsumer.Get());

    firstConsumer->SetReport({.Used = 10_MB, .Demand = 30_MB, .Reclaimable = 5_MB});
    secondConsumer->SetConsumption(20_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Consumption"), 30_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Demand"), 50_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Reclaimable"), 5_MB);

    // Demand fits the 60 MB limit: each registrant gets its demand plus half of the 10 MB left over
    UNIT_ASSERT_VALUES_EQUAL(fixture.Runtime.GrabEdgeEvent<TEvConsumerLimit>(first)->Get()->LimitBytes, 35_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Runtime.GrabEdgeEvent<TEvConsumerLimit>(second)->Get()->LimitBytes, 25_MB);

    // The unregistered entry leaves the sum and the survivor gets the whole limit back
    fixture.Runtime.Send(new IEventHandle(fixture.MemoryController, second, new TEvConsumerUnregister(EMemoryConsumerKind::SharedCache)));
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Consumption"), 10_MB);
    // Skip the queued pre-unregister limits; every tick keeps producing one, so the drain must be bounded
    ui64 lastLimit = 0;
    for (size_t attempt = 0; attempt < 5 && lastLimit != 60_MB; ++attempt) {
        lastLimit = fixture.Runtime.GrabEdgeEvent<TEvConsumerLimit>(first)->Get()->LimitBytes;
    }
    UNIT_ASSERT_VALUES_EQUAL(lastLimit, 60_MB);
}

Y_UNIT_TEST(DeadRegistrantLeavesTheSum) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    TControllerFixture fixture(config);

    TIntrusivePtr<IMemoryConsumer> doomedConsumer;
    const TActorId doomed = fixture.Runtime.Register(new TStubRegistrant(fixture.MemoryController, EMemoryConsumerKind::SharedCache, &doomedConsumer));
    auto survivorConsumer = fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::SharedCache);
    fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    UNIT_ASSERT(doomedConsumer);

    doomedConsumer->SetConsumption(25_MB);
    survivorConsumer->SetConsumption(5_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Consumption"), 30_MB);

    // The poisoned registrant stops existing; the next tracked limit send detects the death
    fixture.Runtime.Send(new IEventHandle(doomed, TActorId(), new TEvents::TEvPoison));
    fixture.Runtime.SimulateSleep(TDuration::Seconds(3));
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumerRegistrantDeaths", true), 1);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Consumption"), 5_MB);
}

Y_UNIT_TEST(ConsumerUnregisterDropsAccounting) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    TControllerFixture fixture(config);

    const TActorId owner = fixture.Runtime.AllocateEdgeActor();
    auto consumer = fixture.Register(owner, EMemoryConsumerKind::SharedCache);
    consumer->SetReport({.Used = 30_MB, .Demand = 40_MB, .Reclaimable = 10_MB});
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersConsumption"), 30_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Demand"), 40_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Reclaimable"), 10_MB);

    // A stale unregister from an actor that no longer owns the kind changes nothing
    const TActorId stranger = fixture.Runtime.AllocateEdgeActor();
    fixture.Runtime.Send(new IEventHandle(fixture.MemoryController, stranger, new TEvConsumerUnregister(EMemoryConsumerKind::SharedCache)));
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersConsumption"), 30_MB);

    // The owner's unregister takes its bytes out of the accounting and zeroes the gauges of the removed kind
    fixture.Runtime.Send(new IEventHandle(fixture.MemoryController, owner, new TEvConsumerUnregister(EMemoryConsumerKind::SharedCache)));
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersConsumption"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Consumption"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Demand"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Reclaimable"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 0);

    // Writes through the unregistered object no longer reach MC
    consumer->SetConsumption(5_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersConsumption"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Consumption"), 0);
}
}

}
