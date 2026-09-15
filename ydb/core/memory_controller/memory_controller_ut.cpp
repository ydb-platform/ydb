#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <memory_controller_config.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tablet_flat/shared_cache_counters.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <util/generic/scope.h>

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

class TWithMemoryControllerServer : public TServer {
    struct TProcessMemoryInfoProvider : public IProcessMemoryInfoProvider {
        TProcessMemoryInfo Get() const override {
            return ProcessMemoryInfo;
        }

        TProcessMemoryInfo ProcessMemoryInfo{0_MB, 0_MB, {}, {}, {}, {}};
    };

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
        ProcessMemoryInfoProvider = MakeIntrusive<TProcessMemoryInfoProvider>();
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
    TIntrusivePtr<TProcessMemoryInfoProvider> ProcessMemoryInfoProvider;

public:
    THolder<TSharedPageCacheCounters> SharedPageCacheCounters;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> MemoryControllerCounters;
    TProcessMemoryInfo* ProcessMemoryInfo;
};

}

Y_UNIT_TEST_SUITE(TMemoryController) {

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
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(94_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 30_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(66_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 70_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(40_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 90_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(40_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 120_MB; // exceeds soft limit
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(28_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->MemLimitBytes->Val());

    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->ActiveBytes->Val(), static_cast<i64>(32_KB), static_cast<i64>(5_KB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->PassiveBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveBytes->Val(), server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/Consumption")->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 1000_MB;
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
    serverSettings.AppConfig->MutableSharedCacheConfig()->SetMemoryLimit(100_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ConfigLimitBytes->Val(), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), 100_MB);

    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ConfigLimitBytes->Val(), 100_MB);
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(141_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 150_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ConfigLimitBytes->Val(), 100_MB);
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(60_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->MemLimitBytes->Val());
}

Y_UNIT_TEST(MemTable) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(200_MB);
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
    UNIT_ASSERT_DOUBLES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Limit")->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_GT(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Consumption")->Val(), static_cast<i64>(100_KB));

    server->ProcessMemoryInfo->AllocatedMemory = 1000_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Limit")->Val(), static_cast<i64>(100_KB));
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
        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * OlapLimits::GroupedMemoryLimiterSoftLimitCoefficient *
                (1.0 - ColumnTablesDeduplicationGroupedMemoryFraction) * readExecutionMemoryLimitPercent / 100 *
                NOlap::NGroupedMemoryManager::TScanMemoryLimiterPolicy::HardLimitMultiplier),
            static_cast<double>(scanCounters->GetCounter("Value/Limit/Soft/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * (1.0 - ColumnTablesDeduplicationGroupedMemoryFraction) * readExecutionMemoryLimitPercent / 100 *
                NOlap::NGroupedMemoryManager::TScanMemoryLimiterPolicy::HardLimitMultiplier),
            static_cast<double>(scanCounters->GetCounter("Value/Limit/Hard/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * OlapLimits::GroupedMemoryLimiterSoftLimitCoefficient *
                (1.0 - ColumnTablesDeduplicationGroupedMemoryFraction) * readExecutionMemoryLimitPercent / 100 *
                NOlap::NGroupedMemoryManager::TDeduplicationMemoryLimiterPolicy::HardLimitMultiplier),
            static_cast<double>(dedupCounters->GetCounter("Value/Limit/Soft/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * (1.0 - ColumnTablesDeduplicationGroupedMemoryFraction) * readExecutionMemoryLimitPercent / 100 *
                NOlap::NGroupedMemoryManager::TDeduplicationMemoryLimiterPolicy::HardLimitMultiplier),
            static_cast<double>(dedupCounters->GetCounter("Value/Limit/Hard/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * OlapLimits::GroupedMemoryLimiterSoftLimitCoefficient * compactionMemoryLimitPercent / 100 *
                NOlap::NGroupedMemoryManager::TCompMemoryLimiterPolicy::HardLimitMultiplier),
            static_cast<double>(compactionCounters->GetCounter("Value/Limit/Soft/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * compactionMemoryLimitPercent / 100.0 *
                NOlap::NGroupedMemoryManager::TCompMemoryLimiterPolicy::HardLimitMultiplier),
            static_cast<double>(compactionCounters->GetCounter("Value/Limit/Hard/Bytes")->Val()),
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

Y_UNIT_TEST(QueryExecution_Register) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root").SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(1000_MB);
    memoryControllerConfig->SetQueryExecutionLimitPercent(15);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    // Register QE consumer
    runtime.Send(new IEventHandle(MakeMemoryControllerId(0), sender,
        new TEvConsumerRegister(EMemoryConsumerKind::QueryExecution)));
    auto registered = runtime.GrabEdgeEvent<TEvConsumerRegistered>(sender);
    UNIT_ASSERT(registered->Get()->Consumer != nullptr);

    // Report 7 MB consumption via the returned consumer interface
    registered->Get()->Consumer->SetConsumption(7_MB);

    runtime.SimulateSleep(TDuration::Seconds(2));

    // Generic consumer loop is now the sole writer of Consumption counter
    UNIT_ASSERT_VALUES_EQUAL(
        server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Consumption")->Val(),
        static_cast<i64>(7_MB));

    // MC sends TEvConsumerLimit(150 MB = 15% of 1000 MB) to the registered actor each tick
    auto limitEv = runtime.GrabEdgeEvent<TEvConsumerLimit>(sender);
    UNIT_ASSERT_VALUES_EQUAL(limitEv->Get()->LimitBytes, 150_MB);
}

Y_UNIT_TEST(QueryExecution_DuplicateRegister) {
    // Duplicate QE registration: Y_ABORT removed, ActorId rebound to new sender which receives the next limit
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root").SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(1000_MB);
    memoryControllerConfig->SetQueryExecutionLimitPercent(15);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();
    auto sender1 = runtime.AllocateEdgeActor();
    auto sender2 = runtime.AllocateEdgeActor();

    runtime.Send(new IEventHandle(MakeMemoryControllerId(0), sender1,
        new TEvConsumerRegister(EMemoryConsumerKind::QueryExecution)));
    auto reg1 = runtime.GrabEdgeEvent<TEvConsumerRegistered>(sender1);
    UNIT_ASSERT(reg1->Get()->Consumer != nullptr);

    // Second registration rebinds ActorId; same consumer object (consumption preserved)
    runtime.Send(new IEventHandle(MakeMemoryControllerId(0), sender2,
        new TEvConsumerRegister(EMemoryConsumerKind::QueryExecution)));
    auto reg2 = runtime.GrabEdgeEvent<TEvConsumerRegistered>(sender2);
    UNIT_ASSERT(reg2->Get()->Consumer != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(reg1->Get()->Consumer.Get(), reg2->Get()->Consumer.Get());

    // After next tick, sender2 (new ActorId) receives TEvConsumerLimit; sender1 does not
    runtime.SimulateSleep(TDuration::Seconds(2));
    auto limitEv = runtime.GrabEdgeEvent<TEvConsumerLimit>(sender2);
    UNIT_ASSERT(limitEv != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(limitEv->Get()->LimitBytes, 150_MB);
}

Y_UNIT_TEST(QueryExecution_LimitParity) {
    // BuildConsumerState min=max guarantees TEvConsumerLimit == RB queue limit every tick
    using namespace NResourceBroker;

    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root").SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetQueryExecutionLimitPercent(15);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    server->ProcessMemoryInfo->CGroupLimit = 1000_MB;
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();
    auto rbSender = runtime.AllocateEdgeActor();
    InitRoot(server, sender);

    // Register QE consumer
    runtime.Send(new IEventHandle(MakeMemoryControllerId(0), sender,
        new TEvConsumerRegister(EMemoryConsumerKind::QueryExecution)));
    auto registered = runtime.GrabEdgeEvent<TEvConsumerRegistered>(sender);
    UNIT_ASSERT(registered->Get()->Consumer != nullptr);

    // Subscribe to RB config changes for parity check
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), rbSender,
        new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue, /*subscribe=*/ true)));
    auto rbConfig = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(rbSender);
    UNIT_ASSERT_VALUES_EQUAL(rbConfig->Get()->QueueConfig->GetLimit().GetMemory(), 150_MB);

    runtime.SimulateSleep(TDuration::Seconds(2));

    // MC sends TEvConsumerLimit twice (2 ticks); both must equal RB queue limit
    auto limitEv = runtime.GrabEdgeEvent<TEvConsumerLimit>(sender);
    UNIT_ASSERT_VALUES_EQUAL(limitEv->Get()->LimitBytes, 150_MB);
    limitEv = runtime.GrabEdgeEvent<TEvConsumerLimit>(sender); // drain second
    UNIT_ASSERT_VALUES_EQUAL(limitEv->Get()->LimitBytes, 150_MB);

    // Change memory, verify parity is maintained
    server->ProcessMemoryInfo->CGroupLimit = 500_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));

    limitEv = runtime.GrabEdgeEvent<TEvConsumerLimit>(sender);
    UNIT_ASSERT_VALUES_EQUAL(limitEv->Get()->LimitBytes, 75_MB);

    // RB subscriber also gets the updated limit
    rbConfig = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(rbSender);
    UNIT_ASSERT_VALUES_EQUAL(rbConfig->Get()->QueueConfig->GetLimit().GetMemory(), 75_MB);
}

Y_UNIT_TEST(QueryExecution_LimitParityUnderSelfConfigOverride) {
    // A self-config override below the formula must reach both gates, not just the queue one
    using namespace NResourceBroker;

    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root").SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(1000_MB);
    memoryControllerConfig->SetQueryExecutionLimitPercent(15); // the formula alone would give 150 MB

    auto resourceBrokerConfig = serverSettings.AppConfig->MutableResourceBrokerConfig();
    auto queue = resourceBrokerConfig->AddQueues();
    queue->SetName(NLocalDb::KqpResourceManagerQueue);
    queue->MutableLimit()->SetMemory(40_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    server->ProcessMemoryInfo->CGroupLimit = 1000_MB;
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();
    auto rbSender = runtime.AllocateEdgeActor();
    InitRoot(server, sender);

    runtime.Send(new IEventHandle(MakeMemoryControllerId(0), sender,
        new TEvConsumerRegister(EMemoryConsumerKind::QueryExecution)));
    auto registered = runtime.GrabEdgeEvent<TEvConsumerRegistered>(sender);
    UNIT_ASSERT(registered->Get()->Consumer != nullptr);

    runtime.SimulateSleep(TDuration::Seconds(2));

    auto limitEv = runtime.GrabEdgeEvent<TEvConsumerLimit>(sender);
    UNIT_ASSERT_VALUES_EQUAL(limitEv->Get()->LimitBytes, 40_MB);

    runtime.Send(new IEventHandle(MakeResourceBrokerID(), rbSender,
        new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue)));
    auto rbConfig = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(rbSender);
    UNIT_ASSERT_VALUES_EQUAL(rbConfig->Get()->QueueConfig->GetLimit().GetMemory(), 40_MB);
}

Y_UNIT_TEST(QueryExecution_Neutrality) {
    // QE with nonzero Consumption must leave the SharedCache and MemTable limits untouched, coefficient in (0, 1)
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(1000_MB);
    memoryControllerConfig->SetQueryExecutionLimitPercent(15);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    server->ProcessMemoryInfo->CGroupLimit = 1000_MB;
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();
    InitRoot(server, sender);

    // AllocatedMemory 100 MB keeps the coefficient near 0.59, inside (0, 1); the counter is scaled by 1e9
    server->ProcessMemoryInfo->AllocatedMemory = 100_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));

    i64 coeff = server->MemoryControllerCounters->GetCounter("Stats/Coefficient")->Val();
    // Verify coefficient is actually in the interesting regime (0 < coef < 1; counter = coef * 1e9)
    UNIT_ASSERT_GT(coeff, 0);
    UNIT_ASSERT_LT(coeff, 1000000000); // coefficient < 1.0

    i64 scLimit = server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/Limit")->Val();
    i64 mtLimit = server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Limit")->Val();

    // QE with nonzero consumption must leave every elastic-consumer limit unchanged: the two effects cancel exactly
    runtime.Send(new IEventHandle(MakeMemoryControllerId(0), sender,
        new TEvConsumerRegister(EMemoryConsumerKind::QueryExecution)));
    auto registered = runtime.GrabEdgeEvent<TEvConsumerRegistered>(sender);
    registered->Get()->Consumer->SetConsumption(50_MB);
    // AllocatedMemory stays at 100 MB (includes QE's page-pool bytes); math cancels

    runtime.SimulateSleep(TDuration::Seconds(2));

    // Only SC/MT limits and coefficient must be unchanged; ResultingConsumersConsumption grows by QE limit
    UNIT_ASSERT_VALUES_EQUAL(
        server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/Limit")->Val(), scLimit);
    UNIT_ASSERT_VALUES_EQUAL(
        server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Limit")->Val(), mtLimit);
    UNIT_ASSERT_VALUES_EQUAL(
        server->MemoryControllerCounters->GetCounter("Stats/Coefficient")->Val(), coeff);

    // Boundary of the same property: AllocatedMemory below consumersConsumption makes SafeDiff clamp
    // otherConsumption to 0, so neutrality no longer holds -- but CanZeroLimit=false keeps the QE limit positive
    server->ProcessMemoryInfo->AllocatedMemory = 50_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/OtherConsumption")->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 150_MB);
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
}

}
