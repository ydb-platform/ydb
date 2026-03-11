#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/mon_alloc/monitor.h>
#include <ydb/core/mon_alloc/stats.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/actors/core/process_stats.h>

#include <algorithm>
#include <util/system/getpid.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

struct TProcessMemorySnapshot {
    bool Valid = false;
    ui64 AnonRss = 0;
    ui64 CGroupMemLimit = 0;
};

struct TTcMallocCountersSnapshot {
    bool Valid = false;
    ui64 PhysicalMemoryUsed = 0;
    ui64 BytesInUseByApp = 0;
    ui64 CachesBytes = 0;
    ui64 PageHeapFree = 0;
    ui64 PageHeapUnmapped = 0;
};

struct TTcMallocActivitySample {
    TString Activity;
    ui64 SampledSize = 0;
    ui64 SampledCount = 0;
};

TProcessMemorySnapshot ReadProcessMemorySnapshot() {
    NActors::TProcStat stat;
    if (!stat.Fill(GetPID())) {
        return {};
    }

    return {
        .Valid = true,
        .AnonRss = stat.AnonRss,
        .CGroupMemLimit = stat.CGroupMemLim,
    };
}

TTcMallocCountersSnapshot ReadTcMallocCountersNow(
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& rootCounters)
{
    if (!rootCounters) {
        return {};
    }

    auto utilsCounters = GetServiceCounters(rootCounters, "utils");
    if (!utilsCounters) {
        return {};
    }

    auto tcmallocCounters = utilsCounters->FindSubgroup("component", "tcmalloc");
    if (!tcmallocCounters) {
        return {};
    }

    auto read = [&](const char* name, bool& found) -> ui64 {
        if (auto counter = tcmallocCounters->FindCounter(name)) {
            found = true;
            return counter->Val();
        }
        return 0;
    };

    bool foundAny = false;
    TTcMallocCountersSnapshot snapshot;
    snapshot.PhysicalMemoryUsed = read("generic.physical_memory_used", foundAny);
    snapshot.BytesInUseByApp = read("generic.bytes_in_use_by_app", foundAny);
    snapshot.CachesBytes = read("tcmalloc.caches_bytes", foundAny);
    snapshot.PageHeapFree = read("tcmalloc.page_heap_free", foundAny);
    snapshot.PageHeapUnmapped = read("tcmalloc.page_heap_unmapped", foundAny);
    snapshot.Valid = foundAny;
    return snapshot;
}

TVector<TTcMallocActivitySample> ReadTcMallocActivitySamplesNow(
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& rootCounters)
{
    TVector<TTcMallocActivitySample> result;
    if (!rootCounters) {
        return result;
    }

    auto utilsCounters = GetServiceCounters(rootCounters, "utils");
    if (!utilsCounters) {
        return result;
    }

    auto tcmallocCounters = utilsCounters->FindSubgroup("component", "tcmalloc");
    if (!tcmallocCounters) {
        return result;
    }

    TVector<TString> activities;
    tcmallocCounters->EnumerateSubgroups([&](const TString& name, const TString& value) {
        if (name == "activity") {
            activities.push_back(value);
        }
    });

    for (const TString& activity : activities) {
        auto group = tcmallocCounters->FindSubgroup("activity", activity);
        if (!group) {
            continue;
        }

        ui64 sampledSize = 0;
        ui64 sampledCount = 0;
        if (auto sizeCounter = group->FindCounter("tcmalloc.sampled_size_by_activity")) {
            sampledSize = sizeCounter->Val();
        }
        if (auto countCounter = group->FindCounter("tcmalloc.sampled_count_by_activity")) {
            sampledCount = countCounter->Val();
        }

        if (sampledSize || sampledCount) {
            result.push_back(TTcMallocActivitySample{
                .Activity = activity,
                .SampledSize = sampledSize,
                .SampledCount = sampledCount,
            });
        }
    }

    std::sort(result.begin(), result.end(), [](const auto& l, const auto& r) {
        if (l.SampledSize != r.SampledSize) {
            return l.SampledSize > r.SampledSize;
        }
        return l.Activity < r.Activity;
    });

    return result;
}

void MergeTcMallocPeak(TTcMallocCountersSnapshot& peak, const TTcMallocCountersSnapshot& current) {
    if (!current.Valid) {
        return;
    }
    if (!peak.Valid) {
        peak = current;
        return;
    }

    peak.PhysicalMemoryUsed = Max(peak.PhysicalMemoryUsed, current.PhysicalMemoryUsed);
    peak.BytesInUseByApp = Max(peak.BytesInUseByApp, current.BytesInUseByApp);
    peak.CachesBytes = Max(peak.CachesBytes, current.CachesBytes);
    peak.PageHeapFree = Max(peak.PageHeapFree, current.PageHeapFree);
    peak.PageHeapUnmapped = Max(peak.PageHeapUnmapped, current.PageHeapUnmapped);
}

TIntrusivePtr<TSharedPageCacheCounters> GetSharedCacheCountersNow(
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& rootCounters)
{
    if (!rootCounters) {
        return {};
    }

    auto score = [](const TIntrusivePtr<TSharedPageCacheCounters>& counters) -> ui64 {
        if (!counters) {
            return 0;
        }
        return counters->ActiveBytes->Val()
            + counters->PassiveBytes->Val()
            + counters->ActiveLimitBytes->Val()
            + counters->MemLimitBytes->Val();
    };

    TIntrusivePtr<TSharedPageCacheCounters> fromRoot = MakeIntrusive<TSharedPageCacheCounters>(rootCounters);

    UNIT_ASSERT(score(fromRoot) > 0);

    return fromRoot;
}

NMonitoring::TDynamicCounters::TCounterPtr GetTabletOpsHostASampledSizeNow(
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& rootCounters)
{
    UNIT_ASSERT(rootCounters);

    auto utilsCounters = GetServiceCounters(rootCounters, "utils");
    
    UNIT_ASSERT(utilsCounters);

    auto tcmallocCounters = utilsCounters->FindSubgroup("component", "tcmalloc");
    UNIT_ASSERT(tcmallocCounters);

    auto hostA = tcmallocCounters->FindSubgroup("activity", "TABLET_OPS_HOST_A");
    UNIT_ASSERT(hostA);

    auto counter = hostA->FindCounter("tcmalloc.sampled_size_by_activity");
    UNIT_ASSERT(counter);

    return counter;
}

void StartMemAllocCollectorsForTestRuntime(
    NActors::TTestActorRuntime& runtime,
    ui32 nodeIndex = 0)
{
    auto rootCounters = runtime.GetDynamicCounters(nodeIndex);

    auto memProfMonitorId = runtime.Register(
        CreateMemProfMonitor(runtime.GetMemObserver(nodeIndex), 1, rootCounters),
        nodeIndex);
    runtime.EnableScheduleForActor(memProfMonitorId, true);

    auto memStatsCollectorId = runtime.Register(
        CreateMemStatsCollector(1, rootCounters),
        nodeIndex);
    runtime.EnableScheduleForActor(memStatsCollectorId, true);
}

void FillKeyValueTableWithLargeRows(
    TTableClient& tableClient,
    ui32 rows,
    ui32 batchSize,
    ui32 valueSize)
{
    const TString value(valueSize, 'm');

    for (ui32 begin = 0; begin < rows; begin += batchSize) {
        const ui32 end = Min(begin + batchSize, rows);
        TValueBuilder rowsBuilder;
        rowsBuilder.BeginList();
        for (ui32 key = begin; key < end; ++key) {
            rowsBuilder.AddListItem()
                .BeginStruct()
                .AddMember("Key").OptionalUint64(key)
                .AddMember("Value").OptionalString(value)
                .EndStruct();
        }
        rowsBuilder.EndList();

        auto result = tableClient.BulkUpsert("/Root/KeyValue", rowsBuilder.Build()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
}

TString BuildMemoryReproDiagnostics(
    ui64 baselineAnonRss,
    ui64 peakAnonRss,
    ui64 cgroupLimit,
    const TTcMallocCountersSnapshot& baselineTcMalloc,
    const TTcMallocCountersSnapshot& peakTcMalloc,
    const TIntrusivePtr<TSharedPageCacheCounters>& sharedCacheCounters,
    const NMonitoring::TDynamicCounters::TCounterPtr& hostASampledBytes,
    const TVector<TTcMallocActivitySample>& activitySamples)
{
    TStringBuilder out;
    const ui64 delta = peakAnonRss > baselineAnonRss ? peakAnonRss - baselineAnonRss : 0;
    auto toMiB = [](ui64 bytes) {
        return bytes / (1024ull * 1024);
    };
    auto appendBytesField = [&](const char* name, ui64 bytes, bool leadingSpace = true) {
        if (leadingSpace) {
            out << " ";
        }
        out << name << "=" << bytes << " (" << toMiB(bytes) << " MiB)\n";
    };

    appendBytesField("baseline_anon_rss", baselineAnonRss, false);
    appendBytesField("peak_anon_rss", peakAnonRss);
    appendBytesField("delta", delta);
    appendBytesField("cgroup_limit", cgroupLimit);

    appendBytesField("shared_cache_active", sharedCacheCounters->ActiveBytes->Val());
    appendBytesField("shared_cache_passive", sharedCacheCounters->PassiveBytes->Val());
    appendBytesField("shared_cache_active_limit", sharedCacheCounters->ActiveLimitBytes->Val());
    appendBytesField("shared_cache_mem_limit", sharedCacheCounters->MemLimitBytes->Val());

    appendBytesField("tcmalloc_tablet_ops_host_a", hostASampledBytes->Val());

    appendBytesField("tcmalloc_physical_baseline", baselineTcMalloc.PhysicalMemoryUsed);
    appendBytesField("tcmalloc_physical_peak", peakTcMalloc.PhysicalMemoryUsed);
    appendBytesField("tcmalloc_in_use_baseline", baselineTcMalloc.BytesInUseByApp);
    appendBytesField("tcmalloc_in_use_peak", peakTcMalloc.BytesInUseByApp);
    appendBytesField("tcmalloc_caches_baseline", baselineTcMalloc.CachesBytes);
    appendBytesField("tcmalloc_caches_peak", peakTcMalloc.CachesBytes);
    appendBytesField("tcmalloc_page_heap_free_peak", peakTcMalloc.PageHeapFree);
    appendBytesField("tcmalloc_page_heap_unmapped_peak", peakTcMalloc.PageHeapUnmapped);
    out << " tcmalloc_activity_samples=" << activitySamples.size() << "\n";
    for (const auto& sample : activitySamples) {
        const TString fieldName = TStringBuilder() << "tcmalloc_activity_size_" << sample.Activity;
        appendBytesField(fieldName.c_str(), sample.SampledSize);
        out << " tcmalloc_activity_count_" << sample.Activity << "=" << sample.SampledCount << "\n";
    }

    return out;
}

void DumpSharedCacheCounters(
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& rootCounters)
{
    auto sharedCacheCounters = GetSharedCacheCountersNow(rootCounters);
    if (!rootCounters || !sharedCacheCounters) {
        return;
    }

    TStringBuilder suffix;
    suffix << " active=" << sharedCacheCounters->ActiveBytes->Val()
        << " passive=" << sharedCacheCounters->PassiveBytes->Val()
        << " active_limit=" << sharedCacheCounters->ActiveLimitBytes->Val()
        << " mem_limit=" << sharedCacheCounters->MemLimitBytes->Val();

    auto utilsCounters = GetServiceCounters(rootCounters, "utils");
    if (utilsCounters) {
        auto procAnon = utilsCounters->FindCounter("Process/AnonRssSize");
        auto procLimit = utilsCounters->FindCounter("Process/CGroupMemLimit");
        if (procAnon || procLimit) {
            suffix << " utils_proc_anon=" << (procAnon ? procAnon->Val() : 0)
                << " utils_proc_limit=" << (procLimit ? procLimit->Val() : 0);
        }
    }

    Cerr << "SharedCacheCounters" << suffix << Endl;
}

void CreateSampleTables(TKikimrRunner& kikimr) {
    kikimr.GetTestClient().CreateTable("/Root", R"(
        Name: "FourShard"
        Columns { Name: "Key", Type: "Uint64" }
        Columns { Name: "Value1", Type: "String" }
        Columns { Name: "Value2", Type: "String" }
        KeyColumnNames: ["Key"],
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 300 } } } }
    )");

    TTableClient tableClient{kikimr.GetDriver()};
    auto session = tableClient.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/FourShard` (Key, Value1, Value2) VALUES
            (1u,   "Value-001",  "1"),
            (2u,   "Value-002",  "2"),
            (101u, "Value-101",  "101"),
            (102u, "Value-102",  "102"),
            (201u, "Value-201",  "201"),
            (202u, "Value-202",  "202"),
            (301u, "Value-301",  "301"),
            (302u, "Value-302",  "302")
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();

    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    session.Close();
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpFlowControl) {

void DoFlowControlTest(ui64 limit, bool hasBlockedByCapacity) {
    NKikimrConfig::TAppConfig appCfg;
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(limit);
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetMinChannelBufferSize(limit);
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetMkqlHeavyProgramMemoryLimit(200ul << 20);
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetQueryMemoryLimit(20ul << 30);
    appCfg.MutableTableServiceConfig()->SetEnableKqpScanQueryStreamLookup(false);

    // TODO: KIKIMR-14294
    auto kikimrSettings = TKikimrSettings()
        .SetAppConfig(appCfg)
        .SetKqpSettings({});

    TKikimrRunner kikimr{kikimrSettings};
    // kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    // kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
    // kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_GATEWAY, NActors::NLog::PRI_DEBUG);
    // kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, NActors::NLog::PRI_DEBUG);

    CreateSampleTables(kikimr);
    auto db = kikimr.GetTableClient();

    Y_DEFER {
        NYql::NDq::GetDqExecutionSettingsForTests().Reset();
    };

    NYql::NDq::GetDqExecutionSettingsForTests().FlowControl.MaxOutputChunkSize = limit;
    NYql::NDq::GetDqExecutionSettingsForTests().FlowControl.InFlightBytesOvercommit = 1.0f;

    TStreamExecScanQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Profile);

    auto it = db.StreamExecuteScanQuery(R"(
            $r = (select * from `/Root/FourShard` where Key > 201);

            SELECT l.Key as key, l.Text as text, r.Value1 as value
            FROM `/Root/EightShard` AS l JOIN $r AS r ON l.Key = r.Key
            ORDER BY key, text, value
        )", settings).GetValueSync();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    auto res = CollectStreamResult(it);

    CompareYson(R"([
            [[202u];["Value2"];["Value-202"]];
            [[301u];["Value1"];["Value-301"]];
            [[302u];["Value2"];["Value-302"]]
        ])", res.ResultSetYson);

    NJson::TJsonValue plan;
    NJson::ReadJsonTree(*res.PlanJson, &plan, true);

    ui32 writesBlockedNoSpace = 0;
    auto nodes = FindPlanNodes(plan, "Pop.WaitTimeUs.Sum");
    for (auto& node : nodes) {
        writesBlockedNoSpace += node.GetIntegerSafe();
    }

    UNIT_ASSERT_EQUAL_C(hasBlockedByCapacity, writesBlockedNoSpace > 0, *res.PlanJson);
}

Y_UNIT_TEST(FlowControl_Unlimited) {
    DoFlowControlTest(100ul << 20, false);
}

Y_UNIT_TEST(FlowControl_BigLimit) {
    DoFlowControlTest(1ul << 10, false);
}

Y_UNIT_TEST(FlowControl_SmallLimit) {
    DoFlowControlTest(1ul, true);
}

Y_UNIT_TEST(FlowControl_ProcessMemoryReproExceedsTwoGiB) {
    constexpr ui64 OneGiB = 1ull << 30;
    constexpr ui64 TwoGiB = 2ull << 30;
    constexpr ui64 SharedCacheLimit = OneGiB;
    constexpr ui64 TargetAnonRss = TwoGiB;
    constexpr ui64 MinExpectedDelta = OneGiB;
    constexpr ui64 ChannelBufferSize = 64ull << 20;
    constexpr ui32 StreamsCount = 48;
    constexpr ui32 FillRows = 6000;
    constexpr ui32 FillBatchSize = 25;
    constexpr ui32 ValueSize = 64 * 1024;

    NKikimrConfig::TAppConfig appCfg;
    auto* rm = appCfg.MutableTableServiceConfig()->MutableResourceManager();
    rm->SetChannelBufferSize(ChannelBufferSize);
    rm->SetMinChannelBufferSize(ChannelBufferSize);
    rm->SetMkqlHeavyProgramMemoryLimit(8ull << 30);
    rm->SetQueryMemoryLimit(64ull << 30);
    appCfg.MutableTableServiceConfig()->SetEnableKqpScanQueryStreamLookup(false);

    auto kikimrSettings = TKikimrSettings()
        .SetAppConfig(appCfg)
        .SetKqpSettings({});

    TKikimrRunner kikimr{kikimrSettings};

    TAtomic previous = 0;
    auto* runtime = kikimr.GetTestServer().GetRuntime();
    runtime->GetAppData().Icb->SetValue("SharedPageCache_Size", SharedCacheLimit, previous);
    runtime->GetAppData().Icb->SetValue("SharedPageCache_Size", SharedCacheLimit, previous);

    auto rootCounters = runtime->GetDynamicCounters(0);
    StartMemAllocCollectorsForTestRuntime(*runtime, 0);

    auto db = kikimr.GetTableClient();

    FillKeyValueTableWithLargeRows(db, FillRows, FillBatchSize, ValueSize);

    auto baseline = ReadProcessMemorySnapshot();
    UNIT_ASSERT_C(baseline.Valid, "failed to read process memory baseline");
    auto baselineTcMalloc = ReadTcMallocCountersNow(rootCounters);

    Y_DEFER {
        NYql::NDq::GetDqExecutionSettingsForTests().Reset();
    };

    NYql::NDq::GetDqExecutionSettingsForTests().FlowControl.MaxOutputChunkSize = ChannelBufferSize;
    NYql::NDq::GetDqExecutionSettingsForTests().FlowControl.InFlightBytesOvercommit = 1.0f;

    TVector<TScanQueryPartIterator> iterators;
    iterators.reserve(StreamsCount);

    for (ui32 i = 0; i < StreamsCount; ++i) {
        auto it = db.StreamExecuteScanQuery("SELECT Key, Value FROM `/Root/KeyValue`").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        iterators.emplace_back(std::move(it));
    }

    ui64 peakAnonRss = baseline.AnonRss;
    ui64 cgroupLimit = baseline.CGroupMemLimit;
    TTcMallocCountersSnapshot peakTcMalloc = baselineTcMalloc;

    TVector<bool> finished(iterators.size(), false);
    size_t unfinished = iterators.size();

    const TInstant deadline = TInstant::Now() + TDuration::Seconds(300);
    bool reachedTarget = false;

    while (unfinished > 0 && TInstant::Now() < deadline) {
        for (size_t i = 0; i < iterators.size(); ++i) {
            if (finished[i]) {
                continue;
            }

            auto part = iterators[i].ReadNext().GetValueSync();
            UNIT_ASSERT_C(part.IsSuccess(), part.GetIssues().ToString());

            if (part.EOS()) {
                finished[i] = true;
                --unfinished;
                continue;
            }

            if (part.HasResultSet()) {
                part.ExtractResultSet();
            }
        }

        auto snapshot = ReadProcessMemorySnapshot();
        if (snapshot.Valid) {
            peakAnonRss = Max(peakAnonRss, snapshot.AnonRss);
            cgroupLimit = snapshot.CGroupMemLimit;
        }
        MergeTcMallocPeak(peakTcMalloc, ReadTcMallocCountersNow(rootCounters));

        if (peakAnonRss > TargetAnonRss && peakAnonRss > baseline.AnonRss + MinExpectedDelta) {
            reachedTarget = true;
            break;
        }

        Sleep(TDuration::MilliSeconds(100));
    }

    auto sharedCacheCountersNow = GetSharedCacheCountersNow(rootCounters);
    auto hostASampledBytesNow = GetTabletOpsHostASampledSizeNow(rootCounters);
    auto activitySamplesNow = ReadTcMallocActivitySamplesNow(rootCounters);

    const TString diagnostics = BuildMemoryReproDiagnostics(
        baseline.AnonRss,
        peakAnonRss,
        cgroupLimit,
        baselineTcMalloc,
        peakTcMalloc,
        sharedCacheCountersNow,
        hostASampledBytesNow,
        activitySamplesNow);

    Cerr << "FlowControl_ProcessMemoryReproExceedsTwoGiB diagnostics: " << diagnostics << Endl;
    DumpSharedCacheCounters(rootCounters);

    UNIT_ASSERT_C(reachedTarget, diagnostics);
    UNIT_ASSERT_C(peakAnonRss > TargetAnonRss, diagnostics);
    UNIT_ASSERT_C(peakAnonRss > baseline.AnonRss + MinExpectedDelta, diagnostics);
}

//Y_UNIT_TEST(SlowClient) {
void SlowClient() {
    NKikimrConfig::TAppConfig appCfg;
    appCfg.MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(1);

    TKikimrRunner kikimr(appCfg);

    {
        TTableClient tableClient{kikimr.GetDriver()};
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto value = std::string(1000, 'a');

        for (int q = 0; q < 100; ++q) {
            TStringBuilder query;
            query << "REPLACE INTO [/Root/KeyValue] (Key, Value) VALUES (" << q << ", \"" << value << "\")";

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    auto db = kikimr.GetTableClient();
    auto it = db.StreamExecuteScanQuery("SELECT Key, Value FROM `/Root/KeyValue`").GetValueSync();
    auto part = it.ReadNext().GetValueSync();

    auto counters = kikimr.GetTestServer().GetRuntime()->GetAppData(0).Counters;
    TKqpCounters kqpCounters(counters);

    UNIT_ASSERT_EQUAL(kqpCounters.RmComputeActors->Val(), 2);

    Cerr << "-- got value and go sleep...\n";
    ::Sleep(TDuration::Seconds(3));
    Cerr << "-- go on...\n";

    UNIT_ASSERT_EQUAL(kqpCounters.RmComputeActors->Val(), 2);

    // consume 990 elements
    int remains = 990;
    while (remains > 0) {
        if (part.HasResultSet()) {
            part.ExtractResultSet();
            --remains;
            ::Sleep(TDuration::MilliSeconds(10));
            Cerr << "-- remains: " << remains << Endl;
        }
        part = it.ReadNext().GetValueSync();
        UNIT_ASSERT(!part.EOS());
    }

    UNIT_ASSERT_EQUAL(kqpCounters.RmComputeActors->Val(), 2);

    while (!part.EOS()) {
        part = it.ReadNext().GetValueSync();
    }

    UNIT_ASSERT_EQUAL(kqpCounters.RmComputeActors->Val(), 0);
    UNIT_ASSERT_EQUAL(kqpCounters.RmMemory->Val(), 0);
}

} // suite

} // namespace NKikimr::NKqp
